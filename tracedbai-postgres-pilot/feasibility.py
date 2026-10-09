#!/usr/bin/env python3
"""
Feasibility measurements for the EDBT'27 vision paper
"Beyond Performance: Authority, Evidence, and Resource Accountability ..."

Everything runs on stock PostgreSQL (tested on 16.x) with the psycopg2 driver.
No extension, no custom optimiser: descriptor checks are plain SQL, dependency
invalidation is a recursive CTE, admission/release is an ordinary transaction.

  E1  R1: loss-witness check as a GROUP BY query (instance level and over the
          declared domain of 3^m states), lossy and loss-free mappings.
  E2  R2: revocation -> blocking of all transitively dependent artefacts
          (recursive CTE), checked against an independent Python BFS oracle.
  E3  R2/R4: admission + dependency registration + release recheck overhead on a
          join workload, versus the same INSERT..SELECT without governance.
  E4  R4/R5: revocation racing a release under READ COMMITTED / snapshot
          isolation / SERIALIZABLE / FOR SHARE barrier (write-skew test).
  E5  R4: atomic budget reservation under concurrency and the budget walkthrough.

Usage:  FEAS_DSN="host=/tmp port=5433 user=postgres dbname=feas" python3 feasibility.py
Output: feasibility_results.json (+ summary on stdout).
Numbers are single-machine, synthetic-data measurements: they show feasibility
and order of magnitude, not performance claims.
"""
import json
import os
import platform
import random
import statistics
import threading
import time
from collections import defaultdict, deque

import psycopg2
from psycopg2 import errors
from psycopg2.extras import execute_values

DSN = os.environ.get("FEAS_DSN", "host=/tmp port=5433 user=postgres dbname=feas")
REPS = int(os.environ.get("FEAS_REPS", "7"))
random.seed(7)
RESULTS = {}


def connect(autocommit=True, iso=None):
    c = psycopg2.connect(DSN)
    if iso:
        c.set_session(isolation_level=iso)
    c.autocommit = autocommit
    return c


def timed(fn, reps):
    ts = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        ts.append((time.perf_counter() - t0) * 1000.0)
    return {"median_ms": round(statistics.median(ts), 2),
            "min_ms": round(min(ts), 2), "max_ms": round(max(ts), 2), "reps": reps}


# ---------------------------------------------------------------- E1 ----
def witness_queries(m, lossy):
    """Return list of (k, loss_sql, collapse_sql) for table dom/inst with columns a0..a{m-1}.
    Output expression e_k per distinction k. Lossy mapping: e_0 maps unknown to 0
    (unknown-collapse witness); e_1 is the constant 'exists' (loss witness); rest identity."""
    qs = []
    for k in range(m):
        a = f"a{k}"
        if lossy and k == 0:
            e = "coalesce(a0,0)"
        elif lossy and k == 1:
            e = "('exists'::text)"
        else:
            e = a
        loss = (f"SELECT count(*) FROM (SELECT 1 FROM {{t}} GROUP BY {e} "
                f"HAVING count(DISTINCT {a}) > 1) w")
        collapse = f"SELECT count(*) FROM {{t}} WHERE {a} IS NULL AND ({e}) IS NOT NULL"
        qs.append((k, loss, collapse))
    return qs


def run_checks(cur, table, m, lossy):
    loss_w = collapse_w = 0
    for _, loss, collapse in witness_queries(m, lossy):
        cur.execute(loss.format(t=table))
        loss_w += cur.fetchone()[0]
        cur.execute(collapse.format(t=table))
        collapse_w += cur.fetchone()[0]
    return loss_w, collapse_w


def e1():
    c = connect()
    cur = c.cursor()
    out = {"declared_domain": [], "instance": []}
    for m in (2, 4, 6, 8, 10, 12):
        cols = ", ".join(f"a{i}" for i in range(m))
        froms = ", ".join(f"(VALUES (0),(1),(NULL::int)) v{i}(a{i})" for i in range(m))
        cur.execute("DROP TABLE IF EXISTS dom")
        cur.execute(f"CREATE UNLOGGED TABLE dom AS SELECT {cols} FROM {froms}")
        cur.execute("ANALYZE dom")
        cur.execute("SELECT count(*) FROM dom")
        n = cur.fetchone()[0]
        reps = REPS if n <= 60000 else 3
        row = {"m": m, "states": n}
        for lossy in (False, True):
            res = {}
            t = timed(lambda: res.update(w=run_checks(cur, "dom", m, lossy)), reps)
            row["lossy" if lossy else "clean"] = {**t, "loss_witness_groups": res["w"][0],
                                                   "unknown_collapse_rows": res["w"][1]}
        # correctness: clean mapping has no witnesses; lossy has exactly the expected ones
        assert row["clean"]["loss_witness_groups"] == 0 and row["clean"]["unknown_collapse_rows"] == 0
        assert row["lossy"]["loss_witness_groups"] == 1
        assert row["lossy"]["unknown_collapse_rows"] == 2 * 3 ** (m - 1)
        out["declared_domain"].append(row)
    # instance-level: n random records, m = 4 distinctions
    m = 4
    for n in (10_000, 100_000, 1_000_000):
        cur.execute("DROP TABLE IF EXISTS inst")
        draw = "(ARRAY[0,1,NULL])[1 + floor(random()*3)::int]"
        cols = ", ".join(f"{draw} AS a{i}" for i in range(m))
        cur.execute(f"CREATE UNLOGGED TABLE inst AS SELECT {cols} FROM generate_series(1,{n})")
        cur.execute("ANALYZE inst")
        row = {"m": m, "records": n}
        for lossy in (False, True):
            res = {}
            t = timed(lambda: res.update(w=run_checks(cur, "inst", m, lossy)), REPS if n < 1_000_000 else 3)
            row["lossy" if lossy else "clean"] = {**t, "loss_witness_groups": res["w"][0],
                                                   "unknown_collapse_rows": res["w"][1]}
        assert row["clean"]["loss_witness_groups"] == 0
        out["instance"].append(row)
    cur.execute("DROP TABLE IF EXISTS dom, inst")
    RESULTS["E1_loss_witness_check"] = out


# ---------------------------------------------------------------- E2 ----
def e2():
    c = connect()
    cur = c.cursor()
    out = []
    for n_der in (1_000, 10_000, 100_000):
        n_src = max(100, n_der // 10)
        cur.execute("DROP TABLE IF EXISTS dep, edge, deriv, src CASCADE")
        cur.execute("CREATE TABLE src(id int PRIMARY KEY, revoked bool DEFAULT false)")
        cur.execute("CREATE TABLE deriv(id int PRIMARY KEY, layer int, status text DEFAULT 'valid')")
        cur.execute("CREATE TABLE dep(deriv int, src int)")
        cur.execute("CREATE TABLE edge(child int, parent int)")
        execute_values(cur, "INSERT INTO src(id) VALUES %s", [(i,) for i in range(n_src)])
        layer = {}
        deps, edges = [], []
        l0 = int(n_der * 0.6)
        l1 = int(n_der * 0.3)
        for d in range(n_der):
            layer[d] = 0 if d < l0 else (1 if d < l0 + l1 else 2)
        l0ids = [d for d in range(n_der) if layer[d] == 0]
        l1ids = [d for d in range(n_der) if layer[d] == 1]
        for d in range(n_der):
            if layer[d] == 0:  # views: 3 sources
                for s in random.sample(range(n_src), 3):
                    deps.append((d, s))
            elif layer[d] == 1:  # summaries: 1-2 views + 1 source (accounts)
                for p in random.sample(l0ids, random.choice((1, 2))):
                    edges.append((d, p))
                deps.append((d, random.randrange(n_src)))
            else:  # caches/memory: 1-2 summaries
                for p in random.sample(l1ids, random.choice((1, 2))):
                    edges.append((d, p))
        execute_values(cur, "INSERT INTO deriv(id, layer) VALUES %s", [(d, layer[d]) for d in range(n_der)])
        execute_values(cur, "INSERT INTO dep VALUES %s", deps)
        execute_values(cur, "INSERT INTO edge VALUES %s", edges)
        cur.execute("CREATE INDEX ON dep(src)")
        cur.execute("CREATE INDEX ON edge(parent)")
        cur.execute("ANALYZE")
        # independent oracle: Python BFS over the same edge lists
        by_src = defaultdict(list)
        for d, s in deps:
            by_src[s].append(d)
        children = defaultdict(list)
        for ch, pa in edges:
            children[pa].append(ch)

        def oracle(s):
            seen = set(by_src[s])
            q = deque(seen)
            while q:
                x = q.popleft()
                for ch in children[x]:
                    if ch not in seen:
                        seen.add(ch)
                        q.append(ch)
            return seen

        sample = random.sample(range(n_src), 20)
        lat, aff, agree = [], [], True
        w = connect(autocommit=False)
        wc = w.cursor()
        for s in sample:
            t0 = time.perf_counter()
            wc.execute("UPDATE src SET revoked = true WHERE id = %s", (s,))
            wc.execute("""WITH RECURSIVE aff(id) AS (
                            SELECT deriv FROM dep WHERE src = %s
                            UNION
                            SELECT e.child FROM edge e JOIN aff ON e.parent = aff.id)
                          UPDATE deriv SET status = 'blocked' WHERE id IN (SELECT id FROM aff)""", (s,))
            blocked_n = wc.rowcount
            w.commit()
            lat.append((time.perf_counter() - t0) * 1000.0)
            wc.execute("SELECT id FROM deriv WHERE status = 'blocked'")
            got = {r[0] for r in wc.fetchall()}
            exp = oracle(s)
            agree &= (got == exp) and blocked_n == len(exp)
            aff.append(len(exp))
            wc.execute("UPDATE deriv SET status='valid' WHERE status='blocked'")
            wc.execute("UPDATE src SET revoked=false WHERE id=%s", (s,))
            w.commit()
        w.close()
        assert agree, "recursive-CTE result disagrees with BFS oracle"
        lat_s = sorted(lat)
        out.append({"derived_artefacts": n_der, "sources": n_src, "dep_edges": len(deps) + len(edges),
                    "revocations_tested": len(sample),
                    "affected_mean": round(statistics.mean(aff), 1), "affected_max": max(aff),
                    "block_latency_median_ms": round(statistics.median(lat), 2),
                    "block_latency_max_ms": round(lat_s[-1], 2),
                    "agrees_with_oracle": bool(agree)})
    cur.execute("DROP TABLE IF EXISTS dep, edge, deriv, src CASCADE")
    RESULTS["E2_revocation_blocking"] = out


# ---------------------------------------------------------------- E3 ----
JQ = ("INSERT INTO j_result SELECT f.facility, f.site, f.charge, h.t_start, h.t_end "
      "FROM F f JOIN H h ON h.facility = f.facility WHERE f.charge IS DISTINCT FROM 1")
POLICY_Q = ("SELECT src, version, revoked, purposes @> ARRAY[%s], NOT (prohibited @> ARRAY[%s]) "
            "FROM policy WHERE src IN ('F','H') ORDER BY src")


def governed(c, barrier):
    cur = c.cursor()
    q = POLICY_Q + (" FOR SHARE" if barrier else "")
    pinned = {"F": 3, "H": 1}
    cur.execute(q, ("facility_planning", "facility_planning"))          # admission: authority + versions
    for src, ver, revoked, ok_p, ok_n in cur.fetchall():
        assert (not revoked) and ok_p and ok_n and pinned[src] == ver
    cur.execute("UPDATE budget SET reserved = reserved + 20 WHERE id=1 AND total-reserved-consumed >= 20 RETURNING 1")
    assert cur.fetchone()                                           # budget reservation
    cur.execute("TRUNCATE j_result")
    cur.execute(JQ)                                                 # the actual workload
    cur.execute("INSERT INTO registry(kind,status,mandate) VALUES ('view','admitted','m1') RETURNING id")
    rid = cur.fetchone()[0]
    cur.execute("INSERT INTO dep3 VALUES (%s,'F',3),(%s,'H',1)", (rid, rid))   # artefact-level dependencies
    cur.execute("UPDATE budget SET reserved = reserved - 20, consumed = consumed + 15 WHERE id=1")  # metering
    cur.execute(q, ("facility_planning", "facility_planning"))          # release-time recheck
    for src, ver, revoked, ok_p, ok_n in cur.fetchall():
        assert (not revoked) and ok_p and ok_n and pinned[src] == ver
    cur.execute("UPDATE registry SET status='released' WHERE id=%s", (rid,))
    c.commit()


def e3():
    c0 = connect()
    cur = c0.cursor()
    out = []
    for nF in (10_000, 100_000, 500_000):
        cur.execute("DROP TABLE IF EXISTS F, H, j_result, policy, budget, registry, dep3 CASCADE")
        cur.execute("""CREATE TABLE F AS SELECT g AS facility, (g %% 500) AS site,
                       CASE WHEN random()<0.4 THEN 0 WHEN random()<0.833 THEN 1 ELSE NULL END AS charge
                       FROM generate_series(1,%s) g""", (nF,))
        cur.execute("ALTER TABLE F ADD PRIMARY KEY (facility)")
        cur.execute("""CREATE TABLE H AS SELECT f.facility, k*8 AS t_start, k*8+8 AS t_end
                       FROM F f, generate_series(0,2) k""")
        cur.execute("CREATE INDEX ON H(facility)")
        cur.execute("CREATE TABLE j_result(facility int, site int, charge int, t_start int, t_end int)")
        cur.execute("""CREATE TABLE policy(src text PRIMARY KEY, version int, revoked bool,
                       purposes text[], prohibited text[])""")
        cur.execute("""INSERT INTO policy VALUES ('F',3,false,'{facility_planning}','{identification}'),
                                                 ('H',1,false,'{facility_planning}','{identification}')""")
        cur.execute("CREATE TABLE budget(id int PRIMARY KEY, total int, reserved int, consumed int)")
        cur.execute("INSERT INTO budget VALUES (1, 100, 0, 0)")
        cur.execute("CREATE TABLE registry(id serial PRIMARY KEY, kind text, status text, mandate text)")
        cur.execute("CREATE TABLE dep3(reg int, src text, version int)")
        cur.execute("VACUUM ANALYZE")
        reps = 15 if nF < 500_000 else 9

        # Connection set-up is excluded from every timed region; variants are interleaved
        # round-robin so that cache and checkpoint drift hits all of them equally.
        def run_base():
            c = connect(autocommit=False)
            k = c.cursor()
            t0 = time.perf_counter()
            k.execute("TRUNCATE j_result")
            k.execute(JQ)
            c.commit()
            dt = time.perf_counter() - t0
            c.close()
            return dt

        def make_ext(iso, barrier):
            def run():
                c = connect(autocommit=False, iso=iso)
                c.cursor().execute("UPDATE budget SET reserved=0, consumed=0 WHERE id=1")
                c.commit()
                t0 = time.perf_counter()
                governed(c, barrier)
                dt = time.perf_counter() - t0
                c.close()
                return dt
            return run

        variants = {"baseline": run_base,
                    "governed_read_committed_for_share": make_ext("READ COMMITTED", True),
                    "governed_serializable": make_ext("SERIALIZABLE", False)}
        for fn in variants.values():          # warm-up round
            fn()
        row = {"facilities": nF, "result_rows": None, "reps": reps}
        cur.execute("SELECT count(*) FROM j_result")
        row["result_rows"] = cur.fetchone()[0]
        samples = {k: [] for k in variants}
        order = list(variants)
        for i in range(reps):
            for name in order[i % 3:] + order[:i % 3]:
                samples[name].append(variants[name]() * 1000.0)
        base_med = statistics.median(samples["baseline"])
        row["baseline"] = {"median_ms": round(base_med, 2), "min_ms": round(min(samples["baseline"]), 2),
                           "max_ms": round(max(samples["baseline"]), 2)}
        for name in ("governed_read_committed_for_share", "governed_serializable"):
            med = statistics.median(samples[name])
            row[name] = {"median_ms": round(med, 2), "min_ms": round(min(samples[name]), 2),
                         "max_ms": round(max(samples[name]), 2),
                         "overhead_ms": round(med - base_med, 2),
                         "overhead_pct": round(100.0 * (med - base_med) / base_med, 1)}
        out.append(row)
    cur.execute("DROP TABLE IF EXISTS F, H, j_result, policy, budget, registry, dep3 CASCADE")
    RESULTS["E3_release_path_overhead"] = out


# ---------------------------------------------------------------- E4 ----
def e4():
    TRIALS = 20
    c0 = connect()
    cur = c0.cursor()
    cur.execute("DROP TABLE IF EXISTS policy4, release4")
    cur.execute("CREATE TABLE policy4(src text PRIMARY KEY, revoked bool)")
    cur.execute("CREATE TABLE release4(id serial PRIMARY KEY, src text, stale bool)")
    out = {}
    variants = [("read_committed", "READ COMMITTED", False),
                ("snapshot_repeatable_read", "REPEATABLE READ", False),
                ("serializable", "SERIALIZABLE", False),
                ("read_committed_for_share_barrier", "READ COMMITTED", True)]
    for name, iso, barrier in variants:
        viol = aborted = blocked = 0
        for _ in range(TRIALS):
            cur.execute("TRUNCATE policy4, release4")
            cur.execute("INSERT INTO policy4 VALUES ('n1', false)")
            rel, rev = connect(False, iso), connect(False, iso)
            failed = set()

            def rel_txn(cn):
                k = cn.cursor()
                k.execute("SELECT revoked FROM policy4 WHERE src='n1'" + (" FOR SHARE" if barrier else ""))
                return k.fetchone()[0]

            def rev_txn(cn):
                k = cn.cursor()
                k.execute("UPDATE policy4 SET revoked = true WHERE src='n1'")
                k.execute("UPDATE release4 SET stale = true WHERE src='n1'")

            def step(who, cn, fn):
                if who in failed:
                    return None
                try:
                    return fn(cn)
                except errors.SerializationFailure:
                    cn.rollback(); failed.add(who)
                except (errors.LockNotAvailable, errors.QueryCanceled):
                    cn.rollback(); failed.add(who + "_blocked")

            def release_insert(cn):
                cn.cursor().execute("INSERT INTO release4(src, stale) VALUES ('n1', false)")

            rev.cursor().execute("SET lock_timeout = '150ms'")
            rev.commit()
            was_revoked = step("rel", rel, rel_txn)           # 1. release reads policy (not revoked)
            step("rev", rev, rev_txn)                         # 2. revoke updates policy, marks seen releases stale
            if was_revoked is False:
                step("rel", rel, release_insert)              # 3. release inserts, commits
            for who, cn in (("rel", rel), ("rev", rev)):      # 4. commits
                if who not in failed and not any(f.startswith(who) for f in failed):
                    try:
                        cn.commit()
                    except errors.SerializationFailure:
                        cn.rollback(); failed.add(who)
            rel.close(); rev.close()
            aborted += sum(1 for f in failed if not f.endswith("_blocked"))
            blocked += sum(1 for f in failed if f.endswith("_blocked"))
            # retry whichever transaction failed, serially (what an application would do)
            if any(f.startswith("rev") for f in failed):
                cur.execute("UPDATE policy4 SET revoked = true WHERE src='n1'")
                cur.execute("UPDATE release4 SET stale = true WHERE src='n1'")
            if any(f.startswith("rel") for f in failed):
                cur.execute("SELECT revoked FROM policy4 WHERE src='n1'")
                if cur.fetchone()[0] is False:
                    cur.execute("INSERT INTO release4(src, stale) VALUES ('n1', false)")
            cur.execute("""SELECT count(*) FROM release4 r JOIN policy4 p ON p.src = r.src
                           WHERE p.revoked AND NOT r.stale""")
            viol += cur.fetchone()[0]
        out[name] = {"trials": TRIALS, "final_violations": viol,
                     "serialization_aborts": aborted, "lock_barrier_blocks": blocked}
    cur.execute("DROP TABLE IF EXISTS policy4, release4")
    RESULTS["E4_revocation_release_race"] = out


# ---------------------------------------------------------------- E5 ----
def e5():
    c0 = connect()
    cur = c0.cursor()
    cur.execute("DROP TABLE IF EXISTS budget5")
    cur.execute("CREATE TABLE budget5(id int PRIMARY KEY, total int, reserved int, consumed int)")
    cur.execute("INSERT INTO budget5 VALUES (1, 100, 0, 0)")
    RUSH = "UPDATE budget5 SET reserved = reserved + %s WHERE id=1 AND total - reserved - consumed >= %s RETURNING 1"

    def reserve(k, amt):
        k.execute(RUSH, (amt, amt))
        return k.fetchone() is not None

    def finish(k, res, used):
        k.execute("UPDATE budget5 SET reserved = reserved - %s, consumed = consumed + %s WHERE id=1", (res, used))

    # walkthrough: reserve 20 (SQL), 60 (model call), 20 (validation); consume 15 and 55
    walk = {}
    walk["reserve_20_60_20"] = [reserve(cur, 20), reserve(cur, 60), reserve(cur, 20)]
    finish(cur, 20, 15)
    finish(cur, 60, 55)
    walk["second_60_unit_call_granted"] = reserve(cur, 60)
    cur.execute("SELECT total - reserved - consumed FROM budget5")
    walk["free_after_consumption"] = cur.fetchone()[0]
    # concurrency: 8 threads x 10 requests of 20 units against a 100-unit budget
    cur.execute("UPDATE budget5 SET reserved=0, consumed=0")
    granted = []
    lock = threading.Lock()

    def worker():
        c = connect()
        k = c.cursor()
        n = 0
        for _ in range(10):
            if reserve(k, 20):
                n += 1
        c.close()
        with lock:
            granted.append(n)

    ths = [threading.Thread(target=worker) for _ in range(8)]
    for t in ths: t.start()
    for t in ths: t.join()
    cur.execute("SELECT reserved FROM budget5")
    RESULTS["E5_budget_reservation"] = {"walkthrough": walk, "concurrent_total_granted": sum(granted),
                                        "concurrent_reserved_units": cur.fetchone()[0],
                                        "budget_units": 100, "request_units": 20}
    cur.execute("DROP TABLE IF EXISTS budget5")


# --------------------------------------------------------------- main ----
if __name__ == "__main__":
    c = connect()
    k = c.cursor()
    k.execute("SELECT version()")
    RESULTS["environment"] = {"postgres": k.fetchone()[0].split(",")[0], "python": platform.python_version(),
                              "cpu": next((l.split(":")[1].strip() for l in open("/proc/cpuinfo") if "model name" in l), "?"),
                              "cores": os.cpu_count(), "reps_default": REPS,
                              "note": "single machine, default PostgreSQL configuration, synthetic data"}
    c.close()
    for name, fn in (("E1", e1), ("E2", e2), ("E3", e3), ("E4", e4), ("E5", e5)):
        t0 = time.time()
        fn()
        print(f"{name} done in {time.time() - t0:.1f}s", flush=True)
    with open("feasibility_results.json", "w") as f:
        json.dump(RESULTS, f, indent=2)
    print(json.dumps(RESULTS, indent=2))
