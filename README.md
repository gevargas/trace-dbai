# TRACE DB+AI PostgreSQL feasibility pilot

This pilot illustrates selected mechanisms using Python and stock PostgreSQL. It requires no PostgreSQL extensions, custom optimiser or language-model service.

The script runs five experiments:

| Experiment | What it checks |
| --- | --- |
| E1 | SQL loss-witness checks over observed records and finite declared domains, including collapse of unknown values |
| E2 | Transitive revocation blocking using a recursive CTE, compared with an independent Python breadth-first-search oracle |
| E3 | Transactional admission, dependency registration and release checks compared with a conventional join workload |
| E4 | A controlled revocation/release race under different isolation levels and a `FOR SHARE` barrier |
| E5 | Atomic budget reservations under concurrency and an illustrative budget walkthrough |

## Files

- `tracedbai-postgres-pilot/feasibility.py`: experiment driver and synthetic-data generator.
- `feasibility_results.json`: results from a previous run. Running the script replaces this file in the current directory.

Use a dedicated test database. The script creates, truncates and drops tables, including names such as `F`, `H`, `policy` and `budget`, and some drops use `CASCADE`. Do not point it at an existing application database.

## Run in GitHub Codespaces

These instructions assume a Linux Codespace with Python 3, Docker and a running Docker daemon. PostgreSQL runs in a separate local container. The supplied results used PostgreSQL 16.15; the commands below are written for the current repository layout.

### 1. Open the repository

Open the repository in GitHub Codespaces, then move into the pilot directory before running the workload:

```bash
cd tracedbai-postgres-pilot
```

Check the prerequisites:

```bash
python3 --version
docker --version
docker info
```

If Docker is unavailable, configure the Codespace with Docker support and rebuild it before continuing. These commands need a working Docker daemon, not just the Docker client.

### 2. Install the Python dependency

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install psycopg2-binary==2.9.10
```

The package provides the `psycopg2` import used by the script. This binary distribution is convenient for this experimental environment.

### 3. Start a dedicated PostgreSQL database

```bash
docker run --name trace-feas-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=trace-local-only \
  -e POSTGRES_DB=feas \
  -p 127.0.0.1:5433:5432 \
  -d postgres:16
```

The password is for this disposable local test container. The port binds to loopback; do not make it public through Codespaces port forwarding.

Wait for database initialisation:

```bash
until docker exec trace-feas-postgres pg_isready -U postgres -d feas; do
  sleep 1
done
```

If the container already exists from an earlier session, start it instead of repeating `docker run`:

```bash
docker start trace-feas-postgres
```

### 4. Set the connection and check it

```bash
export FEAS_DSN='host=127.0.0.1 port=5433 user=postgres password=trace-local-only dbname=feas'

python - <<'PY'
import os
import psycopg2
with psycopg2.connect(os.environ['FEAS_DSN']) as connection:
    with connection.cursor() as cursor:
        cursor.execute('SELECT version()')
        print(cursor.fetchone()[0])
PY
```

Set `FEAS_DSN` again after opening a new terminal. The script's default connection uses a Unix socket at `/tmp` on port 5433, which does not reach this Docker database; the explicit TCP connection above is required for this setup.

### 5. Run all experiments

To preserve the supplied results, run from a separate output directory:

```bash
mkdir -p runs/codespaces
cd runs/codespaces
python -u ../../tracedbai-postgres-pilot/feasibility.py
```

The terminal prints progress for E1–E5, followed by the complete JSON results. A successful run writes `runs/codespaces/feasibility_results.json`. The largest workloads include one million input records, a few hundred thousand join rows and a few million dependency edges.

To reduce some timing repetitions:

```bash
FEAS_REPS=3 python -u ../../tracedbai-postgres-pilot/feasibility.py
```

This does not reduce dataset sizes or every experiment's repetitions: E3 uses 15 or 9 timed repetitions, E4 uses 20 trials per variant, and E5 uses eight worker threads. `FEAS_REPS` must be a positive integer.

Inspect the output:

```bash
python -m json.tool feasibility_results.json
```

In the Codespaces file explorer, right-click the generated JSON and choose Download to save a local copy.

## Expected checks and interpretation

- **E1:** clean mappings have no loss witnesses. For each declared domain, the lossy mapping has one loss-witness group and `2 * 3**(m-1)` unknown-collapse rows.
- **E2:** `agrees_with_oracle` should be `true` for all tested graph sizes.
- **E3:** timings vary with hardware, caching and database activity. A negative measured overhead is not evidence that governance intrinsically improves performance.
- **E4:** the supplied run reports 20 final violations for both unprotected variants, and zero for `SERIALIZABLE` and the locking barrier after the script's serial retry handling. This is one controlled example of the race.
- **E5:** five concurrent requests of 20 units fill the 100-unit budget. The additional 60-unit walkthrough request is refused. Ten units are free after consumption because the validation reservation uses the same `total - reserved - consumed` check.

Budget units in E3 and E5 are illustrative accounting values. The pilot does not measure energy, carbon, water or actual LLM resource consumption. It does not validate the full operator algebra, content ownership model or real-world compliance semantics.

## Reproducibility

Record the Codespace machine configuration, PostgreSQL version and Python dependencies alongside each results file. For example, from the run directory:

```bash
python -m pip freeze > requirements-used.txt
docker image inspect postgres:16 --format '{{json .RepoDigests}}' > postgres-image-digests.txt
```

The results already record the PostgreSQL and Python versions, CPU and core count. Python's random generator is seeded, but PostgreSQL's `random()` is not seeded by this script, so generated database values are intentionally non-deterministic across runs.

## Troubleshooting

| Symptom | Action |
| --- | --- |
| `ModuleNotFoundError: psycopg2` | Activate `.venv` and repeat the dependency installation. |
| Connection refused | Check `docker ps`, wait for `pg_isready`, and verify `FEAS_DSN`. |
| Unix-socket connection error | Export the explicit TCP DSN above. |
| Password authentication failed | Use the credentials supplied when the container was first created. |
| Container name already in use | Run `docker start trace-feas-postgres` for the existing pilot container. |
| Port 5433 already allocated | Choose another host port in `docker run` and update `FEAS_DSN` to match. |
| Interrupted run or assertion failure | Preserve the terminal error, ensure no other driver is running, and rerun against the dedicated database. JSON is written only after all five experiments finish. |

## Stop or remove the test environment

Stop the container when finished:

```bash
docker stop trace-feas-postgres
```

To discard the test database completely, remove its container and anonymous volumes:

```bash
docker rm -v trace-feas-postgres
```

Copy any results you need before deleting the Codespace. The database container is disposable; result files are stored in the repository workspace.
