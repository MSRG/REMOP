# REMOP / REMON Build & Run Instructions (Ubuntu 20.04, 2-node)

This document describes reproducible setup/build/test steps for:
- DuckDB baseline (`no-remon`) with `tpch` + `tpcds`
- DuckDB with REMON integration (`BUILD_WITH_REMON=ON`)
- REMON memory node build on a separate node

Validated environment in this project:
- OS: Ubuntu 20.04
- Compiler: GCC/G++ 9.3.0
- CMake: 3.16.3
- Python: 3.8+
- Perl: 5.30+

## 1. Node Roles
- Compute node: runs DuckDB (`apt056`, private IP `10.10.1.1`)
- Memory node: runs REMON memory service (`apt014`, private IP `10.10.1.2`)

## 2. Storage Note
If your workspace is already on the large data partition (e.g., `/mnt/remop/workspace` on `/dev/sda4`), no extra disk mounting is needed.

## 3. Dependencies
Install on both nodes (minimum required for this repo path):

```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential \
  cmake \
  git \
  python3 \
  perl \
  libssl-dev \
  libtbb-dev \
  valgrind \
  libfuse2 \
  pkg-config \
  jq \
  ninja-build
```

Notes:
- `cmake >= 3.16.3` is required by `cdb` CMake files.
- `libssl-dev` is required by `cdb/compute_node` (`find_package(OpenSSL REQUIRED)`).
- `libtbb-dev` is required for `cdb/tests`.

If `apt-get update` fails on an unrelated third-party repo (e.g., Grafana key issues), disable that repo entry under `/etc/apt/sources.list.d/` and retry.

## 4. Sync Code to Memory Node
From compute node:

```bash
# Use private network IPs between CloudLab nodes
scp -r /path/to/duckdb SqZhang1@10.10.1.2:~/duckdb
```

(For large trees, tar/rsync is faster.)

## 5. Build Memory Node (apt014)
On memory node:

```bash
cd ~/duckdb
cmake -S cdb/memory_node -B cdb/memory_node/build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cdb/memory_node/build-release -- -j "$(nproc)"
```

Binary:
- `~/duckdb/cdb/memory_node/build-release/memory_node`

## 6. Build DuckDB (no-remon) with TPCH/TPCDS
On compute node:

```bash
cd /path/to/duckdb
cmake -S . -B build-noremon-tpch-tpcds \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_WITH_REMON=OFF \
  -DBUILD_EXTENSIONS='tpch;tpcds;parquet;jemalloc'
cmake --build build-noremon-tpch-tpcds -- -j "$(nproc)"
```

### no-remon smoke test (validated)
```bash
cd /path/to/duckdb
./build-noremon-tpch-tpcds/duckdb -csv -c \
"SELECT extension_name, loaded, installed \
 FROM duckdb_extensions() \
 WHERE extension_name IN ('tpch','tpcds') \
 ORDER BY extension_name;"

./build-noremon-tpch-tpcds/duckdb -csv -c \
"CALL dbgen(sf=0.01); SELECT COUNT(*) AS lineitem_rows FROM lineitem;"

./build-noremon-tpch-tpcds/duckdb -csv -c \
"CALL dsdgen(sf=0.01); \
 SELECT COUNT(*) AS customer_rows FROM customer; \
 SELECT COUNT(*) AS store_sales_rows FROM store_sales;"
```

Observed in this workspace:
- `tpch`, `tpcds` both loaded/installed
- `lineitem_rows = 60175` (sf=0.01)
- `customer_rows = 1000`, `store_sales_rows = 28810` (tpcds sf=0.01)

### no-remon TPCH/TPCDS profiled report (validated)
```bash
cd /path/to/duckdb
./eval/scripts/run_noremon_tpch_tpcds_reports.sh
```

Report/output artifacts:
- `eval/results/no_remon_tpch_tpcds_report.md`
- `eval/results/no_remon_tpch_q01_profile.json`
- `eval/results/no_remon_tpcds_q01_profile.json`
- `eval/results/no_remon_tpch_q01_time.txt`
- `eval/results/no_remon_tpcds_q01_time.txt`

Current run (SF=0.1, Q01):
- TPCH Q01 profile timing: `0.016923s`, wall elapsed: `0.03s`
- TPCDS Q01 profile timing: `0.026264s`, wall elapsed: `0.04s`

## 7. Build DuckDB with REMON
Use helper script (updated to use `BUILD_EXTENSIONS` correctly):

```bash
cd /path/to/duckdb
./scripts/build_remon.sh --clean
# default extensions from script:
# parquet;tpch;tpcds;jemalloc
```

Or explicitly:

```bash
cd /path/to/duckdb
cmake -S . -B build-remon \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_WITH_REMON=ON \
  -DBUILD_EXTENSIONS='tpch;tpcds;parquet;jemalloc'
cmake --build build-remon -- -j "$(nproc)"
```

## 8. Run Memory Node
On memory node (`apt014`):

```bash
cd ~/duckdb
mkdir -p ~/duckdb/cdb/memory_node/build-release/remon_swap
./cdb/memory_node/build-release/memory_node
```

Or background mode:

```bash
nohup ~/duckdb/cdb/memory_node/build-release/memory_node \
  > ~/memory_node.log 2>&1 &
```

## 9. Compute-Side REMON Config + Run
REMON reads config from the same directory as the DuckDB executable (`<build_dir>/remon.config`).

Example (`/path/to/duckdb/build/remon.config`):

```text
MAX_RAM_IN_GB: 1
MAX_VM_IN_GB: 16
PEER: 10.10.1.2
SWAP_DISK: /path/to/duckdb/build/remon_swap
PAGE_SIZE: 262144
TOP_LEVEL_ARENA: 8
```

Before running DuckDB with REMON, create and verify the compute-side swap directory:

```bash
mkdir -p /path/to/duckdb/build/remon_swap
ls -ld /path/to/duckdb/build/remon_swap
```

Run on compute node:

```bash
cd /path/to/duckdb
export LD_LIBRARY_PATH=/path/to/duckdb/cdb/compute_node/build-release:$LD_LIBRARY_PATH
./build-remon/duckdb
```

Or use the eval helper (copies `eval/configs/remon_apt056_to_apt014.config` into the build dir):

```bash
cd /path/to/duckdb
OUT_PREFIX=remon_smoke ./eval/scripts/run_remon_smoke.sh
```

Output artifacts:
- `eval/results/remon_smoke.stdout`
- `eval/logs/compute/remon_smoke.stderr`

## 10. Current Branch Status
- `cdb/memory_node` had an immediate startup crash when linked fully static (`-static`).
  - Fixed by removing full static link in `cdb/memory_node/CMakeLists.txt`.
- REMON compute startup no longer hangs after connection when swap paths exist and `SWAP_DISK` is valid.
- Validated on this workspace:
  - `SELECT 42` returns on REMON-enabled build.
  - TPCH smoke query (`CALL dbgen(sf=0.01); SELECT COUNT(*) FROM lineitem;`) returns `60175`.

## 11. Evaluation Workspace
Use `duckdb/eval/`:
- `eval/scripts/`
- `eval/configs/`
- `eval/logs/compute/`
- `eval/logs/memory/`
- `eval/results/`
- `eval/data/`
