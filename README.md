# REMOP: Remote-Memory-aware Operator Optimization

REMOP is an operator optimization framework for database query
processing under memory disaggregation. When local memory is limited and
an operator must swap pages to a remote memory pool, each swap event
pays a fixed network round-trip cost. REMOP minimizes the number of
these *transfer rounds* by tuning how operators partition their internal
buffers.

REMOP is implemented in [DuckDB v1.0.0](https://github.com/duckdb/duckdb/tree/v1.0.0)
with [REMON](https://github.com/MSRG/cdb) (Remote Memory over the Network)
as the remote memory backend.


## Repository Layout

```
├── cdb/                     # REMON submodule
│   ├── compute_node/        #   compute node backend
│   └── memory_node/         #   remote memory node server
├── src/
│   ├── execution/
│   │   ├── operator/join/   # Optimized blocked NLJ operators
│   │   ├── operator/order/  # Optimized k-way merge sort operator
│   │   ├── operator_memory_policy.cpp
│   │   └── physical_plan/   # Plan-level integration
│   └── common/sort/         # K-way merge sorter, partitioner, merge tree
└── scripts/
    └── build_remon.sh 
```


## Prerequisites

- OS: Ubuntu 20.04+
- Compiler: GCC/G++ 9.3.0+
- CMake ≥ 3.16.3

```bash
sudo apt-get install -y build-essential cmake git libssl-dev
```

## Quick Start

### REMON Submodule

To add the REMON submodule, clone REMOP with `--recursive`, or run the following after cloning:

```bash
git submodule update --init
```

### Build Without REMON

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -- -j "$(nproc)"
./build/duckdb
```

### Build With REMON

```bash
./scripts/build_remon.sh --clean
```

Or manually:

```bash
# 1. Build libremon.so
cmake -S cdb/compute_node -B cdb/compute_node/build-release \
  -DCMAKE_BUILD_TYPE=Release
cmake --build cdb/compute_node/build-release -- -j "$(nproc)"

# 2. Build DuckDB with REMON
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_WITH_REMON=ON
cmake --build build -- -j "$(nproc)"
```

### Run With REMON

Before starting DuckDB, set up and run the memory node on the
remote host. For REMON configuration (memory node setup, config file,
two-node deployment), please see the [REMON README](https://github.com/MSRG/cdb).

```bash
export LD_LIBRARY_PATH=cdb/compute_node/build-release:$LD_LIBRARY_PATH
./build/duckdb
```


## REMOP Runtime Settings

REMOP exposes its operator policies as DuckDB runtime settings:

```sql
SET threads = 16;
SET operator_memory_optimizations = true;
SET operator_memory_budget = '64MB';              
SET operator_nlj_input_buffer_ratio = 0.4;       
SET operator_nlj_short_table_buffer_ratio = 0.6;  
SET operator_kway_merge_k = 16;                   
SET operator_sort_input_buffer_ratio = 0.6;       
```

When `operator_memory_optimizations` is enabled, the query planner
substitutes the default NLJ and sort operators with REMOP's optimized
variants.

## License

REMOP inherits DuckDB's [MIT License](LICENSE).
