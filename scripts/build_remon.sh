#!/usr/bin/env bash
#
# build_remon.sh — Build DuckDB with or without REMON integration.
#
# Usage:
#   ./scripts/build_remon.sh              # Build with REMON (default)
#   ./scripts/build_remon.sh --no-remon   # Build vanilla DuckDB
#   ./scripts/build_remon.sh --clean       # Clean and rebuild with REMON
#   ./scripts/build_remon.sh --debug       # Debug build with REMON
#
# The script expects to be run from the DuckDB root directory (or auto-detects
# it from the script location).
#
set -euo pipefail

# ── Locate project root ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKDB_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

REMON_SRC="$DUCKDB_ROOT/cdb/compute_node"
REMON_BUILD="$REMON_SRC/build-release"

BUILD_TYPE="Release"
WITH_REMON=1
CLEAN=0
BUILD_EXTENSIONS="${BUILD_EXTENSIONS:-parquet;tpch;tpcds;jemalloc}"

# ── Parse arguments ──────────────────────────────────────────────────────────
for arg in "$@"; do
  case "$arg" in
    --no-remon)  WITH_REMON=0 ;;
    --clean)     CLEAN=1 ;;
    --debug)     BUILD_TYPE="Debug" ;;
    --release)   BUILD_TYPE="Release" ;;
    -h|--help)
      head -14 "$0" | tail -11
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      exit 1
      ;;
  esac
done

# ── Determine parallelism ────────────────────────────────────────────────────
if command -v nproc &>/dev/null; then
  JOBS=$(nproc)
elif command -v sysctl &>/dev/null; then
  JOBS=$(sysctl -n hw.ncpu 2>/dev/null || echo 4)
else
  JOBS=4
fi

# ── Helper ────────────────────────────────────────────────────────────────────
banner() { echo -e "\n\033[1;36m==> $1\033[0m"; }

# ── Build libremon.so first (if needed) ──────────────────────────────────────
if [[ $WITH_REMON -eq 1 ]]; then
  banner "Building REMON compute_node (libremon.so)"

  # Initialize submodule if not already present
  if [[ ! -f "$REMON_SRC/remon.h" ]]; then
    banner "Initializing REMON submodule"
    (cd "$DUCKDB_ROOT" && git submodule update --init)
  fi

  if [[ ! -f "$REMON_SRC/remon.h" ]]; then
    echo "ERROR: REMON source not found at $REMON_SRC" >&2
    echo "       Run: git submodule update --init" >&2
    echo "       (Requires access to https://github.com/MSRG/cdb)" >&2
    exit 1
  fi

  if [[ $CLEAN -eq 1 && -d "$REMON_BUILD" ]]; then
    echo "  Cleaning $REMON_BUILD"
    rm -rf "$REMON_BUILD"
  fi

  mkdir -p "$REMON_BUILD"
  cmake -S "$REMON_SRC" -B "$REMON_BUILD" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
  cmake --build "$REMON_BUILD" -- -j "$JOBS"

  # Verify the library was produced
  if [[ ! -f "$REMON_BUILD/libremon.so" ]]; then
    echo "ERROR: libremon.so not found after build" >&2
    exit 1
  fi
  echo "  libremon.so ready at $REMON_BUILD/libremon.so"
fi

# ── Build DuckDB ─────────────────────────────────────────────────────────────
DUCKDB_BUILD="$DUCKDB_ROOT/build"

if [[ $CLEAN -eq 1 && -d "$DUCKDB_BUILD" ]]; then
  banner "Cleaning DuckDB build directory"
  rm -rf "$DUCKDB_BUILD"
fi

mkdir -p "$DUCKDB_BUILD"

CMAKE_FLAGS=(
  -DCMAKE_BUILD_TYPE="$BUILD_TYPE"
  -DBUILD_EXTENSIONS="$BUILD_EXTENSIONS"
)

if [[ $WITH_REMON -eq 1 ]]; then
  banner "Configuring DuckDB with REMON"
  CMAKE_FLAGS+=( -DBUILD_WITH_REMON=ON )
else
  banner "Configuring DuckDB (vanilla, no REMON)"
  CMAKE_FLAGS+=( -DBUILD_WITH_REMON=OFF )
fi

cmake -S "$DUCKDB_ROOT" -B "$DUCKDB_BUILD" "${CMAKE_FLAGS[@]}"

banner "Building DuckDB (-j $JOBS)"
cmake --build "$DUCKDB_BUILD" -- -j "$JOBS"

# ── Summary ──────────────────────────────────────────────────────────────────
banner "Build complete"
echo "  Build type : $BUILD_TYPE"
echo "  REMON      : $([ $WITH_REMON -eq 1 ] && echo 'ON' || echo 'OFF')"
echo "  Extensions : $BUILD_EXTENSIONS"
echo "  DuckDB bin : $DUCKDB_BUILD/duckdb"
if [[ $WITH_REMON -eq 1 ]]; then
  echo "  libremon   : $REMON_BUILD/libremon.so"
  echo ""
  echo "  NOTE: Set LD_LIBRARY_PATH to include libremon.so at runtime:"
  echo "    export LD_LIBRARY_PATH=$REMON_BUILD:\$LD_LIBRARY_PATH"
fi
