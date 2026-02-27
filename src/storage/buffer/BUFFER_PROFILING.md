# Buffer Manager Profiling

This document describes the buffer manager profiling capabilities added to DuckDB.

## Overview

The buffer manager profiling system provides detailed insights into memory management, swap I/O, evictions, and buffer pool operations. This is particularly useful for:

- Understanding memory pressure and swap behavior
- Optimizing memory-intensive queries
- Debugging out-of-memory issues
- Analyzing the effectiveness of memory-aware operators

## Metrics Tracked

### Swap/Disk I/O Metrics
- **swap_write_count**: Number of blocks written to disk (evictions)
- **swap_write_bytes**: Total bytes written to disk
- **swap_read_count**: Number of blocks read from disk (reloads)
- **swap_read_bytes**: Total bytes read from disk

### Eviction Metrics
- **eviction_attempt_count**: Number of times eviction was triggered
- **eviction_success_count**: Number of successful evictions
- **eviction_failure_count**: Number of failed evictions
- **blocks_evicted_count**: Total number of blocks evicted

### Memory Allocation Metrics
- **allocation_count**: Number of allocation requests
- **allocation_bytes**: Total bytes allocated
- **pin_count**: Number of pin operations
- **unpin_count**: Number of unpin operations
- **peak_memory_usage**: Maximum memory used during execution

### Buffer Pool Metrics
- **buffer_reuse_count**: Number of times a buffer was reused during eviction
- **purge_count**: Number of eviction queue purge operations
- **purge_nodes_processed**: Total nodes processed during purges

### Per-Tag Metrics
- **swap_write_per_tag**: Swap writes broken down by MemoryTag
- **swap_read_per_tag**: Swap reads broken down by MemoryTag

## C++ API Usage

```cpp
#include "duckdb.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"

using namespace duckdb;

// Get buffer manager from connection
auto &buffer_manager = StandardBufferManager::GetBufferManager(con.context->db->GetDatabase());

// Enable profiling
buffer_manager.SetProfiling(true);

// Run queries...
con.Query("SELECT * FROM large_table");

// Get profiling report
std::string report = buffer_manager.GetProfilingReport();
std::cout << report << std::endl;

// Get raw metrics for custom analysis
const BufferPoolMetrics *metrics = buffer_manager.GetProfilingMetrics();
if (metrics) {
    std::cout << "Peak memory: " << metrics->peak_memory_usage.load() << " bytes" << std::endl;
    std::cout << "Swap writes: " << metrics->swap_write_count.load() << std::endl;
}

// Reset metrics
buffer_manager.ResetProfilingMetrics();

// Disable profiling
buffer_manager.SetProfiling(false);
```

## Example Output

```
Buffer Pool Profiling Metrics:
================================

Swap/Disk I/O:
  Writes: 245 blocks (512.3 MB)
  Reads:  123 blocks (256.7 MB)

Evictions:
  Attempts:       67
  Successes:      65
  Failures:       2
  Blocks Evicted: 245

Operations:
  Allocations: 15234 (3.2 GB)
  Pins:        45678
  Unpins:      45234
  Buffer Reuses: 34

Memory:
  Peak Usage:  768.5 MB

Eviction Queue:
  Purges:          23
  Nodes Processed: 94208
```

## Performance Considerations

- Profiling adds minimal overhead (atomic increment operations)
- Metrics are thread-safe using atomic operations
- When profiling is disabled, no overhead is incurred
- Recommended to enable only when needed for analysis

## Implementation Details

### Key Files
- `buffer_pool_profiler.hpp`: Metrics structure and definitions
- `buffer_pool.cpp`: BufferPool instrumentation
- `standard_buffer_manager.cpp`: StandardBufferManager instrumentation

### Instrumentation Points
- **BufferPool::EvictBlocks()**: Tracks eviction attempts, successes, failures
- **BufferPool::UpdateUsedMemory()**: Tracks peak memory usage
- **BufferPool::PurgeQueue()**: Tracks queue purge operations
- **StandardBufferManager::Allocate()**: Tracks allocations
- **StandardBufferManager::Pin/Unpin()**: Tracks pin/unpin operations
- **StandardBufferManager::WriteTemporaryBuffer()**: Tracks swap writes
- **StandardBufferManager::ReadTemporaryBuffer()**: Tracks swap reads

## Use Cases

### 1. Analyzing Memory-Intensive Queries

```cpp
buffer_manager.SetProfiling(true);
buffer_manager.ResetProfilingMetrics();

// Run memory-intensive query
con.Query("SELECT * FROM huge_table JOIN another_huge_table USING (id)");

// Check if swap occurred
auto metrics = buffer_manager.GetProfilingMetrics();
if (metrics->swap_write_count > 0) {
    std::cout << "Query triggered " << metrics->swap_write_count 
              << " swap writes (" << (metrics->swap_write_bytes / (1024.0 * 1024.0)) 
              << " MB)" << std::endl;
}
```

### 2. Monitoring Eviction Effectiveness

```cpp
auto metrics = buffer_manager.GetProfilingMetrics();
double eviction_success_rate = (double)metrics->eviction_success_count / 
                                metrics->eviction_attempt_count;
std::cout << "Eviction success rate: " << (eviction_success_rate * 100) << "%" << std::endl;

if (metrics->eviction_failure_count > 0) {
    std::cout << "Warning: " << metrics->eviction_failure_count 
              << " eviction failures detected" << std::endl;
}
```

### 3. Debugging Memory-Aware Operators

When developing memory-aware operators (like the optimized NLJ), profiling helps verify:
- Block eviction behavior under memory pressure
- Effectiveness of result buffering
- Impact of memory budget configurations

```cpp
// Enable profiling
buffer_manager.SetProfiling(true);

// Run blocked NLJ with specific memory budget
con.Query("SET operator_memory_budget='100MB'");
con.Query("SELECT * FROM lhs JOIN rhs ON lhs.a IS DISTINCT FROM rhs.b");

// Analyze behavior
std::cout << buffer_manager.GetProfilingReport() << std::endl;
```

## Future Enhancements

Potential extensions to the profiling system:
- Time-series tracking of metrics
- Per-query profiling breakdown
- Integration with DuckDB's existing query profiler
- Histogram of eviction queue sizes
- Latency tracking for pin/unpin operations
- Per-operator memory usage attribution

## See Also

- [Buffer Manager Architecture](../storage/buffer_manager.hpp)
- [Memory Management Guide](../docs/memory_management.md)
- [Operator Memory Optimizations](../execution/operator_memory_policy.hpp)
