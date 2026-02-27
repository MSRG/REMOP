//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/remon_allocator.hpp
//
// Shared REMON VMM singleton for integration between allocator and buffer manager
// Also provides global allocator metrics for timing (works for both OS and REMON)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/buffer/buffer_pool_profiler.hpp"
#include <chrono>
#include <atomic>

namespace duckdb {

//! Global profiler metrics for allocation timing (thread-safe atomic access)
//! This allows the allocator (which doesn't have access to BufferPool) to record timing
//! Works for both OS malloc and REMON allocations
inline BufferPoolMetrics *&GetGlobalAllocatorMetrics() {
	static BufferPoolMetrics *metrics = nullptr;
	return metrics;
}

//! Set the global allocator metrics pointer (called by BufferPool when profiling enabled)
inline void SetGlobalAllocatorMetrics(BufferPoolMetrics *metrics) {
	GetGlobalAllocatorMetrics() = metrics;
}

} // namespace duckdb

#ifdef REMON_MALLOC
#include "remon.h"
// glibc's sys/mman.h defines MAP_TYPE; this collides with DuckDB template identifiers.
#ifdef MAP_TYPE
#undef MAP_TYPE
#endif

namespace duckdb {

//! Get the singleton REMON VMM instance
//! This function is thread-safe (C++11 guarantees thread-safe initialization of static locals)
//! The same instance is used by both the allocator and buffer manager
inline remon_vmm &GetRemonVMM() {
	static remon_vmm instance;
	return instance;
}

//! Safe remon_malloc wrapper (timing is done in DefaultAllocate)
inline void *SafeRemonMalloc(idx_t size) {
	return GetRemonVMM().remon_malloc(size);
}

//! Safe remon_free wrapper (timing is done in DefaultFree)
inline void SafeRemonFree(void *ptr) {
	GetRemonVMM().remon_free(ptr);
}

//===--------------------------------------------------------------------===//
// Buffer Manager Integration APIs
//===--------------------------------------------------------------------===//

//! Demote a block to REMON (hint for eviction to remote memory)
//! This moves the pod to the tail of REMON's LRU list
//! Returns 0 if already evicted, 1 if demoted successfully
inline int RemonDemote(void *ptr) {
	return GetRemonVMM().remonDemote(ptr);
}

//! Synchronously activate a block (fetch from remote/disk if evicted)
//! Blocks until the data is available in local memory
//! Returns 1 if fetched, 0 if already local, -1 on error
inline int RemonActivateSync(void *ptr) {
	return GetRemonVMM().remonActivateSync(ptr);
}

//! Clean up evicted state before freeing a block
//! Call this when destroying a BlockHandle that was demoted to REMON
//! This properly cleans up remote/disk resources
inline void RemonCleanupEvicted(void *ptr) {
	GetRemonVMM().remon_cleanup_evicted(ptr);
}

} // namespace duckdb

#endif // REMON_MALLOC
