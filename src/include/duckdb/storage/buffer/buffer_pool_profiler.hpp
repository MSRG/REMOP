//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer/buffer_pool_profiler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include <sstream>

namespace duckdb {

//! BufferPoolMetrics tracks profiling statistics for buffer pool operations
//! including swap I/O, evictions, allocations, and memory usage
struct BufferPoolMetrics {
	//===--------------------------------------------------------------------===//
	// Swap/Disk I/O Metrics
	//===--------------------------------------------------------------------===//
	//! Number of blocks written to disk (evictions)
	atomic<idx_t> swap_write_count {0};
	//! Total bytes written to disk
	atomic<idx_t> swap_write_bytes {0};
	//! Number of blocks read from disk (reloads)
	atomic<idx_t> swap_read_count {0};
	//! Total bytes read from disk
	atomic<idx_t> swap_read_bytes {0};
	//! Number of persistent database block reads
	atomic<idx_t> db_read_count {0};
	//! Total bytes read from persistent database file
	atomic<idx_t> db_read_bytes {0};
	//! Number of persistent database block writes
	atomic<idx_t> db_write_count {0};
	//! Total bytes written to persistent database file
	atomic<idx_t> db_write_bytes {0};

	//===--------------------------------------------------------------------===//
	// Eviction Metrics
	//===--------------------------------------------------------------------===//
	//! Number of times eviction was triggered
	atomic<idx_t> eviction_attempt_count {0};
	//! Number of successful evictions
	atomic<idx_t> eviction_success_count {0};
	//! Number of failed evictions (couldn't free enough memory)
	atomic<idx_t> eviction_failure_count {0};
	//! Total number of blocks evicted
	atomic<idx_t> blocks_evicted_count {0};

	//===--------------------------------------------------------------------===//
	// Memory Allocation Metrics
	//===--------------------------------------------------------------------===//
	//! Number of allocation requests
	atomic<idx_t> allocation_count {0};
	//! Total bytes allocated
	atomic<idx_t> allocation_bytes {0};
	//! Number of pin operations
	atomic<idx_t> pin_count {0};
	//! Number of unpin operations
	atomic<idx_t> unpin_count {0};
	//! Maximum memory used during execution
	atomic<idx_t> peak_memory_usage {0};

	//===--------------------------------------------------------------------===//
	// Buffer Pool Metrics
	//===--------------------------------------------------------------------===//
	//! Number of times a buffer was reused during eviction
	atomic<idx_t> buffer_reuse_count {0};
	//! Number of eviction queue purge operations
	atomic<idx_t> purge_count {0};
	//! Total nodes processed during purges
	atomic<idx_t> purge_nodes_processed {0};

	//===--------------------------------------------------------------------===//
	// Per-Tag Swap Metrics (optional, for detailed analysis)
	//===--------------------------------------------------------------------===//
	//! Number of swap writes per memory tag
	atomic<idx_t> swap_write_per_tag[MEMORY_TAG_COUNT];
	//! Number of swap reads per memory tag
	atomic<idx_t> swap_read_per_tag[MEMORY_TAG_COUNT];

	//===--------------------------------------------------------------------===//
	// Transfer Round Metrics (operator-level, for paper's C_read/C_write)
	//===--------------------------------------------------------------------===//
	//! Number of logical read rounds (inner table block loads, input buffer refills)
	atomic<idx_t> transfer_read_rounds {0};
	//! Number of logical write rounds (output buffer flushes)
	atomic<idx_t> transfer_write_rounds {0};

	//===--------------------------------------------------------------------===//
	// Timing Metrics (in microseconds, for latency breakdown)
	//===--------------------------------------------------------------------===//
	//! Total time spent in swap write operations (eviction to disk/remote)
	atomic<uint64_t> swap_write_time_us {0};
	//! Total time spent in swap read operations (reload from disk/remote)
	atomic<uint64_t> swap_read_time_us {0};
	//! Total time spent in persistent DB read operations
	atomic<uint64_t> db_read_time_us {0};
	//! Total time spent in persistent DB write operations
	atomic<uint64_t> db_write_time_us {0};
	//! Total time spent in allocation operations
	atomic<uint64_t> allocation_time_us {0};
	//! Total time spent in free operations
	atomic<uint64_t> free_time_us {0};

	//! Reset all metrics to zero
	void Reset() {
		swap_write_count = 0;
		swap_write_bytes = 0;
		swap_read_count = 0;
		swap_read_bytes = 0;
		db_read_count = 0;
		db_read_bytes = 0;
		db_write_count = 0;
		db_write_bytes = 0;

		eviction_attempt_count = 0;
		eviction_success_count = 0;
		eviction_failure_count = 0;
		blocks_evicted_count = 0;

		allocation_count = 0;
		allocation_bytes = 0;
		pin_count = 0;
		unpin_count = 0;
		peak_memory_usage = 0;

		buffer_reuse_count = 0;
		purge_count = 0;
		purge_nodes_processed = 0;

		for (idx_t i = 0; i < MEMORY_TAG_COUNT; i++) {
			swap_write_per_tag[i] = 0;
			swap_read_per_tag[i] = 0;
		}

		// Transfer round metrics
		transfer_read_rounds = 0;
		transfer_write_rounds = 0;

		// Timing metrics
		swap_write_time_us = 0;
		swap_read_time_us = 0;
		db_read_time_us = 0;
		db_write_time_us = 0;
		allocation_time_us = 0;
		free_time_us = 0;
	}

	//! Convert metrics to human-readable string
	string ToString() const {
		std::ostringstream ss;
		ss << "Buffer Pool Profiling Metrics:\n";
		ss << "================================\n\n";

		// Swap/Disk I/O
		ss << "Swap/Disk I/O:\n";
		ss << "  Writes: " << swap_write_count.load() << " blocks ("
		   << (swap_write_bytes.load() / (1024.0 * 1024.0)) << " MB)\n";
		ss << "  Reads:  " << swap_read_count.load() << " blocks ("
		   << (swap_read_bytes.load() / (1024.0 * 1024.0)) << " MB)\n\n";
		ss << "Persistent DB I/O:\n";
		ss << "  Writes: " << db_write_count.load() << " blocks (" << (db_write_bytes.load() / (1024.0 * 1024.0))
		   << " MB)\n";
		ss << "  Reads:  " << db_read_count.load() << " blocks (" << (db_read_bytes.load() / (1024.0 * 1024.0))
		   << " MB)\n\n";

		// Evictions
		ss << "Evictions:\n";
		ss << "  Attempts:       " << eviction_attempt_count.load() << "\n";
		ss << "  Successes:      " << eviction_success_count.load() << "\n";
		ss << "  Failures:       " << eviction_failure_count.load() << "\n";
		ss << "  Blocks Evicted: " << blocks_evicted_count.load() << "\n\n";

		// Operations
		ss << "Operations:\n";
		ss << "  Allocations: " << allocation_count.load() << " ("
		   << (allocation_bytes.load() / (1024.0 * 1024.0)) << " MB)\n";
		ss << "  Pins:        " << pin_count.load() << "\n";
		ss << "  Unpins:      " << unpin_count.load() << "\n";
		ss << "  Buffer Reuses: " << buffer_reuse_count.load() << "\n\n";

		// Memory
		ss << "Memory:\n";
		ss << "  Peak Usage:  " << (peak_memory_usage.load() / (1024.0 * 1024.0)) << " MB\n\n";

		// Eviction Queue
		ss << "Eviction Queue:\n";
		ss << "  Purges:          " << purge_count.load() << "\n";
		ss << "  Nodes Processed: " << purge_nodes_processed.load() << "\n\n";

		// Transfer Rounds (operator-level, for paper metrics)
		ss << "Transfer Rounds:\n";
		ss << "  Read Rounds:  " << transfer_read_rounds.load() << "\n";
		ss << "  Write Rounds: " << transfer_write_rounds.load() << "\n";
		ss << "  Total Rounds: " << (transfer_read_rounds.load() + transfer_write_rounds.load()) << "\n\n";

		// Data Transfer (page counts for paper metrics)
		ss << "Data Transfer:\n";
		ss << "  Pages Written: " << swap_write_count.load() << "\n";
		ss << "  Pages Read:    " << swap_read_count.load() << "\n";
		ss << "  Total Pages:   " << (swap_write_count.load() + swap_read_count.load()) << "\n\n";

		// Timing (for latency breakdown)
		uint64_t total_swap_us = swap_write_time_us.load() + swap_read_time_us.load();
		uint64_t total_db_io_us = db_write_time_us.load() + db_read_time_us.load();
		uint64_t total_alloc_us = allocation_time_us.load() + free_time_us.load();
		uint64_t total_memory_op_us = total_swap_us + total_db_io_us + total_alloc_us;
		ss << "Timing:\n";
		ss << "  Swap Write Time:  " << (swap_write_time_us.load() / 1000.0) << " ms\n";
		ss << "  Swap Read Time:   " << (swap_read_time_us.load() / 1000.0) << " ms\n";
		ss << "  Total Swap Time:  " << (total_swap_us / 1000.0) << " ms\n";
		ss << "  DB Write Time:    " << (db_write_time_us.load() / 1000.0) << " ms\n";
		ss << "  DB Read Time:     " << (db_read_time_us.load() / 1000.0) << " ms\n";
		ss << "  Total DB I/O:     " << (total_db_io_us / 1000.0) << " ms\n";
		ss << "  Alloc Time:       " << (allocation_time_us.load() / 1000.0) << " ms\n";
		ss << "  Free Time:        " << (free_time_us.load() / 1000.0) << " ms\n";
		ss << "  Total Alloc Time: " << (total_alloc_us / 1000.0) << " ms\n";
		ss << "  Total Memory Ops: " << (total_memory_op_us / 1000.0) << " ms\n";

		return ss.str();
	}

	//! Get per-tag swap statistics
	string GetPerTagSwapStats() const {
		std::ostringstream ss;
		ss << "\nPer-Tag Swap Statistics:\n";
		ss << "========================\n";
		for (idx_t i = 0; i < MEMORY_TAG_COUNT; i++) {
			idx_t writes = swap_write_per_tag[i].load();
			idx_t reads = swap_read_per_tag[i].load();
			if (writes > 0 || reads > 0) {
				ss << "  Tag[" << i << "]:\n";
				ss << "    Writes: " << writes << "\n";
				ss << "    Reads:  " << reads << "\n";
			}
		}
		return ss.str();
	}
};

} // namespace duckdb
