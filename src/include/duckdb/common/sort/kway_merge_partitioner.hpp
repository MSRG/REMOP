//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sort/kway_merge_partitioner.hpp
//
// K-way merge partitioner for parallel k-way merge sort
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/atomic.hpp"
#include <unordered_map>

namespace duckdb {

//! Partition range for a single run within a partition
struct KWayRunPartition {
	//! Start position (global index in run, inclusive)
	idx_t start_idx = 0;
	//! End position (global index in run, exclusive)
	idx_t end_idx = 0;

	//! Number of elements in this partition from this run
	idx_t Count() const {
		return end_idx - start_idx;
	}
};

//! A complete partition across all k runs
struct KWayPartition {
	//! Partition index (for ordering results)
	idx_t partition_idx = 0;
	//! Partition ranges for each run
	vector<KWayRunPartition> run_partitions;
	//! Total elements in this partition (sum across all runs)
	idx_t total_elements = 0;
};

//! Computes partition boundaries for parallel k-way merge using binary search
//!
//! Algorithm: For each partition target T (cumulative elements), find positions
//! (p₀, p₁, ..., p_{k-1}) in k runs such that Σpᵢ = T and all elements before
//! these positions are smaller than all elements after.
//!
//! Uses binary search with key bound narrowing for efficiency:
//! - Process runs sequentially
//! - Each run's partition point constrains subsequent runs
//! - Key bounds narrow the search space progressively
//!
//! Complexity: O(P × k² × log²(N/k)) worst case, better with narrowing
class KWayMergePartitioner {
public:
	//! Create a partitioner
	//! @param state Global sort state (for sort layout, external flag)
	//! @param buffer_manager Buffer manager for pinning blocks
	//! @param partition_size Target number of elements per partition
	KWayMergePartitioner(GlobalSortState &state, BufferManager &buffer_manager, idx_t partition_size);

	//! Compute all partition boundaries for the given runs
	//! @param runs Vector of sorted blocks to partition
	void ComputePartitions(const vector<SortedBlock *> &runs);

	//! Get next partition (thread-safe work stealing)
	//! @param partition Output parameter for the partition info
	//! @return true if a partition was claimed, false if no more partitions
	bool GetNextPartition(KWayPartition &partition);

	//! Total number of partitions
	idx_t NumPartitions() const {
		return partitions.size();
	}

	//! Check if partitions have been computed
	bool IsInitialized() const {
		return !partitions.empty();
	}

private:
	//===--------------------------------------------------------------------===//
	// Partition Point Finding
	//===--------------------------------------------------------------------===//

	//! Find partition boundaries for target merged position T
	//! @param runs Vector of sorted blocks
	//! @param target Target cumulative element count
	//! @param prev_positions Previous partition's end positions (start of this partition)
	//! @return End positions in each run for this partition
	vector<idx_t> FindPartitionPoint(const vector<SortedBlock *> &runs, idx_t target,
	                                 const vector<idx_t> &prev_positions);

	//! Binary search for upper_bound of key in a run, within narrowed range
	//! @param run The sorted block to search
	//! @param key The key to compare against
	//! @param search_lo Lower bound of search range (inclusive)
	//! @param search_hi Upper bound of search range (exclusive)
	//! @return First position where element > key (or search_hi if all <= key)
	idx_t UpperBoundInRun(SortedBlock *run, data_ptr_t key, idx_t search_lo, idx_t search_hi);

	//! Get key pointer at a global position in a run
	//! @param run The sorted block
	//! @param global_idx Global element index
	//! @return Pointer to the key data
	data_ptr_t GetKeyAtPosition(SortedBlock *run, idx_t global_idx);

	//! Compare two keys using sort layout
	//! @return negative if key_a < key_b, 0 if equal, positive if key_a > key_b
	int CompareKeys(data_ptr_t key_a, data_ptr_t key_b);

	//===--------------------------------------------------------------------===//
	// Utility Functions
	//===--------------------------------------------------------------------===//

	//! Convert global position to block/entry indices
	void GlobalToLocal(SortedBlock *run, idx_t global_idx, idx_t &block_idx, idx_t &entry_idx);

	//! Pin a block and get pointer to key at entry
	data_ptr_t PinAndGetKey(SortedBlock *run, idx_t block_idx, idx_t entry_idx, BufferHandle &handle);

private:
	//! Global sort state
	GlobalSortState &state;

	//! Buffer manager
	BufferManager &buffer_manager;

	//! Sort layout for key comparison
	const SortLayout &sort_layout;

	//! Target partition size
	idx_t partition_size;

	//! Computed partitions
	vector<KWayPartition> partitions;

	//! Thread-safe partition claiming
	mutex lock;
	atomic<idx_t> next_partition_idx;

	//! Cached scan states for blob comparison (one per run)
	vector<unique_ptr<SBScanState>> scan_states;

	//! O(1) lookup from run pointer to run index in pinned_blocks
	std::unordered_map<SortedBlock *, idx_t> run_index_map;

	//! Cached buffer handles for currently pinned blocks
	struct PinnedBlockInfo {
		idx_t run_idx = 0;
		idx_t block_idx = NumericLimits<idx_t>::Maximum();
		BufferHandle handle;
	};
	vector<PinnedBlockInfo> pinned_blocks;
};

} // namespace duckdb
