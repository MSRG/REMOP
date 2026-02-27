//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sort/kway_merge_sorter.hpp
//
// K-way merge sorter for efficient external merge sort
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/sort/merge_tree.hpp"
#include "duckdb/common/sort/kway_merge_partitioner.hpp"
#include "duckdb/execution/operator_memory_policy.hpp"

namespace duckdb {

//! Batch size for merge operations (same as 2-way merge)
static constexpr idx_t KWAY_MERGE_BATCH_SIZE = STANDARD_VECTOR_SIZE;

//! MergeDecision stores the source location for one merged entry
struct MergeDecision {
	idx_t run_idx;    //! Which run the entry comes from
	idx_t block_idx;  //! Block index within the run
	idx_t entry_idx;  //! Entry index within the block
};

//! InputRunBuffer manages buffered reading from a single sorted run
struct InputRunBuffer {
	//! The source sorted block
	SortedBlock *source = nullptr;
	//! Current block index in the source
	idx_t current_block_idx = 0;
	//! Current entry index within the block
	idx_t current_entry_idx = 0;
	//! Total entries remaining in this run
	idx_t total_remaining = 0;

	//! Buffer for radix sorting data
	BufferHandle radix_handle;
	data_ptr_t radix_ptr = nullptr;

	//! Buffer for blob sorting data (if needed)
	BufferHandle blob_data_handle;
	BufferHandle blob_heap_handle;

	//! Entries in current pinned block
	idx_t entries_in_block = 0;

	//! Whether this run is exhausted
	bool exhausted = false;

	//! Initialize the buffer for a sorted block
	void Initialize(SortedBlock *block, BufferManager &buffer_manager);

	//! Pin the current block
	void PinCurrentBlock(BufferManager &buffer_manager, const SortLayout &sort_layout);

	//! Advance to next entry, returns false if run is exhausted
	bool Advance(BufferManager &buffer_manager, const SortLayout &sort_layout);

	//! Get current key pointer
	data_ptr_t GetCurrentKey(const SortLayout &sort_layout) const;

	//! Get remaining entries
	idx_t Remaining() const;
};

//! KWayMergeSorter performs k-way merge sort with configurable buffer sizes
class KWayMergeSorter {
public:
	//! Create a k-way merge sorter
	//! @param state Global sort state
	//! @param buffer_manager Buffer manager for memory allocation
	//! @param config K-way merge configuration from memory policy
	KWayMergeSorter(GlobalSortState &state, BufferManager &buffer_manager, const KWayMergePolicyConfig &config);

	//! Perform one round of k-way merge (serial version)
	//! Takes up to k sorted blocks and merges them into fewer, larger blocks
	void PerformKWayMergeRound();

	//! Get the actual k being used (may be less than configured if fewer runs available)
	idx_t GetActualK() const {
		return actual_k;
	}

	//===--------------------------------------------------------------------===//
	// Parallel Merge Support
	//===--------------------------------------------------------------------===//

	//! Initialize partitioner for parallel k-way merge
	//! @param runs The runs to merge in this round
	//! @param partition_size Target elements per partition (usually block_capacity)
	void InitializePartitioner(const vector<SortedBlock *> &runs, idx_t partition_size);

	//! Merge a single partition (called by parallel tasks)
	//! @param partition The partition to merge
	//! @param result_block Output: the merged result block for this partition
	void MergePartition(const KWayPartition &partition, unique_ptr<SortedBlock> &result_block);

	//! Get the partitioner (for parallel task scheduling)
	KWayMergePartitioner *GetPartitioner() {
		return partitioner.get();
	}

	//! Add a result block from a parallel task (thread-safe)
	void AddResultBlock(unique_ptr<SortedBlock> block, idx_t partition_idx);

	//! Finalize parallel merge: sort result blocks by partition index and update state
	void FinalizeParallelMerge();

private:
	//! Initialize input buffers for the current merge group
	void InitializeInputBuffers(const vector<SortedBlock *> &runs);

	//! Initialize scan states for the loser tree
	void InitializeScanStates();

	//! Merge the current group of runs into result blocks
	void MergeGroup();

	//! Copy one entry from the winner run to the output
	void CopyWinnerToOutput();

	//===--------------------------------------------------------------------===//
	// Batched Merge Methods (similar to 2-way MergeSorter)
	//===--------------------------------------------------------------------===//

	//! Pop up to 'max_count' winners from merge tree and record decisions
	//! Returns actual number of winners popped
	idx_t PopWinnersBatch(idx_t max_count, MergeDecision decisions[]);

	//! Batch-copy radix data using pre-computed decisions
	void MergeRadixBatch(idx_t count, const MergeDecision decisions[]);

	//! Batch-copy payload data using pre-computed decisions
	void MergePayloadBatch(idx_t count, const MergeDecision decisions[]);

	//! Batch-copy blob data using pre-computed decisions (if needed)
	void MergeBlobBatch(idx_t count, const MergeDecision decisions[]);

	//! Flush remaining entries when a run exhausts (bulk copy from single source)
	void FlushRemainingFromRun(idx_t run_idx);

	//! Flush the output buffer to result blocks
	void FlushOutputBuffer();

	//! Allocate output buffers based on configuration
	void AllocateOutputBuffers();

	//! Check if all input runs are exhausted
	bool AllInputsExhausted() const;

	//! Get the next group of runs to merge (up to k runs)
	vector<SortedBlock *> GetNextMergeGroup();

private:
	//! Global sort state
	GlobalSortState &state;

	//! Buffer manager
	BufferManager &buffer_manager;

	//! Configuration
	KWayMergePolicyConfig config;

	//! Actual k being used for this merge round
	idx_t actual_k;

	//! Sort layout shortcuts
	const SortLayout &sort_layout;
	const RowLayout &payload_layout;

	//! Input run buffers (one per run being merged)
	vector<unique_ptr<InputRunBuffer>> input_buffers;

	//! Scan states for each input (for merge tree comparisons)
	vector<unique_ptr<SBScanState>> scan_states;

	//! Merge tree (tournament tree) for k-way merge
	unique_ptr<MergeTree> merge_tree;

	//! Result blocks being built
	vector<unique_ptr<SortedBlock>> result_blocks;

	//! Current output block
	SortedBlock *current_output = nullptr;

	//! Output buffer handles
	BufferHandle output_radix_handle;
	BufferHandle output_blob_data_handle;
	BufferHandle output_blob_heap_handle;
	BufferHandle output_payload_data_handle;
	BufferHandle output_payload_heap_handle;

	//! Output write positions
	data_ptr_t output_radix_ptr = nullptr;
	data_ptr_t output_payload_ptr = nullptr;
	idx_t output_count = 0;
	idx_t output_capacity = 0;

	//! Index of next run group to process
	idx_t next_group_start = 0;

	//===--------------------------------------------------------------------===//
	// Parallel Merge State
	//===--------------------------------------------------------------------===//

	//! Partitioner for parallel k-way merge
	unique_ptr<KWayMergePartitioner> partitioner;

	//! Current runs being merged (for parallel access)
	vector<SortedBlock *> current_runs;

	//! Result blocks from parallel merge (slot index == partition_idx)
	vector<unique_ptr<SortedBlock>> partition_result_slots;
};

} // namespace duckdb
