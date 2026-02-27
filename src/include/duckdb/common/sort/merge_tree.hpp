//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sort/merge_tree.hpp
//
// Merge Tree (Tournament Tree) data structure for efficient k-way merge
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/sort/comparators.hpp"

namespace duckdb {

//! MergeTree implements a tournament tree for k-way merge operations.
//! It maintains the minimum element among k sorted runs efficiently.
//!
//! Current implementation: O(k) per GetWinner/ReplaceWinner operation.
//! This is efficient for small k (typical k <= 16).
//!
//! TODO: For large k (> 16), implement proper O(log k) heap-based tournament tree:
//!       - Use power-of-2 padded tree structure
//!       - Internal nodes store losers, winner propagates to root
//!       - ReplayFromLeaf traverses O(log k) levels instead of O(k) scan
//!       - Consider threshold-based switching between O(k) and O(log k) implementations
class MergeTree {
public:
	//! Sentinel value indicating an exhausted run
	static constexpr idx_t INVALID_RUN = NumericLimits<idx_t>::Maximum();

	//! Create a merge tree for k-way merge
	//! @param k Number of runs to merge (must be >= 2)
	//! @param sort_layout The sort layout for key comparison
	//! @param external Whether we're doing external sort (affects comparison)
	MergeTree(idx_t k, const SortLayout &sort_layout, bool external);

	//! Initialize the tree with the first key from each run
	//! @param initial_keys Pointers to the first key from each run (nullptr if run is empty)
	//! @param scan_states Scan states for blob data access (needed for variable-length keys)
	void Initialize(const vector<data_ptr_t> &initial_keys, const vector<SBScanState *> &scan_states);

	//! Get the index of the run with the minimum key (the winner)
	//! @return Run index, or INVALID_RUN if all runs are exhausted
	idx_t GetWinner() const;

	//! Get the key pointer of the current winner
	//! @return Pointer to winner's key, or nullptr if no winner
	data_ptr_t GetWinnerKey() const;

	//! Update the tree after popping from the winner run
	//! @param run_idx The run that was just popped (should be current winner)
	//! @param new_key The new key from that run (nullptr if run is now exhausted)
	//! @param scan_state Updated scan state for the run
	void ReplaceWinner(idx_t run_idx, data_ptr_t new_key, SBScanState *scan_state);

	//! Mark a run as exhausted (no more data)
	void MarkExhausted(idx_t run_idx);

	//! Check if all runs are exhausted
	bool AllExhausted() const;

	//! Get the number of active (non-exhausted) runs
	idx_t ActiveRunCount() const;

	//! Reset the tree for reuse
	void Reset();

private:
	//! Compare two keys and return true if key_a < key_b
	bool IsLess(idx_t run_a, idx_t run_b) const;

	//! Replay the tournament from a leaf to the root after updating a run
	void ReplayFromLeaf(idx_t leaf_idx);

	//! Build the initial tree (called by Initialize)
	void BuildTree();

	//! Get the leaf index for a given run
	idx_t GetLeafIndex(idx_t run_idx) const;

	//! Get the parent index of a node
	idx_t GetParent(idx_t node_idx) const;

private:
	//! Number of runs (k)
	idx_t k;

	//! The tournament tree: tree[0] stores the overall winner
	//! For O(log k) implementation, tree[1..k-1] would store losers
	vector<idx_t> tree;

	//! Current key pointer for each run (nullptr = exhausted)
	vector<data_ptr_t> keys;

	//! Scan states for each run (needed for variable-length key comparison)
	vector<SBScanState *> scan_states;

	//! Whether each run is exhausted
	vector<bool> exhausted;

	//! Number of active runs
	idx_t active_count;

	//! Sort layout for key comparison
	const SortLayout &sort_layout;

	//! Whether we're doing external sort
	bool external;
};

} // namespace duckdb
