#include "duckdb/common/sort/merge_tree.hpp"
#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/sort/comparators.hpp"

namespace duckdb {

// C++11 requires out-of-line definition for static constexpr members that are ODR-used
constexpr idx_t MergeTree::INVALID_RUN;

MergeTree::MergeTree(idx_t k_p, const SortLayout &sort_layout_p, bool external_p)
    : k(k_p), active_count(0), sort_layout(sort_layout_p), external(external_p) {
	D_ASSERT(k >= 2);
	// Tree has k-1 internal nodes (indices 1 to k-1) plus root winner at index 0
	// For simplicity, we allocate k entries: tree[0] = winner, tree[1..k-1] = internal losers
	tree.resize(k, INVALID_RUN);
	keys.resize(k, nullptr);
	scan_states.resize(k, nullptr);
	exhausted.resize(k, true);
}

void MergeTree::Initialize(const vector<data_ptr_t> &initial_keys, const vector<SBScanState *> &scan_states_p) {
	D_ASSERT(initial_keys.size() == k);
	D_ASSERT(scan_states_p.size() == k);

	active_count = 0;
	for (idx_t i = 0; i < k; i++) {
		keys[i] = initial_keys[i];
		scan_states[i] = scan_states_p[i];
		exhausted[i] = (initial_keys[i] == nullptr);
		if (!exhausted[i]) {
			active_count++;
		}
	}

	// Build the tournament tree
	BuildTree();
}

void MergeTree::BuildTree() {
	if (active_count == 0) {
		tree[0] = INVALID_RUN;
		return;
	}

	// Initialize all internal nodes to INVALID_RUN
	for (idx_t i = 0; i < k; i++) {
		tree[i] = INVALID_RUN;
	}

	// For a proper loser tree with arbitrary k, we use a bottom-up tournament
	// We'll use a simplified approach: replay from each leaf
	// This is O(k log k) but only done once during initialization

	// Find the first non-exhausted run as the initial "winner"
	idx_t initial_winner = INVALID_RUN;
	for (idx_t i = 0; i < k; i++) {
		if (!exhausted[i]) {
			initial_winner = i;
			break;
		}
	}

	if (initial_winner == INVALID_RUN) {
		tree[0] = INVALID_RUN;
		return;
	}

	// Set initial winner
	tree[0] = initial_winner;

	// Now replay tournaments for all other runs to establish losers
	for (idx_t run = 0; run < k; run++) {
		if (run != initial_winner && !exhausted[run]) {
			ReplayFromLeaf(run);
		}
	}
}

idx_t MergeTree::GetWinner() const {
	return tree[0];
}

data_ptr_t MergeTree::GetWinnerKey() const {
	idx_t winner = tree[0];
	if (winner == INVALID_RUN) {
		return nullptr;
	}
	return keys[winner];
}

void MergeTree::ReplaceWinner(idx_t run_idx, data_ptr_t new_key, SBScanState *scan_state) {
	D_ASSERT(run_idx < k);
	D_ASSERT(tree[0] == run_idx); // Should only replace the current winner

	keys[run_idx] = new_key;
	scan_states[run_idx] = scan_state;

	if (new_key == nullptr) {
		// Run is exhausted
		exhausted[run_idx] = true;
		active_count--;
	}

	// Replay the tournament from this run
	ReplayFromLeaf(run_idx);
}

void MergeTree::MarkExhausted(idx_t run_idx) {
	D_ASSERT(run_idx < k);
	if (!exhausted[run_idx]) {
		exhausted[run_idx] = true;
		keys[run_idx] = nullptr;
		active_count--;
		ReplayFromLeaf(run_idx);
	}
}

bool MergeTree::AllExhausted() const {
	return active_count == 0;
}

idx_t MergeTree::ActiveRunCount() const {
	return active_count;
}

void MergeTree::Reset() {
	for (idx_t i = 0; i < k; i++) {
		tree[i] = INVALID_RUN;
		keys[i] = nullptr;
		scan_states[i] = nullptr;
		exhausted[i] = true;
	}
	active_count = 0;
}

bool MergeTree::IsLess(idx_t run_a, idx_t run_b) const {
	// Handle exhausted runs: exhausted runs are "infinite" (never win)
	if (exhausted[run_a]) {
		return false;
	}
	if (exhausted[run_b]) {
		return true;
	}

	// Compare the keys
	data_ptr_t key_a = keys[run_a];
	data_ptr_t key_b = keys[run_b];

	D_ASSERT(key_a != nullptr);
	D_ASSERT(key_b != nullptr);

	int cmp;
	if (sort_layout.all_constant) {
		// All constant size columns - use fast memcmp
		cmp = FastMemcmp(key_a, key_b, sort_layout.comparison_size);
	} else {
		// Variable size columns - need full comparison
		D_ASSERT(scan_states[run_a] != nullptr);
		D_ASSERT(scan_states[run_b] != nullptr);
		cmp = Comparators::CompareTuple(*scan_states[run_a], *scan_states[run_b], key_a, key_b, sort_layout, external);
	}

	// Return true if a < b
	// For stable sort with equal keys, prefer the run with smaller index
	if (cmp == 0) {
		return run_a < run_b;
	}
	return cmp < 0;
}

void MergeTree::ReplayFromLeaf(idx_t leaf_run) {
	// Current implementation: O(k) linear scan to find minimum
	// This is efficient for small k (typical k <= 16)
	//
	// TODO: For large k (> 16), implement O(log k) tournament tree traversal:
	//       - Traverse from leaf position to root (O(log k) levels)
	//       - At each level, compare with stored loser
	//       - Winner moves up, loser stays at node
	//       - Requires power-of-2 tree structure with proper indexing

	if (active_count == 0) {
		tree[0] = INVALID_RUN;
		return;
	}

	if (active_count == 1) {
		// Only one active run - it's the winner
		for (idx_t i = 0; i < k; i++) {
			if (!exhausted[i]) {
				tree[0] = i;
				return;
			}
		}
		tree[0] = INVALID_RUN;
		return;
	}

	// For a general merge tree with arbitrary k runs:
	// Use simple tournament approach that works for any k value.
	// This is O(k) per replacement but is simple and correct.
	// For high-performance scenarios with large k, a proper
	// power-of-2 padded tree would give O(log k) performance.
	(void)leaf_run; // Mark as intentionally unused in this simple implementation

	idx_t winner = INVALID_RUN;
	for (idx_t i = 0; i < k; i++) {
		if (!exhausted[i]) {
			if (winner == INVALID_RUN) {
				winner = i;
			} else if (IsLess(i, winner)) {
				winner = i;
			}
		}
	}
	tree[0] = winner;
}

idx_t MergeTree::GetLeafIndex(idx_t run_idx) const {
	// In a complete binary tree with k leaves, leaves are at positions k to 2k-1
	return k + run_idx;
}

idx_t MergeTree::GetParent(idx_t node_idx) const {
	return node_idx / 2;
}

} // namespace duckdb
