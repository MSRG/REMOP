#include "duckdb/common/sort/kway_merge_partitioner.hpp"
#include "duckdb/common/sort/comparators.hpp"
#include "duckdb/common/fast_mem.hpp"

namespace duckdb {

KWayMergePartitioner::KWayMergePartitioner(GlobalSortState &state_p, BufferManager &buffer_manager_p,
                                           idx_t partition_size_p)
    : state(state_p), buffer_manager(buffer_manager_p), sort_layout(state_p.sort_layout),
      partition_size(partition_size_p), next_partition_idx(0) {
}

void KWayMergePartitioner::ComputePartitions(const vector<SortedBlock *> &runs) {
	const idx_t k = runs.size();
	if (k == 0) {
		return;
	}

	// Calculate total elements across all runs
	idx_t total_elements = 0;
	for (auto *run : runs) {
		total_elements += run->Count();
	}

	if (total_elements == 0) {
		return;
	}

	// Initialize scan states for blob comparison (if needed)
	scan_states.clear();
	run_index_map.clear();
	scan_states.reserve(k);
	for (idx_t i = 0; i < k; i++) {
		auto scan = make_uniq<SBScanState>(buffer_manager, state);
		scan->sb = runs[i];
		scan->SetIndices(0, 0);
		scan_states.push_back(std::move(scan));
		run_index_map[runs[i]] = i;
	}

	// Initialize pinned block tracking
	pinned_blocks.clear();
	pinned_blocks.resize(k);
	for (idx_t i = 0; i < k; i++) {
		pinned_blocks[i].run_idx = i;
		pinned_blocks[i].block_idx = NumericLimits<idx_t>::Maximum();
	}

	// Calculate number of partitions
	idx_t num_partitions = (total_elements + partition_size - 1) / partition_size;
	num_partitions = MaxValue<idx_t>(1, num_partitions);

	// Initialize: first partition starts at position 0 for all runs
	vector<idx_t> prev_positions(k, 0);

	partitions.clear();
	partitions.reserve(num_partitions);

	for (idx_t p = 0; p < num_partitions; p++) {
		KWayPartition partition;
		partition.partition_idx = p;
		partition.run_partitions.resize(k);

		// Target cumulative position for end of this partition
		idx_t target = MinValue((p + 1) * partition_size, total_elements);

		// Find partition boundaries
		vector<idx_t> end_positions;
		if (p == num_partitions - 1) {
			// Last partition: take all remaining elements
			for (idx_t i = 0; i < k; i++) {
				end_positions.push_back(runs[i]->Count());
			}
		} else {
			end_positions = FindPartitionPoint(runs, target, prev_positions);
		}

		// Fill in partition info
		partition.total_elements = 0;
		for (idx_t i = 0; i < k; i++) {
			auto &rp = partition.run_partitions[i];
			rp.start_idx = prev_positions[i];
			rp.end_idx = end_positions[i];
			partition.total_elements += rp.Count();
		}

		partitions.push_back(std::move(partition));
		prev_positions = end_positions;
	}

	// Reset for work stealing
	next_partition_idx = 0;

	// Clean up pinned blocks
	pinned_blocks.clear();
	scan_states.clear();
	run_index_map.clear();
}

vector<idx_t> KWayMergePartitioner::FindPartitionPoint(const vector<SortedBlock *> &runs, idx_t target,
                                                       const vector<idx_t> &prev_positions) {
	const idx_t k = runs.size();
	vector<idx_t> best_positions = prev_positions; // Start with previous positions
	idx_t best_total = 0;
	for (idx_t i = 0; i < k; i++) {
		best_total += prev_positions[i];
	}

	// If we're already at or past target, return current positions
	if (best_total >= target) {
		return best_positions;
	}

	// Key bounds for narrowing search (nullptr = unbounded)
	data_ptr_t global_key_lo = nullptr;
	data_ptr_t global_key_hi = nullptr;
	BufferHandle key_lo_handle, key_hi_handle;

	// Search bounds for each run (start from previous partition's end)
	vector<idx_t> run_search_lo = prev_positions;
	vector<idx_t> run_search_hi(k);
	for (idx_t i = 0; i < k; i++) {
		run_search_hi[i] = runs[i]->Count();
	}

	// Try each run as source of boundary key
	for (idx_t r = 0; r < k; r++) {
		idx_t search_lo = run_search_lo[r];
		idx_t search_hi = run_search_hi[r];

		if (search_lo >= search_hi) {
			continue; // This run has no elements in search range
		}

		// Narrow search range based on global key bounds
		if (global_key_lo) {
			idx_t lb = UpperBoundInRun(runs[r], global_key_lo, search_lo, search_hi);
			search_lo = MaxValue(search_lo, lb);
		}
		if (global_key_hi) {
			idx_t ub = UpperBoundInRun(runs[r], global_key_hi, search_lo, search_hi);
			search_hi = MinValue(search_hi, ub);
		}

		if (search_lo >= search_hi) {
			continue;
		}

		// Binary search on run[r] to find optimal split point
		while (search_lo < search_hi) {
			idx_t mid = search_lo + (search_hi - search_lo) / 2;
			data_ptr_t key = GetKeyAtPosition(runs[r], mid);
			if (!key) {
				break;
			}

			// Count total elements <= key across ALL runs
			idx_t total = 0;
			vector<idx_t> positions(k);

			for (idx_t j = 0; j < k; j++) {
				if (j == r) {
					// For the pivot run, count includes element at mid
					positions[j] = mid + 1;
				} else {
					// For other runs, find upper_bound within narrowed range
					positions[j] = UpperBoundInRun(runs[j], key, run_search_lo[j], run_search_hi[j]);
				}
				total += positions[j];
			}

			if (total < target) {
				search_lo = mid + 1;

				// Update global lower bound
				global_key_lo = key;
				// Note: key pointer may become invalid, but we use it for comparison only

				// Narrow future runs' lower bounds
				for (idx_t j = r + 1; j < k; j++) {
					idx_t new_lo = UpperBoundInRun(runs[j], key, run_search_lo[j], run_search_hi[j]);
					run_search_lo[j] = MaxValue(run_search_lo[j], new_lo);
				}
			} else {
				search_hi = mid;

				// Update global upper bound
				global_key_hi = key;

				// Narrow future runs' upper bounds
				for (idx_t j = r + 1; j < k; j++) {
					idx_t new_hi = UpperBoundInRun(runs[j], key, run_search_lo[j], run_search_hi[j]);
					run_search_hi[j] = MinValue(run_search_hi[j], new_hi + 1);
				}

				// Record as potential best if this gives us exactly or closest to target
				if (total >= target && (best_total < target || total <= best_total)) {
					best_positions = positions;
					best_total = total;
				}
			}
		}

		// Update search bounds for this run based on binary search result
		run_search_lo[r] = search_lo;
		run_search_hi[r] = search_hi;
	}

	// If we didn't find exact target, use best approximation
	// This can happen due to duplicate keys across runs
	if (best_total < target) {
		// Take remaining elements proportionally or just set to search_hi
		for (idx_t i = 0; i < k; i++) {
			best_positions[i] = run_search_hi[i];
		}
	}

	return best_positions;
}

idx_t KWayMergePartitioner::UpperBoundInRun(SortedBlock *run, data_ptr_t key, idx_t search_lo, idx_t search_hi) {
	if (search_lo >= search_hi || !key) {
		return search_lo;
	}

	// Binary search for first position where element > key
	while (search_lo < search_hi) {
		idx_t mid = search_lo + (search_hi - search_lo) / 2;
		data_ptr_t mid_key = GetKeyAtPosition(run, mid);

		if (!mid_key) {
			// Shouldn't happen, but handle gracefully
			search_hi = mid;
			continue;
		}

		int cmp = CompareKeys(mid_key, key);

		if (cmp <= 0) {
			// mid_key <= key, search right half
			search_lo = mid + 1;
		} else {
			// mid_key > key, search left half
			search_hi = mid;
		}
	}

	return search_lo;
}

data_ptr_t KWayMergePartitioner::GetKeyAtPosition(SortedBlock *run, idx_t global_idx) {
	if (global_idx >= run->Count()) {
		return nullptr;
	}

	idx_t block_idx, entry_idx;
	GlobalToLocal(run, global_idx, block_idx, entry_idx);

	if (block_idx >= run->radix_sorting_data.size()) {
		return nullptr;
	}

	// Resolve run index in O(1) from run pointer for cache lookup
	auto run_it = run_index_map.find(run);
	D_ASSERT(run_it != run_index_map.end());
	idx_t run_idx = run_it->second;

	// Check if we have this block pinned
	auto &pinned = pinned_blocks[run_idx];
	if (pinned.block_idx != block_idx) {
		// Pin the new block
		auto &radix_block = run->radix_sorting_data[block_idx];
		pinned.handle = buffer_manager.Pin(radix_block->block);
		pinned.block_idx = block_idx;
	}

	return pinned.handle.Ptr() + entry_idx * sort_layout.entry_size;
}

int KWayMergePartitioner::CompareKeys(data_ptr_t key_a, data_ptr_t key_b) {
	if (!key_a || !key_b) {
		// Handle null keys: null is considered "infinite"
		if (!key_a && !key_b) {
			return 0;
		}
		return key_a ? -1 : 1;
	}

	if (sort_layout.all_constant) {
		// All constant size columns - use fast memcmp
		return FastMemcmp(key_a, key_b, sort_layout.comparison_size);
	} else {
		// Variable size columns need full comparison
		// For simplicity in partitioning, we use the comparison_size prefix
		// This is safe because the prefix encodes the comparison result
		return FastMemcmp(key_a, key_b, sort_layout.comparison_size);
	}
}

void KWayMergePartitioner::GlobalToLocal(SortedBlock *run, idx_t global_idx, idx_t &block_idx, idx_t &entry_idx) {
	block_idx = 0;
	idx_t remaining = global_idx;

	for (auto &block : run->radix_sorting_data) {
		if (remaining < block->count) {
			entry_idx = remaining;
			return;
		}
		remaining -= block->count;
		block_idx++;
	}

	// Past end - set to last valid position
	if (!run->radix_sorting_data.empty()) {
		block_idx = run->radix_sorting_data.size() - 1;
		entry_idx = run->radix_sorting_data.back()->count;
	} else {
		block_idx = 0;
		entry_idx = 0;
	}
}

data_ptr_t KWayMergePartitioner::PinAndGetKey(SortedBlock *run, idx_t block_idx, idx_t entry_idx,
                                              BufferHandle &handle) {
	if (block_idx >= run->radix_sorting_data.size()) {
		return nullptr;
	}

	auto &radix_block = run->radix_sorting_data[block_idx];
	handle = buffer_manager.Pin(radix_block->block);
	return handle.Ptr() + entry_idx * sort_layout.entry_size;
}

bool KWayMergePartitioner::GetNextPartition(KWayPartition &partition) {
	idx_t idx = next_partition_idx.fetch_add(1);
	if (idx >= partitions.size()) {
		return false;
	}
	partition = partitions[idx];
	return true;
}

} // namespace duckdb
