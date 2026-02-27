#include "duckdb/common/sort/kway_merge_sorter.hpp"
#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/sort/comparators.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include <sstream>
#include <unordered_map>

namespace duckdb {

//===--------------------------------------------------------------------===//
// InputRunBuffer Implementation
//===--------------------------------------------------------------------===//
void InputRunBuffer::Initialize(SortedBlock *block, BufferManager &buffer_manager) {
	source = block;
	current_block_idx = 0;
	current_entry_idx = 0;
	total_remaining = block ? block->Count() : 0;
	exhausted = (total_remaining == 0);
	entries_in_block = 0;
}

void InputRunBuffer::PinCurrentBlock(BufferManager &buffer_manager, const SortLayout &sort_layout) {
	if (exhausted || !source) {
		return;
	}

	if (current_block_idx >= source->radix_sorting_data.size()) {
		exhausted = true;
		return;
	}

	auto &radix_block = source->radix_sorting_data[current_block_idx];
	radix_handle = buffer_manager.Pin(radix_block->block);
	radix_ptr = radix_handle.Ptr();
	entries_in_block = radix_block->count;

	// Pin blob data if needed
	if (!sort_layout.all_constant && source->blob_sorting_data) {
		auto &blob_data = source->blob_sorting_data;
		if (current_block_idx < blob_data->data_blocks.size()) {
			blob_data_handle = buffer_manager.Pin(blob_data->data_blocks[current_block_idx]->block);
			if (!blob_data->heap_blocks.empty() && current_block_idx < blob_data->heap_blocks.size()) {
				blob_heap_handle = buffer_manager.Pin(blob_data->heap_blocks[current_block_idx]->block);
			}
		}
	}

	// Increment read round counter - each block pin from input run = one read round
	auto &buffer_pool = buffer_manager.GetBufferPool();
	if (buffer_pool.IsProfilingEnabled() && buffer_pool.GetMetrics()) {
		auto *metrics = const_cast<BufferPoolMetrics *>(buffer_pool.GetMetrics());
		metrics->transfer_read_rounds++;
	}
}

bool InputRunBuffer::Advance(BufferManager &buffer_manager, const SortLayout &sort_layout) {
	if (exhausted) {
		return false;
	}

	current_entry_idx++;
	total_remaining--;

	if (total_remaining == 0) {
		exhausted = true;
		return false;
	}

	// Check if we need to move to the next block
	if (current_entry_idx >= entries_in_block) {
		current_block_idx++;
		current_entry_idx = 0;
		PinCurrentBlock(buffer_manager, sort_layout);
	}

	return !exhausted;
}

data_ptr_t InputRunBuffer::GetCurrentKey(const SortLayout &sort_layout) const {
	if (exhausted || !radix_ptr) {
		return nullptr;
	}
	return radix_ptr + current_entry_idx * sort_layout.entry_size;
}

idx_t InputRunBuffer::Remaining() const {
	return total_remaining;
}

//===--------------------------------------------------------------------===//
// KWayMergeSorter Implementation
//===--------------------------------------------------------------------===//
KWayMergeSorter::KWayMergeSorter(GlobalSortState &state_p, BufferManager &buffer_manager_p,
                                 const KWayMergePolicyConfig &config_p)
    : state(state_p), buffer_manager(buffer_manager_p), config(config_p), actual_k(2),
      sort_layout(state.sort_layout), payload_layout(state.payload_layout), next_group_start(0) {
}

void KWayMergeSorter::PerformKWayMergeRound() {
	if (state.sorted_blocks.size() <= 1) {
		return; // Nothing to merge
	}

	// Calculate optimal k based on number of runs and memory
	idx_t num_runs = state.sorted_blocks.size();
	idx_t row_width = payload_layout.GetRowWidth();
	actual_k = config.ComputeOptimalK(num_runs, row_width);

	// Clear result storage
	result_blocks.clear();
	next_group_start = 0;

	// Process runs in groups of k
	while (next_group_start < state.sorted_blocks.size()) {
		auto merge_group = GetNextMergeGroup();
		if (merge_group.empty()) {
			break;
		}

		// Initialize for this merge group
		InitializeInputBuffers(merge_group);
		InitializeScanStates();
		AllocateOutputBuffers();

		// Create merge tree (tournament tree)
		merge_tree = make_uniq<MergeTree>(merge_group.size(), sort_layout, state.external);

		// Get initial keys
		vector<data_ptr_t> initial_keys;
		vector<SBScanState *> scan_state_ptrs;
		for (idx_t i = 0; i < merge_group.size(); i++) {
			initial_keys.push_back(input_buffers[i]->GetCurrentKey(sort_layout));
			scan_state_ptrs.push_back(scan_states[i].get());
		}
		merge_tree->Initialize(initial_keys, scan_state_ptrs);

		// Merge this group
		MergeGroup();

		// Flush any remaining output
		if (output_count > 0) {
			FlushOutputBuffer();
		}

		// Clean up input buffers
		input_buffers.clear();
		scan_states.clear();
		merge_tree.reset();
	}

	// Replace sorted_blocks with merged results
	state.sorted_blocks.clear();
	for (auto &result : result_blocks) {
		state.sorted_blocks.push_back(std::move(result));
	}
	result_blocks.clear();
}

vector<SortedBlock *> KWayMergeSorter::GetNextMergeGroup() {
	vector<SortedBlock *> group;

	idx_t group_size = MinValue(actual_k, state.sorted_blocks.size() - next_group_start);
	for (idx_t i = 0; i < group_size; i++) {
		group.push_back(state.sorted_blocks[next_group_start + i].get());
	}
	next_group_start += group_size;

	return group;
}

void KWayMergeSorter::InitializeInputBuffers(const vector<SortedBlock *> &runs) {
	input_buffers.clear();
	input_buffers.reserve(runs.size());

	for (auto *run : runs) {
		auto buffer = make_uniq<InputRunBuffer>();
		buffer->Initialize(run, buffer_manager);
		buffer->PinCurrentBlock(buffer_manager, sort_layout);
		input_buffers.push_back(std::move(buffer));
	}
}

void KWayMergeSorter::InitializeScanStates() {
	scan_states.clear();
	scan_states.reserve(input_buffers.size());

	for (idx_t i = 0; i < input_buffers.size(); i++) {
		auto scan_state = make_uniq<SBScanState>(buffer_manager, state);
		if (input_buffers[i]->source) {
			scan_state->sb = input_buffers[i]->source;
			scan_state->block_idx = input_buffers[i]->current_block_idx;
			scan_state->entry_idx = input_buffers[i]->current_entry_idx;

			// Pin data for blob comparison if needed
			if (!sort_layout.all_constant && scan_state->sb->blob_sorting_data) {
				scan_state->PinData(*scan_state->sb->blob_sorting_data);
			}
		}
		scan_states.push_back(std::move(scan_state));
	}
}

void KWayMergeSorter::AllocateOutputBuffers() {
	// Create a new result block
	current_output = new SortedBlock(buffer_manager, state);
	current_output->InitializeWrite();

	// Pin output buffers
	auto &radix_block = current_output->radix_sorting_data.back();
	output_radix_handle = buffer_manager.Pin(radix_block->block);
	output_radix_ptr = output_radix_handle.Ptr();
	output_capacity = radix_block->capacity;

	// Pin payload output
	auto &payload_block = current_output->payload_data->data_blocks.back();
	output_payload_data_handle = buffer_manager.Pin(payload_block->block);
	output_payload_ptr = output_payload_data_handle.Ptr();

	// Pin blob output once per output block
	if (!sort_layout.all_constant) {
		auto &blob_data_block = current_output->blob_sorting_data->data_blocks.back();
		output_blob_data_handle = buffer_manager.Pin(blob_data_block->block);
		if (state.external && !sort_layout.blob_layout.AllConstant()) {
			auto &blob_heap_block = current_output->blob_sorting_data->heap_blocks.back();
			output_blob_heap_handle = buffer_manager.Pin(blob_heap_block->block);
		}
	}

	// Pin payload heap output once per output block (external + varlen payload)
	if (state.external && !payload_layout.AllConstant()) {
		auto &payload_heap_block = current_output->payload_data->heap_blocks.back();
		output_payload_heap_handle = buffer_manager.Pin(payload_heap_block->block);
	}

	output_count = 0;
}

void KWayMergeSorter::MergeGroup() {
	// Batched merge: determine merge order first, then copy data in batches
	// This improves cache locality by grouping copies from the same source block
	MergeDecision decisions[KWAY_MERGE_BATCH_SIZE];

	while (!merge_tree->AllExhausted()) {
		// Compute available space in output
		idx_t remaining_capacity = output_capacity - output_count;
		idx_t max_batch = MinValue(remaining_capacity, (idx_t)KWAY_MERGE_BATCH_SIZE);

		// Phase 1: Pop winners and record decisions
		idx_t batch_size = PopWinnersBatch(max_batch, decisions);

		if (batch_size == 0) {
			break;
		}

		// Phase 2: Batch-copy radix data
		MergeRadixBatch(batch_size, decisions);

		// Phase 3: Batch-copy blob data (if variable-size sorting columns)
		if (!sort_layout.all_constant) {
			MergeBlobBatch(batch_size, decisions);
		}

		// Phase 4: Batch-copy payload data
		MergePayloadBatch(batch_size, decisions);

		output_count += batch_size;

		// Check if output buffer is full
		if (output_count >= output_capacity) {
			FlushOutputBuffer();
			AllocateOutputBuffers();
		}
	}

	// Check for remaining entries in exhausted state (only one run left)
	// Count non-exhausted runs
	idx_t remaining_run = MergeTree::INVALID_RUN;
	idx_t non_exhausted_count = 0;
	for (idx_t i = 0; i < input_buffers.size(); i++) {
		if (!input_buffers[i]->exhausted) {
			remaining_run = i;
			non_exhausted_count++;
		}
	}

	// If only one run remains, flush it directly (bulk copy optimization)
	const bool has_external_varlen = state.external && (!sort_layout.all_constant || !payload_layout.AllConstant());
	if (!has_external_varlen && non_exhausted_count == 1 && remaining_run != MergeTree::INVALID_RUN) {
		FlushRemainingFromRun(remaining_run);
	}
}

void KWayMergeSorter::CopyWinnerToOutput() {
	idx_t winner = merge_tree->GetWinner();
	if (winner == MergeTree::INVALID_RUN) {
		return;
	}

	auto &winner_buffer = input_buffers[winner];
	auto *source = winner_buffer->source;

	// Copy radix sorting data
	data_ptr_t src_radix = winner_buffer->GetCurrentKey(sort_layout);
	FastMemcpy(output_radix_ptr + output_count * sort_layout.entry_size, src_radix, sort_layout.entry_size);

	// Copy payload data
	// Get payload pointer from source
	auto &payload_data = source->payload_data;
	idx_t src_block_idx = winner_buffer->current_block_idx;
	idx_t src_entry_idx = winner_buffer->current_entry_idx;

	if (src_block_idx < payload_data->data_blocks.size()) {
		auto payload_handle = buffer_manager.Pin(payload_data->data_blocks[src_block_idx]->block);
		idx_t payload_width = payload_layout.GetRowWidth();
		data_ptr_t src_payload = payload_handle.Ptr() + src_entry_idx * payload_width;
		FastMemcpy(output_payload_ptr + output_count * payload_width, src_payload, payload_width);
	}

	// Copy blob sorting data if present
	if (!sort_layout.all_constant && source->blob_sorting_data) {
		auto &blob_data = source->blob_sorting_data;
		if (src_block_idx < blob_data->data_blocks.size()) {
			// For simplicity, we copy blob data similarly
			// In a full implementation, we'd handle heap data more carefully
			auto blob_handle = buffer_manager.Pin(blob_data->data_blocks[src_block_idx]->block);
			idx_t blob_width = sort_layout.blob_layout.GetRowWidth();
			data_ptr_t src_blob = blob_handle.Ptr() + src_entry_idx * blob_width;

			auto &out_blob = current_output->blob_sorting_data;
			auto out_blob_handle = buffer_manager.Pin(out_blob->data_blocks.back()->block);
			FastMemcpy(out_blob_handle.Ptr() + output_count * blob_width, src_blob, blob_width);
		}
	}

	output_count++;

	// Advance the winner run
	bool has_more = winner_buffer->Advance(buffer_manager, sort_layout);

	// Update scan state
	if (has_more) {
		scan_states[winner]->block_idx = winner_buffer->current_block_idx;
		scan_states[winner]->entry_idx = winner_buffer->current_entry_idx;
		if (!sort_layout.all_constant && scan_states[winner]->sb->blob_sorting_data) {
			scan_states[winner]->PinData(*scan_states[winner]->sb->blob_sorting_data);
		}
	}

	// Update merge tree
	data_ptr_t new_key = has_more ? winner_buffer->GetCurrentKey(sort_layout) : nullptr;
	merge_tree->ReplaceWinner(winner, new_key, scan_states[winner].get());
}

void KWayMergeSorter::FlushOutputBuffer() {
	if (output_count == 0) {
		delete current_output;
		current_output = nullptr;
		return;
	}

	// Update counts in the blocks
	current_output->radix_sorting_data.back()->count = output_count;
	current_output->payload_data->data_blocks.back()->count = output_count;
	if (!sort_layout.all_constant) {
		current_output->blob_sorting_data->data_blocks.back()->count = output_count;
	}
	if (state.external && !payload_layout.AllConstant()) {
		current_output->payload_data->heap_blocks.back()->count = output_count;
	}
	if (state.external && !sort_layout.all_constant) {
		current_output->blob_sorting_data->heap_blocks.back()->count = output_count;
	}

	// Increment write round counter - flushing output buffer = one write round
	auto &buffer_pool = buffer_manager.GetBufferPool();
	if (buffer_pool.IsProfilingEnabled() && buffer_pool.GetMetrics()) {
		auto *metrics = const_cast<BufferPoolMetrics *>(buffer_pool.GetMetrics());
		metrics->transfer_write_rounds++;
	}

	// Move to results
	result_blocks.push_back(unique_ptr<SortedBlock>(current_output));
	current_output = nullptr;

	// Release handles
	output_radix_handle.Destroy();
	output_blob_data_handle.Destroy();
	output_blob_heap_handle.Destroy();
	output_payload_data_handle.Destroy();
	output_payload_heap_handle.Destroy();
}

bool KWayMergeSorter::AllInputsExhausted() const {
	for (auto &buffer : input_buffers) {
		if (!buffer->exhausted) {
			return false;
		}
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Batched Merge Implementation
//===--------------------------------------------------------------------===//

idx_t KWayMergeSorter::PopWinnersBatch(idx_t max_count, MergeDecision decisions[]) {
	idx_t count = 0;

	while (count < max_count && !merge_tree->AllExhausted()) {
		idx_t winner = merge_tree->GetWinner();
		if (winner == MergeTree::INVALID_RUN) {
			break;
		}

		auto &winner_buffer = input_buffers[winner];

		// Record the decision
		decisions[count].run_idx = winner;
		decisions[count].block_idx = winner_buffer->current_block_idx;
		decisions[count].entry_idx = winner_buffer->current_entry_idx;
		count++;

		// Advance the winner run
		bool has_more = winner_buffer->Advance(buffer_manager, sort_layout);

		// Update scan state for next comparison
		if (has_more) {
			scan_states[winner]->block_idx = winner_buffer->current_block_idx;
			scan_states[winner]->entry_idx = winner_buffer->current_entry_idx;
			if (!sort_layout.all_constant && scan_states[winner]->sb->blob_sorting_data) {
				scan_states[winner]->PinData(*scan_states[winner]->sb->blob_sorting_data);
			}
		}

		// Update merge tree with new key (or nullptr if exhausted)
		data_ptr_t new_key = has_more ? winner_buffer->GetCurrentKey(sort_layout) : nullptr;
		merge_tree->ReplaceWinner(winner, new_key, scan_states[winner].get());
	}

	return count;
}

void KWayMergeSorter::MergeRadixBatch(idx_t count, const MergeDecision decisions[]) {
	const idx_t entry_size = sort_layout.entry_size;
	data_ptr_t output_ptr = output_radix_ptr + output_count * entry_size;

	idx_t i = 0;
	while (i < count) {
		const idx_t run_idx = decisions[i].run_idx;
		const idx_t block_idx = decisions[i].block_idx;
		auto *source = input_buffers[run_idx]->source;

		// Pin the source block once
		auto radix_handle = buffer_manager.Pin(source->radix_sorting_data[block_idx]->block);
		data_ptr_t src_base = radix_handle.Ptr();

		// Find consecutive entries from the same (run, block)
		while (i < count && decisions[i].run_idx == run_idx && decisions[i].block_idx == block_idx) {
			// Check if entries are consecutive within the block
			idx_t j = i;
			idx_t start_entry = decisions[j].entry_idx;
			while (j < count && decisions[j].run_idx == run_idx && decisions[j].block_idx == block_idx &&
			       decisions[j].entry_idx == start_entry + (j - i)) {
				j++;
			}

			// Batch copy consecutive entries
			idx_t consecutive_count = j - i;
			idx_t copy_bytes = consecutive_count * entry_size;
			data_ptr_t src_ptr = src_base + start_entry * entry_size;
			FastMemcpy(output_ptr, src_ptr, copy_bytes);
			output_ptr += copy_bytes;
			i = j;
		}
	}
}

void KWayMergeSorter::MergePayloadBatch(idx_t count, const MergeDecision decisions[]) {
	const idx_t row_width = payload_layout.GetRowWidth();
	if (payload_layout.AllConstant() || !state.external) {
		data_ptr_t output_ptr = output_payload_ptr + output_count * row_width;

		idx_t i = 0;
		while (i < count) {
			const idx_t run_idx = decisions[i].run_idx;
			const idx_t block_idx = decisions[i].block_idx;
			auto *source = input_buffers[run_idx]->source;
			auto &payload_data = source->payload_data;

			if (block_idx >= payload_data->data_blocks.size()) {
				i++;
				continue;
			}

			// Pin the source payload block once
			auto payload_handle = buffer_manager.Pin(payload_data->data_blocks[block_idx]->block);
			data_ptr_t src_base = payload_handle.Ptr();

			// Find consecutive entries from the same (run, block)
			while (i < count && decisions[i].run_idx == run_idx && decisions[i].block_idx == block_idx) {
				idx_t j = i;
				idx_t start_entry = decisions[j].entry_idx;
				while (j < count && decisions[j].run_idx == run_idx && decisions[j].block_idx == block_idx &&
				       decisions[j].entry_idx == start_entry + (j - i)) {
					j++;
				}

				// Batch copy consecutive entries
				idx_t consecutive_count = j - i;
				idx_t copy_bytes = consecutive_count * row_width;
				data_ptr_t src_ptr = src_base + start_entry * row_width;
				FastMemcpy(output_ptr, src_ptr, copy_bytes);
				output_ptr += copy_bytes;
				i = j;
			}
		}
		return;
	}

	D_ASSERT(payload_layout.GetHeapOffset() < row_width);
	auto &output_payload_data = current_output->payload_data;
	data_ptr_t output_data_ptr = output_payload_data_handle.Ptr() + output_count * row_width;

	auto &output_heap_block = output_payload_data->heap_blocks.back();

	const idx_t heap_pointer_offset = payload_layout.GetHeapOffset();
	idx_t source_heap_offsets[KWAY_MERGE_BATCH_SIZE];
	idx_t source_heap_sizes[KWAY_MERGE_BATCH_SIZE];

	idx_t source_run = NumericLimits<idx_t>::Maximum();
	idx_t source_block = NumericLimits<idx_t>::Maximum();
	BufferHandle source_data_handle;
	BufferHandle source_heap_handle;
	data_ptr_t source_data_base = nullptr;
	data_ptr_t source_heap_base = nullptr;
	idx_t heap_copy_bytes = 0;

	for (idx_t i = 0; i < count; i++) {
		const idx_t run_idx = decisions[i].run_idx;
		const idx_t block_idx = decisions[i].block_idx;
		auto *source = input_buffers[run_idx]->source;
		D_ASSERT(source);
		auto &payload_data = source->payload_data;
		D_ASSERT(block_idx < payload_data->data_blocks.size());
		D_ASSERT(block_idx < payload_data->heap_blocks.size());

		if (run_idx != source_run || block_idx != source_block) {
			source_data_handle = buffer_manager.Pin(payload_data->data_blocks[block_idx]->block);
			source_heap_handle = buffer_manager.Pin(payload_data->heap_blocks[block_idx]->block);
			source_data_base = source_data_handle.Ptr();
			source_heap_base = source_heap_handle.Ptr();
			source_run = run_idx;
			source_block = block_idx;
		}

		auto source_row_ptr = source_data_base + decisions[i].entry_idx * row_width;
		auto target_row_ptr = output_data_ptr + i * row_width;
		FastMemcpy(target_row_ptr, source_row_ptr, row_width);

		auto source_heap_offset = Load<idx_t>(source_row_ptr + heap_pointer_offset);
		auto source_heap_ptr = source_heap_base + source_heap_offset;
		auto entry_size = Load<uint32_t>(source_heap_ptr);
		D_ASSERT(entry_size >= sizeof(uint32_t));
		source_heap_offsets[i] = source_heap_offset;
		source_heap_sizes[i] = entry_size;
		Store<idx_t>(output_heap_block->byte_offset + heap_copy_bytes, target_row_ptr + heap_pointer_offset);
		heap_copy_bytes += entry_size;
	}

	if (output_heap_block->byte_offset + heap_copy_bytes > output_heap_block->capacity) {
		auto new_capacity = output_heap_block->byte_offset + heap_copy_bytes;
		buffer_manager.ReAllocate(output_heap_block->block, new_capacity);
		output_heap_block->capacity = new_capacity;
	}
	auto target_heap_ptr = output_payload_heap_handle.Ptr() + output_heap_block->byte_offset;
	idx_t copy_run = NumericLimits<idx_t>::Maximum();
	idx_t copy_block = NumericLimits<idx_t>::Maximum();
	BufferHandle copy_heap_handle;
	data_ptr_t copy_heap_base = nullptr;
	for (idx_t i = 0; i < count; i++) {
		const idx_t run_idx = decisions[i].run_idx;
		const idx_t block_idx = decisions[i].block_idx;
		auto *source = input_buffers[run_idx]->source;
		auto &payload_data = source->payload_data;
		if (run_idx != copy_run || block_idx != copy_block) {
			copy_heap_handle = buffer_manager.Pin(payload_data->heap_blocks[block_idx]->block);
			copy_heap_base = copy_heap_handle.Ptr();
			copy_run = run_idx;
			copy_block = block_idx;
		}
		auto source_heap_ptr = copy_heap_base + source_heap_offsets[i];
		memcpy(target_heap_ptr, source_heap_ptr, source_heap_sizes[i]);
		D_ASSERT(Load<uint32_t>(target_heap_ptr) == source_heap_sizes[i]);
		target_heap_ptr += source_heap_sizes[i];
	}
	output_heap_block->count += count;
	output_heap_block->byte_offset += heap_copy_bytes;
}

void KWayMergeSorter::MergeBlobBatch(idx_t count, const MergeDecision decisions[]) {
	if (sort_layout.all_constant) {
		return;
	}

	const idx_t blob_width = sort_layout.blob_layout.GetRowWidth();
	auto &out_blob = current_output->blob_sorting_data;
	if (sort_layout.blob_layout.AllConstant() || !state.external) {
		data_ptr_t output_ptr = output_blob_data_handle.Ptr() + output_count * blob_width;

		idx_t i = 0;
		while (i < count) {
			const idx_t run_idx = decisions[i].run_idx;
			const idx_t block_idx = decisions[i].block_idx;
			auto *source = input_buffers[run_idx]->source;

			if (!source->blob_sorting_data || block_idx >= source->blob_sorting_data->data_blocks.size()) {
				i++;
				continue;
			}

			auto &blob_data = source->blob_sorting_data;
			auto blob_handle = buffer_manager.Pin(blob_data->data_blocks[block_idx]->block);
			data_ptr_t src_base = blob_handle.Ptr();

			// Find consecutive entries from same (run, block)
			while (i < count && decisions[i].run_idx == run_idx && decisions[i].block_idx == block_idx) {
				idx_t j = i;
				idx_t start_entry = decisions[j].entry_idx;
				while (j < count && decisions[j].run_idx == run_idx && decisions[j].block_idx == block_idx &&
				       decisions[j].entry_idx == start_entry + (j - i)) {
					j++;
				}

				// Batch copy consecutive entries
				idx_t consecutive_count = j - i;
				idx_t copy_bytes = consecutive_count * blob_width;
				data_ptr_t src_ptr = src_base + start_entry * blob_width;
				FastMemcpy(output_ptr, src_ptr, copy_bytes);
				output_ptr += copy_bytes;
				i = j;
			}
		}
		return;
	}

	D_ASSERT(sort_layout.blob_layout.GetHeapOffset() < blob_width);
	auto &output_blob_data = *out_blob;
	data_ptr_t output_data_ptr = output_blob_data_handle.Ptr() + output_count * blob_width;

	auto &output_heap_block = output_blob_data.heap_blocks.back();

	const idx_t heap_pointer_offset = sort_layout.blob_layout.GetHeapOffset();
	idx_t source_heap_offsets[KWAY_MERGE_BATCH_SIZE];
	idx_t source_heap_sizes[KWAY_MERGE_BATCH_SIZE];

	idx_t source_run = NumericLimits<idx_t>::Maximum();
	idx_t source_block = NumericLimits<idx_t>::Maximum();
	BufferHandle source_data_handle;
	BufferHandle source_heap_handle;
	data_ptr_t source_data_base = nullptr;
	data_ptr_t source_heap_base = nullptr;
	idx_t heap_copy_bytes = 0;

	for (idx_t i = 0; i < count; i++) {
		const idx_t run_idx = decisions[i].run_idx;
		const idx_t block_idx = decisions[i].block_idx;
		auto *source = input_buffers[run_idx]->source;
		D_ASSERT(source);
		D_ASSERT(source->blob_sorting_data);
		auto &blob_data = *source->blob_sorting_data;
		D_ASSERT(block_idx < blob_data.data_blocks.size());
		D_ASSERT(block_idx < blob_data.heap_blocks.size());

		if (run_idx != source_run || block_idx != source_block) {
			source_data_handle = buffer_manager.Pin(blob_data.data_blocks[block_idx]->block);
			source_heap_handle = buffer_manager.Pin(blob_data.heap_blocks[block_idx]->block);
			source_data_base = source_data_handle.Ptr();
			source_heap_base = source_heap_handle.Ptr();
			source_run = run_idx;
			source_block = block_idx;
		}

		auto source_row_ptr = source_data_base + decisions[i].entry_idx * blob_width;
		auto target_row_ptr = output_data_ptr + i * blob_width;
		FastMemcpy(target_row_ptr, source_row_ptr, blob_width);

		auto source_heap_offset = Load<idx_t>(source_row_ptr + heap_pointer_offset);
		auto source_heap_ptr = source_heap_base + source_heap_offset;
		auto entry_size = Load<uint32_t>(source_heap_ptr);
		D_ASSERT(entry_size >= sizeof(uint32_t));
		source_heap_offsets[i] = source_heap_offset;
		source_heap_sizes[i] = entry_size;
		Store<idx_t>(output_heap_block->byte_offset + heap_copy_bytes, target_row_ptr + heap_pointer_offset);
		heap_copy_bytes += entry_size;
	}

	if (output_heap_block->byte_offset + heap_copy_bytes > output_heap_block->capacity) {
		auto new_capacity = output_heap_block->byte_offset + heap_copy_bytes;
		buffer_manager.ReAllocate(output_heap_block->block, new_capacity);
		output_heap_block->capacity = new_capacity;
	}
	auto target_heap_ptr = output_blob_heap_handle.Ptr() + output_heap_block->byte_offset;
	idx_t copy_run = NumericLimits<idx_t>::Maximum();
	idx_t copy_block = NumericLimits<idx_t>::Maximum();
	BufferHandle copy_heap_handle;
	data_ptr_t copy_heap_base = nullptr;
	for (idx_t i = 0; i < count; i++) {
		const idx_t run_idx = decisions[i].run_idx;
		const idx_t block_idx = decisions[i].block_idx;
		auto *source = input_buffers[run_idx]->source;
		auto &blob_data = *source->blob_sorting_data;
		if (run_idx != copy_run || block_idx != copy_block) {
			copy_heap_handle = buffer_manager.Pin(blob_data.heap_blocks[block_idx]->block);
			copy_heap_base = copy_heap_handle.Ptr();
			copy_run = run_idx;
			copy_block = block_idx;
		}
		auto source_heap_ptr = copy_heap_base + source_heap_offsets[i];
		memcpy(target_heap_ptr, source_heap_ptr, source_heap_sizes[i]);
		D_ASSERT(Load<uint32_t>(target_heap_ptr) == source_heap_sizes[i]);
		target_heap_ptr += source_heap_sizes[i];
	}
	output_heap_block->count += count;
	output_heap_block->byte_offset += heap_copy_bytes;
}

void KWayMergeSorter::FlushRemainingFromRun(idx_t run_idx) {
	auto &buffer = input_buffers[run_idx];
	if (buffer->exhausted) {
		return;
	}

	auto *source = buffer->source;
	const idx_t entry_size = sort_layout.entry_size;
	const idx_t row_width = payload_layout.GetRowWidth();

	while (!buffer->exhausted) {
		idx_t block_idx = buffer->current_block_idx;
		idx_t start_entry = buffer->current_entry_idx;

		// Calculate how many entries we can copy from this block
		idx_t entries_in_block = source->radix_sorting_data[block_idx]->count;
		idx_t available = entries_in_block - start_entry;
		idx_t output_space = output_capacity - output_count;
		idx_t to_copy = MinValue(available, output_space);
		to_copy = MinValue(to_copy, buffer->total_remaining);

		if (to_copy == 0) {
			if (output_count >= output_capacity) {
				FlushOutputBuffer();
				AllocateOutputBuffers();
			}
			continue;
		}

		// Bulk copy radix data
		auto radix_handle = buffer_manager.Pin(source->radix_sorting_data[block_idx]->block);
		data_ptr_t src_radix = radix_handle.Ptr() + start_entry * entry_size;
		FastMemcpy(output_radix_ptr + output_count * entry_size, src_radix, to_copy * entry_size);

		// Bulk copy payload data
		auto &payload_data = source->payload_data;
		if (block_idx < payload_data->data_blocks.size()) {
			auto payload_handle = buffer_manager.Pin(payload_data->data_blocks[block_idx]->block);
			data_ptr_t src_payload = payload_handle.Ptr() + start_entry * row_width;
			FastMemcpy(output_payload_ptr + output_count * row_width, src_payload, to_copy * row_width);
		}

		// Bulk copy blob data if needed
			if (!sort_layout.all_constant && source->blob_sorting_data) {
				auto &blob_data = source->blob_sorting_data;
				if (block_idx < blob_data->data_blocks.size()) {
					idx_t blob_width = sort_layout.blob_layout.GetRowWidth();
					auto blob_handle = buffer_manager.Pin(blob_data->data_blocks[block_idx]->block);
					data_ptr_t src_blob = blob_handle.Ptr() + start_entry * blob_width;
					FastMemcpy(output_blob_data_handle.Ptr() + output_count * blob_width, src_blob, to_copy * blob_width);
				}
			}

		output_count += to_copy;
		buffer->current_entry_idx += to_copy;
		buffer->total_remaining -= to_copy;

		// Check if we need to move to next block
		if (buffer->current_entry_idx >= entries_in_block) {
			buffer->current_block_idx++;
			buffer->current_entry_idx = 0;
			if (buffer->current_block_idx >= source->radix_sorting_data.size()) {
				buffer->exhausted = true;
			}
		}

		if (buffer->total_remaining == 0) {
			buffer->exhausted = true;
		}

		// Flush output if full
		if (output_count >= output_capacity) {
			FlushOutputBuffer();
			AllocateOutputBuffers();
		}
	}
}

//===--------------------------------------------------------------------===//
// Parallel Merge Implementation
//===--------------------------------------------------------------------===//

void KWayMergeSorter::InitializePartitioner(const vector<SortedBlock *> &runs, idx_t partition_size) {
	current_runs = runs;
	partition_result_slots.clear();

	// Create partitioner and compute partitions
	partitioner = make_uniq<KWayMergePartitioner>(state, buffer_manager, partition_size);
	partitioner->ComputePartitions(runs);
	partition_result_slots.resize(partitioner->NumPartitions());
#ifndef NDEBUG
	{
		std::ostringstream run_counts_ss;
		run_counts_ss << "[";
		for (idx_t i = 0; i < runs.size(); i++) {
			if (i > 0) {
				run_counts_ss << ",";
			}
			run_counts_ss << runs[i]->Count();
		}
		run_counts_ss << "]";
		Printer::Print("[KWAY_DEBUG] InitializePartitioner: input_runs=" + std::to_string(runs.size()) +
		               " partition_size=" + std::to_string(partition_size) + " run_counts=" + run_counts_ss.str() +
		               " num_partitions=" + std::to_string(partitioner->NumPartitions()));
	}
#endif
}

void KWayMergeSorter::MergePartition(const KWayPartition &partition, unique_ptr<SortedBlock> &result_block) {
	const idx_t k = partition.run_partitions.size();

	if (partition.total_elements == 0) {
		result_block = nullptr;
		return;
	}

	// Create input buffers for this partition's slices
	vector<unique_ptr<InputRunBuffer>> partition_buffers;
	partition_buffers.reserve(k);

	for (idx_t i = 0; i < k; i++) {
		auto &rp = partition.run_partitions[i];
		auto buffer = make_uniq<InputRunBuffer>();

		if (rp.Count() > 0) {
			buffer->source = current_runs[i];
			buffer->total_remaining = rp.Count();
			buffer->exhausted = false;

			// Convert global start index to block/entry
			idx_t block_idx = 0;
			idx_t entry_idx = 0;
			idx_t remaining = rp.start_idx;

			for (auto &block : current_runs[i]->radix_sorting_data) {
				if (remaining < block->count) {
					entry_idx = remaining;
					break;
				}
				remaining -= block->count;
				block_idx++;
			}

			buffer->current_block_idx = block_idx;
			buffer->current_entry_idx = entry_idx;
			buffer->PinCurrentBlock(buffer_manager, sort_layout);
		} else {
			buffer->source = nullptr;
			buffer->exhausted = true;
			buffer->total_remaining = 0;
		}

		partition_buffers.push_back(std::move(buffer));
	}

	// Create scan states for merge tree
	vector<unique_ptr<SBScanState>> partition_scan_states;
	partition_scan_states.reserve(k);

	for (idx_t i = 0; i < k; i++) {
		auto scan_state = make_uniq<SBScanState>(buffer_manager, state);
		if (partition_buffers[i]->source && !partition_buffers[i]->exhausted) {
			scan_state->sb = partition_buffers[i]->source;
			scan_state->block_idx = partition_buffers[i]->current_block_idx;
			scan_state->entry_idx = partition_buffers[i]->current_entry_idx;

			if (!sort_layout.all_constant && scan_state->sb->blob_sorting_data) {
				scan_state->PinData(*scan_state->sb->blob_sorting_data);
			}
		}
		partition_scan_states.push_back(std::move(scan_state));
	}

	// Create merge tree
	auto partition_merge_tree = make_uniq<MergeTree>(k, sort_layout, state.external);

	// Initialize merge tree with initial keys
	vector<data_ptr_t> initial_keys;
	vector<SBScanState *> scan_state_ptrs;
	for (idx_t i = 0; i < k; i++) {
		initial_keys.push_back(partition_buffers[i]->GetCurrentKey(sort_layout));
		scan_state_ptrs.push_back(partition_scan_states[i].get());
	}
	partition_merge_tree->Initialize(initial_keys, scan_state_ptrs);

	// Create output block
	result_block = make_uniq<SortedBlock>(buffer_manager, state);
	result_block->InitializeWrite();

	auto &radix_block = result_block->radix_sorting_data.back();
	auto output_radix_handle_local = buffer_manager.Pin(radix_block->block);
	data_ptr_t output_radix_ptr_local = output_radix_handle_local.Ptr();
	idx_t output_capacity_local = radix_block->capacity;

	auto &payload_block = result_block->payload_data->data_blocks.back();
	auto output_payload_handle_local = buffer_manager.Pin(payload_block->block);
	data_ptr_t output_payload_ptr_local = output_payload_handle_local.Ptr();
	BufferHandle output_payload_heap_handle_local;
	BufferHandle output_blob_data_handle_local;
	BufferHandle output_blob_heap_handle_local;
	if (state.external && !payload_layout.AllConstant()) {
		auto &payload_heap_block = result_block->payload_data->heap_blocks.back();
		output_payload_heap_handle_local = buffer_manager.Pin(payload_heap_block->block);
	}
	if (!sort_layout.all_constant) {
		auto &blob_data_block = result_block->blob_sorting_data->data_blocks.back();
		output_blob_data_handle_local = buffer_manager.Pin(blob_data_block->block);
		if (state.external && !sort_layout.blob_layout.AllConstant()) {
			auto &blob_heap_block = result_block->blob_sorting_data->heap_blocks.back();
			output_blob_heap_handle_local = buffer_manager.Pin(blob_heap_block->block);
		}
	}

	idx_t output_count_local = 0;
	auto increment_write_round = [&]() {
		auto &buffer_pool = buffer_manager.GetBufferPool();
		if (buffer_pool.IsProfilingEnabled() && buffer_pool.GetMetrics()) {
			auto *metrics = const_cast<BufferPoolMetrics *>(buffer_pool.GetMetrics());
			metrics->transfer_write_rounds++;
		}
	};

	// Batch merge arrays
	MergeDecision decisions[KWAY_MERGE_BATCH_SIZE];
	const idx_t entry_size = sort_layout.entry_size;
	const idx_t row_width = payload_layout.GetRowWidth();
	struct PartitionBatchSourceEntry {
		data_ptr_t radix_ptr = nullptr;
		data_ptr_t payload_data_ptr = nullptr;
		data_ptr_t payload_heap_ptr = nullptr;
		data_ptr_t blob_data_ptr = nullptr;
		data_ptr_t blob_heap_ptr = nullptr;
		BufferHandle radix_handle;
		BufferHandle payload_data_handle;
		BufferHandle payload_heap_handle;
		BufferHandle blob_data_handle;
		BufferHandle blob_heap_handle;
	};
	auto source_key = [](idx_t run_idx, idx_t block_idx) {
		return (uint64_t(run_idx) << 32) | uint64_t(block_idx);
	};

	// Batched merge loop
	while (!partition_merge_tree->AllExhausted()) {
		// Phase 1: Pop winners batch
		idx_t remaining_capacity = output_capacity_local - output_count_local;
		idx_t max_batch = MinValue(remaining_capacity, (idx_t)KWAY_MERGE_BATCH_SIZE);
		idx_t batch_size = 0;

		while (batch_size < max_batch && !partition_merge_tree->AllExhausted()) {
			idx_t winner = partition_merge_tree->GetWinner();
			if (winner == MergeTree::INVALID_RUN) {
				break;
			}

			auto &winner_buffer = partition_buffers[winner];

			decisions[batch_size].run_idx = winner;
			decisions[batch_size].block_idx = winner_buffer->current_block_idx;
			decisions[batch_size].entry_idx = winner_buffer->current_entry_idx;
			batch_size++;

			bool has_more = winner_buffer->Advance(buffer_manager, sort_layout);

			if (has_more) {
				partition_scan_states[winner]->block_idx = winner_buffer->current_block_idx;
				partition_scan_states[winner]->entry_idx = winner_buffer->current_entry_idx;
				if (!sort_layout.all_constant && partition_scan_states[winner]->sb->blob_sorting_data) {
					partition_scan_states[winner]->PinData(*partition_scan_states[winner]->sb->blob_sorting_data);
				}
			}

			data_ptr_t new_key = has_more ? winner_buffer->GetCurrentKey(sort_layout) : nullptr;
			partition_merge_tree->ReplaceWinner(winner, new_key, partition_scan_states[winner].get());
		}

			if (batch_size == 0) {
				break;
			}
			vector<PartitionBatchSourceEntry> batch_sources;
			std::unordered_map<uint64_t, idx_t> batch_source_lookup;
			batch_sources.reserve(batch_size);
			batch_source_lookup.reserve(batch_size);
			auto get_source_entry = [&](idx_t run_idx, idx_t blk_idx) -> PartitionBatchSourceEntry & {
				auto key = source_key(run_idx, blk_idx);
				auto it = batch_source_lookup.find(key);
				if (it != batch_source_lookup.end()) {
					return batch_sources[it->second];
				}
				PartitionBatchSourceEntry entry;
				auto *source = partition_buffers[run_idx]->source;
				D_ASSERT(source);
				entry.radix_handle = buffer_manager.Pin(source->radix_sorting_data[blk_idx]->block);
				entry.radix_ptr = entry.radix_handle.Ptr();
				auto &payload_data = source->payload_data;
				if (blk_idx < payload_data->data_blocks.size()) {
					entry.payload_data_handle = buffer_manager.Pin(payload_data->data_blocks[blk_idx]->block);
					entry.payload_data_ptr = entry.payload_data_handle.Ptr();
				}
				if (state.external && !payload_layout.AllConstant() && blk_idx < payload_data->heap_blocks.size()) {
					entry.payload_heap_handle = buffer_manager.Pin(payload_data->heap_blocks[blk_idx]->block);
					entry.payload_heap_ptr = entry.payload_heap_handle.Ptr();
				}
				if (!sort_layout.all_constant && source->blob_sorting_data &&
				    blk_idx < source->blob_sorting_data->data_blocks.size()) {
					entry.blob_data_handle = buffer_manager.Pin(source->blob_sorting_data->data_blocks[blk_idx]->block);
					entry.blob_data_ptr = entry.blob_data_handle.Ptr();
					if (state.external && !sort_layout.blob_layout.AllConstant() &&
					    blk_idx < source->blob_sorting_data->heap_blocks.size()) {
						entry.blob_heap_handle = buffer_manager.Pin(source->blob_sorting_data->heap_blocks[blk_idx]->block);
						entry.blob_heap_ptr = entry.blob_heap_handle.Ptr();
					}
				}
				auto idx = batch_sources.size();
				batch_sources.push_back(std::move(entry));
				batch_source_lookup.emplace(key, idx);
				return batch_sources.back();
			};
			for (idx_t i = 0; i < batch_size; i++) {
				get_source_entry(decisions[i].run_idx, decisions[i].block_idx);
			}

			// Phase 2: Batch-copy radix data
			{
				data_ptr_t out_ptr = output_radix_ptr_local + output_count_local * entry_size;
				idx_t i = 0;
				while (i < batch_size) {
					const idx_t run_idx = decisions[i].run_idx;
					const idx_t blk_idx = decisions[i].block_idx;
					auto &entry = get_source_entry(run_idx, blk_idx);
					data_ptr_t src_base = entry.radix_ptr;

				while (i < batch_size && decisions[i].run_idx == run_idx && decisions[i].block_idx == blk_idx) {
					idx_t j = i;
					idx_t start_entry = decisions[j].entry_idx;
					while (j < batch_size && decisions[j].run_idx == run_idx && decisions[j].block_idx == blk_idx &&
					       decisions[j].entry_idx == start_entry + (j - i)) {
						j++;
					}
					idx_t consecutive = j - i;
					FastMemcpy(out_ptr, src_base + start_entry * entry_size, consecutive * entry_size);
					out_ptr += consecutive * entry_size;
					i = j;
				}
			}
		}

			// Phase 3: Batch-copy blob data (if needed)
			if (!sort_layout.all_constant) {
				const idx_t blob_width = sort_layout.blob_layout.GetRowWidth();
				auto &out_blob = result_block->blob_sorting_data;
				if (sort_layout.blob_layout.AllConstant() || !state.external) {
					data_ptr_t out_ptr = output_blob_data_handle_local.Ptr() + output_count_local * blob_width;

					idx_t i = 0;
					while (i < batch_size) {
						const idx_t run_idx = decisions[i].run_idx;
						const idx_t blk_idx = decisions[i].block_idx;
						auto &entry = get_source_entry(run_idx, blk_idx);
						if (!entry.blob_data_ptr) {
							i++;
							continue;
						}
						data_ptr_t src_base = entry.blob_data_ptr;

					while (i < batch_size && decisions[i].run_idx == run_idx && decisions[i].block_idx == blk_idx) {
						idx_t j = i;
						idx_t start_entry = decisions[j].entry_idx;
						while (j < batch_size && decisions[j].run_idx == run_idx && decisions[j].block_idx == blk_idx &&
						       decisions[j].entry_idx == start_entry + (j - i)) {
							j++;
						}
						idx_t consecutive = j - i;
						FastMemcpy(out_ptr, src_base + start_entry * blob_width, consecutive * blob_width);
						out_ptr += consecutive * blob_width;
						i = j;
					}
				}
				} else {
					D_ASSERT(sort_layout.blob_layout.GetHeapOffset() < blob_width);
					auto &output_blob_data = *out_blob;
					data_ptr_t output_data_ptr = output_blob_data_handle_local.Ptr() + output_count_local * blob_width;

					auto &output_heap_block = output_blob_data.heap_blocks.back();
					const idx_t heap_pointer_offset = sort_layout.blob_layout.GetHeapOffset();

				idx_t source_heap_offsets[KWAY_MERGE_BATCH_SIZE];
				idx_t source_heap_sizes[KWAY_MERGE_BATCH_SIZE];
					idx_t heap_copy_bytes = 0;

					for (idx_t i = 0; i < batch_size; i++) {
						const idx_t run_idx = decisions[i].run_idx;
						const idx_t blk_idx = decisions[i].block_idx;
						auto &entry = get_source_entry(run_idx, blk_idx);
						D_ASSERT(entry.blob_data_ptr && entry.blob_heap_ptr);
						auto source_row_ptr = entry.blob_data_ptr + decisions[i].entry_idx * blob_width;
						auto target_row_ptr = output_data_ptr + i * blob_width;
						FastMemcpy(target_row_ptr, source_row_ptr, blob_width);

						auto source_heap_offset = Load<idx_t>(source_row_ptr + heap_pointer_offset);
						auto source_heap_ptr = entry.blob_heap_ptr + source_heap_offset;
						auto entry_size = Load<uint32_t>(source_heap_ptr);
					D_ASSERT(entry_size >= sizeof(uint32_t));
					source_heap_offsets[i] = source_heap_offset;
					source_heap_sizes[i] = entry_size;
					Store<idx_t>(output_heap_block->byte_offset + heap_copy_bytes, target_row_ptr + heap_pointer_offset);
					heap_copy_bytes += entry_size;
				}

				if (output_heap_block->byte_offset + heap_copy_bytes > output_heap_block->capacity) {
					auto new_capacity = output_heap_block->byte_offset + heap_copy_bytes;
					buffer_manager.ReAllocate(output_heap_block->block, new_capacity);
					output_heap_block->capacity = new_capacity;
				}
					auto target_heap_ptr = output_blob_heap_handle_local.Ptr() + output_heap_block->byte_offset;
					for (idx_t i = 0; i < batch_size; i++) {
						const idx_t run_idx = decisions[i].run_idx;
						const idx_t blk_idx = decisions[i].block_idx;
						auto &entry = get_source_entry(run_idx, blk_idx);
						auto source_heap_ptr = entry.blob_heap_ptr + source_heap_offsets[i];
						memcpy(target_heap_ptr, source_heap_ptr, source_heap_sizes[i]);
					D_ASSERT(Load<uint32_t>(target_heap_ptr) == source_heap_sizes[i]);
					target_heap_ptr += source_heap_sizes[i];
				}
				output_heap_block->count += batch_size;
				output_heap_block->byte_offset += heap_copy_bytes;
			}
		}

		// Phase 4: Batch-copy payload data
			{
				if (payload_layout.AllConstant() || !state.external) {
					data_ptr_t out_ptr = output_payload_ptr_local + output_count_local * row_width;
					idx_t i = 0;
					while (i < batch_size) {
						const idx_t run_idx = decisions[i].run_idx;
						const idx_t blk_idx = decisions[i].block_idx;
						auto &entry = get_source_entry(run_idx, blk_idx);
						if (!entry.payload_data_ptr) {
							i++;
							continue;
						}
						data_ptr_t src_base = entry.payload_data_ptr;

					while (i < batch_size && decisions[i].run_idx == run_idx && decisions[i].block_idx == blk_idx) {
						idx_t j = i;
						idx_t start_entry = decisions[j].entry_idx;
						while (j < batch_size && decisions[j].run_idx == run_idx && decisions[j].block_idx == blk_idx &&
						       decisions[j].entry_idx == start_entry + (j - i)) {
							j++;
						}
						idx_t consecutive = j - i;
						FastMemcpy(out_ptr, src_base + start_entry * row_width, consecutive * row_width);
						out_ptr += consecutive * row_width;
						i = j;
					}
				}
				} else {
					D_ASSERT(payload_layout.GetHeapOffset() < row_width);
					auto &output_payload_data = *result_block->payload_data;
					data_ptr_t output_data_ptr = output_payload_handle_local.Ptr() + output_count_local * row_width;

					auto &output_heap_block = output_payload_data.heap_blocks.back();
					const idx_t heap_pointer_offset = payload_layout.GetHeapOffset();

				idx_t source_heap_offsets[KWAY_MERGE_BATCH_SIZE];
				idx_t source_heap_sizes[KWAY_MERGE_BATCH_SIZE];
					idx_t heap_copy_bytes = 0;

					for (idx_t i = 0; i < batch_size; i++) {
						const idx_t run_idx = decisions[i].run_idx;
						const idx_t blk_idx = decisions[i].block_idx;
						auto &entry = get_source_entry(run_idx, blk_idx);
						D_ASSERT(entry.payload_data_ptr && entry.payload_heap_ptr);
						auto source_row_ptr = entry.payload_data_ptr + decisions[i].entry_idx * row_width;
						auto target_row_ptr = output_data_ptr + i * row_width;
						FastMemcpy(target_row_ptr, source_row_ptr, row_width);

						auto source_heap_offset = Load<idx_t>(source_row_ptr + heap_pointer_offset);
						auto source_heap_ptr = entry.payload_heap_ptr + source_heap_offset;
						auto entry_size = Load<uint32_t>(source_heap_ptr);
					D_ASSERT(entry_size >= sizeof(uint32_t));
					source_heap_offsets[i] = source_heap_offset;
					source_heap_sizes[i] = entry_size;
					Store<idx_t>(output_heap_block->byte_offset + heap_copy_bytes, target_row_ptr + heap_pointer_offset);
					heap_copy_bytes += entry_size;
				}

				if (output_heap_block->byte_offset + heap_copy_bytes > output_heap_block->capacity) {
					auto new_capacity = output_heap_block->byte_offset + heap_copy_bytes;
					buffer_manager.ReAllocate(output_heap_block->block, new_capacity);
					output_heap_block->capacity = new_capacity;
				}
					auto target_heap_ptr = output_payload_heap_handle_local.Ptr() + output_heap_block->byte_offset;
					for (idx_t i = 0; i < batch_size; i++) {
						const idx_t run_idx = decisions[i].run_idx;
						const idx_t blk_idx = decisions[i].block_idx;
						auto &entry = get_source_entry(run_idx, blk_idx);
						auto source_heap_ptr = entry.payload_heap_ptr + source_heap_offsets[i];
						memcpy(target_heap_ptr, source_heap_ptr, source_heap_sizes[i]);
					D_ASSERT(Load<uint32_t>(target_heap_ptr) == source_heap_sizes[i]);
					target_heap_ptr += source_heap_sizes[i];
				}
				output_heap_block->count += batch_size;
				output_heap_block->byte_offset += heap_copy_bytes;
			}
		}

		output_count_local += batch_size;

		// Check if we need to allocate a new output block
			if (output_count_local >= output_capacity_local) {
				result_block->radix_sorting_data.back()->count = output_count_local;
				result_block->payload_data->data_blocks.back()->count = output_count_local;
				if (!sort_layout.all_constant) {
					result_block->blob_sorting_data->data_blocks.back()->count = output_count_local;
				}
				if (state.external && !payload_layout.AllConstant()) {
					result_block->payload_data->heap_blocks.back()->count = output_count_local;
				}
				if (state.external && !sort_layout.all_constant) {
					result_block->blob_sorting_data->heap_blocks.back()->count = output_count_local;
				}

				// Full buffer rollover in partitioned k-way merge.
				increment_write_round();

				result_block->CreateBlock();
				if (!sort_layout.all_constant) {
					result_block->blob_sorting_data->CreateBlock();
				}
				result_block->payload_data->CreateBlock();
					auto &new_radix_block = result_block->radix_sorting_data.back();
					output_radix_handle_local = buffer_manager.Pin(new_radix_block->block);
					output_radix_ptr_local = output_radix_handle_local.Ptr();
					output_capacity_local = new_radix_block->capacity;

				auto &new_payload_block = result_block->payload_data->data_blocks.back();
				output_payload_handle_local = buffer_manager.Pin(new_payload_block->block);
				output_payload_ptr_local = output_payload_handle_local.Ptr();
				if (state.external && !payload_layout.AllConstant()) {
					auto &new_payload_heap_block = result_block->payload_data->heap_blocks.back();
					output_payload_heap_handle_local = buffer_manager.Pin(new_payload_heap_block->block);
				}
				if (!sort_layout.all_constant) {
					auto &new_blob_data_block = result_block->blob_sorting_data->data_blocks.back();
					output_blob_data_handle_local = buffer_manager.Pin(new_blob_data_block->block);
					if (state.external && !sort_layout.blob_layout.AllConstant()) {
						auto &new_blob_heap_block = result_block->blob_sorting_data->heap_blocks.back();
						output_blob_heap_handle_local = buffer_manager.Pin(new_blob_heap_block->block);
					}
				}

			output_count_local = 0;
		}
	}

	// Finalize output block counts
	if (output_count_local > 0) {
		result_block->radix_sorting_data.back()->count = output_count_local;
		result_block->payload_data->data_blocks.back()->count = output_count_local;
		if (!sort_layout.all_constant) {
			result_block->blob_sorting_data->data_blocks.back()->count = output_count_local;
		}
		if (state.external && !payload_layout.AllConstant()) {
			result_block->payload_data->heap_blocks.back()->count = output_count_local;
		}
		if (state.external && !sort_layout.all_constant) {
			result_block->blob_sorting_data->heap_blocks.back()->count = output_count_local;
		}

		// Final (possibly partial) partition output flush.
		increment_write_round();
	}
}

void KWayMergeSorter::AddResultBlock(unique_ptr<SortedBlock> block, idx_t partition_idx) {
	if (!block) {
		return;
	}
	D_ASSERT(partition_idx < partition_result_slots.size());
	D_ASSERT(!partition_result_slots[partition_idx]);
	partition_result_slots[partition_idx] = std::move(block);
}

void KWayMergeSorter::FinalizeParallelMerge() {
	idx_t prior_runs = state.sorted_blocks.size();
	idx_t parallel_blocks = partition_result_slots.size();

	// Move results to result_blocks in order
	result_blocks.clear();
	for (auto &slot : partition_result_slots) {
		if (slot) {
			result_blocks.push_back(std::move(slot));
		}
	}
	idx_t merged_output_blocks = result_blocks.size();
	partition_result_slots.clear();

	// Update state.sorted_blocks
	state.sorted_blocks.clear();
	for (auto &result : result_blocks) {
		state.sorted_blocks.push_back(std::move(result));
	}
#ifndef NDEBUG
	Printer::Print("[KWAY_DEBUG] FinalizeParallelMerge: prior_runs=" + std::to_string(prior_runs) +
	               " current_runs=" + std::to_string(current_runs.size()) + " partition_results=" +
	               std::to_string(parallel_blocks) + " merged_output_blocks=" + std::to_string(merged_output_blocks) +
	               " new_runs=" + std::to_string(state.sorted_blocks.size()));
#endif
	result_blocks.clear();

	// Clean up
	partitioner.reset();
	current_runs.clear();
}

} // namespace duckdb
