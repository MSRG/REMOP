#include "duckdb/execution/operator/join/physical_blockwise_nl_join_optimized.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/outer_join_marker.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/operator/join/physical_cross_product.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Global Sink State - extends original with size tracking
//===--------------------------------------------------------------------===//
class BlockedBlockwiseNLJGlobalState : public GlobalSinkState {
public:
	explicit BlockedBlockwiseNLJGlobalState(ClientContext &context, const PhysicalBlockwiseNLJoinOptimized &op,
	                                        const NLJPolicyConfig &config)
	    : right_chunks(context, op.children[1]->GetTypes()), right_outer(PropagatesBuildSide(op.join_type)),
	      use_blocked_mode(false), rhs_budget_bytes(config.GetShortTableBufferBudget()),
	      lhs_budget_bytes(config.GetLongTableBufferBudget()), rhs_total_bytes(0) {
	}

	mutex lock;
	//! Materialized data of the RHS (no separate condition data for blockwise NLJ)
	ColumnDataCollection right_chunks;
	//! Marker for right outer join
	OuterJoinMarker right_outer;

	//! Whether to use blocked execution mode
	bool use_blocked_mode;
	//! Budget for RHS blocks (inner side)
	idx_t rhs_budget_bytes;
	//! Budget for LHS buffer (outer side)
	idx_t lhs_budget_bytes;
	//! Total size of RHS data
	idx_t rhs_total_bytes;
};

class BlockedBlockwiseNLJLocalSinkState : public LocalSinkState {
public:
	BlockedBlockwiseNLJLocalSinkState() {
	}
};

//===--------------------------------------------------------------------===//
// Operator State - for blocked execution
//===--------------------------------------------------------------------===//
class BlockedBlockwiseNLJOperatorState : public CachingOperatorState {
public:
	//! Execution phases for blocked mode
	enum class Phase {
		ACCUMULATE_LHS,       // Buffering LHS chunks
		PROCESS_BLOCKS,       // Processing LHS buffer × RHS blocks
		DRAIN_FINAL_RESULTS,  // Draining remaining results after blocks done
		FINISHED              // Done
	};

	BlockedBlockwiseNLJOperatorState(ExecutionContext &context, const PhysicalBlockwiseNLJoinOptimized &op,
	                                 ColumnDataCollection &rhs, idx_t lhs_budget, idx_t rhs_budget, idx_t output_budget)
	    : // LHS buffer (buffer-managed ColumnDataCollection) - NO condition buffer for blockwise
	      lhs_payload_buffer(context.client, op.children[0]->types),
	      lhs_buffer_bytes(0),
	      lhs_budget_bytes(lhs_budget),
	      // Cross product executor for regular mode
	      cross_product(rhs),
	      // Expression executor for filtering
	      executor(context.client, *op.condition),
	      // State flags
	      match_sel(STANDARD_VECTOR_SIZE),
	      left_outer(IsLeftOuterJoin(op.join_type)),
	      // Blocked mode state
	      current_phase(Phase::ACCUMULATE_LHS),
	      rhs_block_start_row(0),
	      rhs_block_end_row(0),
	      rhs_budget_bytes(rhs_budget),
	      lhs_scan_initialized(false),
	      rhs_scan_initialized(false),
	      pending_input(false),
	      current_lhs_position(0),
	      current_rhs_position(0),
	      // Result buffer state
	      result_buffer(context.client, op.types),
	      result_buffer_bytes(0),
	      result_buffer_budget(output_budget),
	      result_scan_initialized(false),
	      draining_results(false) {

		auto &allocator = Allocator::Get(context.client);

		// Intermediate chunk for cross product (LHS types + RHS types)
		vector<LogicalType> intermediate_types;
		for (auto &type : op.children[0]->types) {
			intermediate_types.push_back(type);
		}
		for (auto &type : op.children[1]->types) {
			intermediate_types.push_back(type);
		}
		intermediate_chunk.Initialize(allocator, intermediate_types);

		// Scratch chunks for scanning
		lhs_payload_chunk.Initialize(allocator, op.children[0]->types);
		rhs_chunk.Initialize(allocator, op.children[1]->types);

		// Result buffer scratch chunk
		result_build_chunk.Initialize(allocator, op.types);

		// Initialize outer join marker
		left_outer.Initialize(STANDARD_VECTOR_SIZE);
	}

	//--- LHS Buffer (NO condition buffer - key difference from NLJ) ---
	ColumnDataCollection lhs_payload_buffer;
	idx_t lhs_buffer_bytes;
	idx_t lhs_budget_bytes;

	//--- Scanning State ---
	ColumnDataScanState lhs_payload_scan;
	ColumnDataScanState rhs_scan_state;

	//--- Scratch Chunks ---
	DataChunk lhs_payload_chunk;
	DataChunk rhs_chunk;
	DataChunk intermediate_chunk;  // For cross product output

	//--- Cross Product Executor (for regular mode) ---
	CrossProductExecutor cross_product;

	//--- Expression Executor (for filtering) ---
	ExpressionExecutor executor;
	SelectionVector match_sel;

	//--- Outer Join Marker ---
	OuterJoinMarker left_outer;

	//--- Blocked Mode State ---
	Phase current_phase;
	idx_t rhs_block_start_row;
	idx_t rhs_block_end_row;
	idx_t rhs_budget_bytes;
	bool lhs_scan_initialized;
	bool rhs_scan_initialized;
	bool pending_input;

	//--- Position tracking for blocked cross product ---
	idx_t current_lhs_position;  // Current position within LHS chunk
	idx_t current_rhs_position;  // Current position within RHS chunk

	//--- Block Pinning State ---
	unordered_map<uint32_t, BufferHandle> lhs_payload_handles;
	unordered_map<uint32_t, BufferHandle> rhs_handles;

	//--- Result Buffer State ---
	ColumnDataCollection result_buffer;
	idx_t result_buffer_bytes;
	idx_t result_buffer_budget;
	DataChunk result_build_chunk;
	ColumnDataScanState result_scan_state;
	bool result_scan_initialized;
	bool draining_results;

	//--- Helper Methods ---

	//! Try to add an LHS chunk to the buffer, returns false if would exceed budget
	bool TryAddLHSChunk(DataChunk &payload) {
		idx_t chunk_bytes = EstimateChunkSize(payload);
		// Always allow at least one chunk
		if (lhs_buffer_bytes > 0 && lhs_buffer_bytes + chunk_bytes > lhs_budget_bytes) {
			return false;
		}
		lhs_payload_buffer.Append(payload);
		lhs_buffer_bytes += chunk_bytes;
		return true;
	}

	//! Reset the LHS buffer
	void ResetLHSBuffer() {
		// Release pinned handles first
		lhs_payload_handles.clear();
		// Reset the buffer
		lhs_payload_buffer.Reset();
		lhs_buffer_bytes = 0;
		lhs_scan_initialized = false;
	}

	//! Release RHS block handles
	void ReleaseRHSBlockHandles() {
		rhs_handles.clear();
	}

	//! Estimate chunk size in bytes
	static idx_t EstimateChunkSize(DataChunk &chunk) {
		if (chunk.size() == 0) {
			return 0;
		}
		idx_t total_bytes = 0;
		for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			auto &vec = chunk.data[col_idx];
			auto &type = vec.GetType();
			idx_t type_size = GetTypeIdSize(type.InternalType());
			total_bytes += chunk.size() * type_size;
			if (type.InternalType() == PhysicalType::VARCHAR) {
				total_bytes += chunk.size() * 16;  // Rough estimate for strings
			}
		}
		return total_bytes;
	}

	//! Try to add result chunk to buffer
	bool TryAddResultChunk(DataChunk &result_chunk) {
		idx_t chunk_bytes = EstimateChunkSize(result_chunk);
		if (result_buffer_bytes > 0 && result_buffer_bytes + chunk_bytes > result_buffer_budget) {
			return false;
		}
		result_buffer.Append(result_chunk);
		result_buffer_bytes += chunk_bytes;
		return true;
	}

	//! Drain one chunk from result buffer to output
	bool DrainResultBuffer(DataChunk &output) {
		if (result_buffer.Count() == 0) {
			return false;
		}

		if (!result_scan_initialized) {
			result_buffer.InitializeScan(result_scan_state);
			result_scan_initialized = true;
		}

		if (result_buffer.Scan(result_scan_state, output)) {
			return true;
		}

		// Buffer exhausted - reset for next batch
		ResetResultBuffer();
		return false;
	}

	//! Reset the result buffer after draining
	void ResetResultBuffer() {
		result_buffer.Reset();
		result_buffer_bytes = 0;
		result_scan_initialized = false;
	}

	//! Pin all chunks in the LHS buffer
	void PinLHSBuffer() {
		auto &payload_segments = lhs_payload_buffer.GetSegments();
		for (auto &segment : payload_segments) {
			auto handles = segment->PinChunkRange(0, segment->ChunkCount());
			for (auto &kv : handles) {
				lhs_payload_handles[kv.first] = std::move(kv.second);
			}
		}
	}

	//! Pin RHS blocks for the given chunk range
	void PinRHSChunkRange(const ColumnDataCollection &rhs, idx_t start_chunk, idx_t end_chunk) {
		ReleaseRHSBlockHandles();

		auto &segments = rhs.GetSegments();
		idx_t current_chunk = 0;
		for (auto &segment : segments) {
			idx_t segment_chunk_count = segment->ChunkCount();
			idx_t segment_start = current_chunk;
			idx_t segment_end = current_chunk + segment_chunk_count;

			if (segment_end > start_chunk && segment_start < end_chunk) {
				idx_t local_start = (start_chunk > segment_start) ? (start_chunk - segment_start) : 0;
				idx_t local_end = (end_chunk < segment_end) ? (end_chunk - segment_start) : segment_chunk_count;

				auto handles = segment->PinChunkRange(local_start, local_end);
				for (auto &kv : handles) {
					rhs_handles[kv.first] = std::move(kv.second);
				}
			}
			current_chunk += segment_chunk_count;
		}
	}

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, executor, "executor", 0);
	}
};

//===--------------------------------------------------------------------===//
// Source State - for RIGHT/FULL OUTER join
//===--------------------------------------------------------------------===//
class BlockedBlockwiseNLJGlobalScanState : public GlobalSourceState {
public:
	explicit BlockedBlockwiseNLJGlobalScanState(const PhysicalBlockwiseNLJoinOptimized &op) : op(op) {
		D_ASSERT(op.sink_state);
		auto &sink = op.sink_state->Cast<BlockedBlockwiseNLJGlobalState>();
		sink.right_outer.InitializeScan(sink.right_chunks, scan_state);
	}

	const PhysicalBlockwiseNLJoinOptimized &op;
	OuterJoinGlobalScanState scan_state;

public:
	idx_t MaxThreads() override {
		auto &sink = op.sink_state->Cast<BlockedBlockwiseNLJGlobalState>();
		return sink.right_outer.MaxThreads();
	}
};

class BlockedBlockwiseNLJLocalScanState : public LocalSourceState {
public:
	explicit BlockedBlockwiseNLJLocalScanState(const PhysicalBlockwiseNLJoinOptimized &op,
	                                           BlockedBlockwiseNLJGlobalScanState &gstate) {
		D_ASSERT(op.sink_state);
		auto &sink = op.sink_state->Cast<BlockedBlockwiseNLJGlobalState>();
		sink.right_outer.InitializeScan(gstate.scan_state, scan_state);
	}

	OuterJoinLocalScanState scan_state;
};

//===--------------------------------------------------------------------===//
// Constructor
//===--------------------------------------------------------------------===//
PhysicalBlockwiseNLJoinOptimized::PhysicalBlockwiseNLJoinOptimized(LogicalOperator &op,
                                                                   unique_ptr<PhysicalOperator> left,
                                                                   unique_ptr<PhysicalOperator> right,
                                                                   unique_ptr<Expression> condition,
                                                                   JoinType join_type, idx_t estimated_cardinality,
                                                                   shared_ptr<OperatorMemoryPolicy> policy_p)
    : PhysicalBlockwiseNLJoin(op, std::move(left), std::move(right), std::move(condition), join_type,
                              estimated_cardinality),
      policy(std::move(policy_p)) {
	type = PhysicalOperatorType::BLOCKWISE_NL_JOIN_OPTIMIZED;
	Printer::Print("PhysicalBlockwiseNLJoinOptimized: Initialized with memory-aware execution");
}

//===--------------------------------------------------------------------===//
// Sink Interface
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> PhysicalBlockwiseNLJoinOptimized::GetGlobalSinkState(ClientContext &context) const {
	auto config = policy->GetBlockwiseNLJConfig();
	return make_uniq<BlockedBlockwiseNLJGlobalState>(context, *this, config);
}

unique_ptr<LocalSinkState> PhysicalBlockwiseNLJoinOptimized::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<BlockedBlockwiseNLJLocalSinkState>();
}

SinkResultType PhysicalBlockwiseNLJoinOptimized::Sink(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BlockedBlockwiseNLJGlobalState>();
	lock_guard<mutex> nl_lock(gstate.lock);
	gstate.right_chunks.Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalBlockwiseNLJoinOptimized::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                            OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<BlockedBlockwiseNLJGlobalState>();

	// Initialize outer join marker
	gstate.right_outer.Initialize(gstate.right_chunks.Count());

	// Check for empty RHS
	if (gstate.right_chunks.Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Compute total RHS size
	gstate.rhs_total_bytes = gstate.right_chunks.SizeInBytes();

	// Decide whether to use blocked mode
	if (gstate.rhs_budget_bytes > 0 && gstate.rhs_total_bytes > gstate.rhs_budget_bytes) {
		gstate.use_blocked_mode = true;
		Printer::Print("PhysicalBlockwiseNLJoinOptimized: Using BLOCKED mode (RHS=" +
		               to_string(gstate.rhs_total_bytes) + " bytes > budget=" + to_string(gstate.rhs_budget_bytes) +
		               " bytes)");
	} else {
		gstate.use_blocked_mode = false;
		Printer::Print("PhysicalBlockwiseNLJoinOptimized: Using REGULAR mode (RHS=" +
		               to_string(gstate.rhs_total_bytes) + " bytes <= budget=" + to_string(gstate.rhs_budget_bytes) +
		               " bytes)");
	}

	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator Interface
//===--------------------------------------------------------------------===//
unique_ptr<OperatorState> PhysicalBlockwiseNLJoinOptimized::GetOperatorState(ExecutionContext &context) const {
	auto &gstate = sink_state->Cast<BlockedBlockwiseNLJGlobalState>();
	auto config = policy->GetBlockwiseNLJConfig();
	auto result = make_uniq<BlockedBlockwiseNLJOperatorState>(context, *this, gstate.right_chunks,
	                                                          config.GetLongTableBufferBudget(),
	                                                          config.GetShortTableBufferBudget(),
	                                                          config.GetOutputBufferBudget());

	// Handle special join types that need intermediate chunks
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
		vector<LogicalType> intermediate_types;
		for (auto &type : children[0]->types) {
			intermediate_types.emplace_back(type);
		}
		for (auto &type : children[1]->types) {
			intermediate_types.emplace_back(type);
		}
		result->intermediate_chunk.Initialize(Allocator::DefaultAllocator(), intermediate_types);
	}
	if (join_type == JoinType::RIGHT_ANTI || join_type == JoinType::RIGHT_SEMI) {
		throw NotImplementedException("physical blockwise RIGHT_SEMI/RIGHT_ANTI join not yet implemented");
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Helper: Estimate chunk size
//===--------------------------------------------------------------------===//
idx_t PhysicalBlockwiseNLJoinOptimized::EstimateChunkSize(DataChunk &chunk) {
	return BlockedBlockwiseNLJOperatorState::EstimateChunkSize(chunk);
}

//===--------------------------------------------------------------------===//
// Main Execution Entry Point
//===--------------------------------------------------------------------===//
OperatorResultType PhysicalBlockwiseNLJoinOptimized::ExecuteInternal(ExecutionContext &context, DataChunk &input,
                                                                     DataChunk &chunk, GlobalOperatorState &gstate_p,
                                                                     OperatorState &state_p) const {
	auto &gstate = sink_state->Cast<BlockedBlockwiseNLJGlobalState>();
	auto &state = state_p.Cast<BlockedBlockwiseNLJOperatorState>();

	// Handle empty RHS
	if (gstate.right_chunks.Count() == 0) {
		if (!EmptyResultIfRHSIsEmpty()) {
			PhysicalComparisonJoin::ConstructEmptyJoinResult(join_type, false, input, chunk);
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::FINISHED;
		}
	}

	// Choose execution path based on mode
	if (!gstate.use_blocked_mode) {
		return ExecuteRegularBlockwiseNLJ(context, input, chunk, gstate, state);
	} else {
		return ExecuteBlockedBlockwiseNLJ(context, input, chunk, gstate, state);
	}
}

//===--------------------------------------------------------------------===//
// Regular Blockwise NLJ Execution (RHS fits in memory)
// This is essentially the original PhysicalBlockwiseNLJoin::ExecuteInternal
//===--------------------------------------------------------------------===//
OperatorResultType PhysicalBlockwiseNLJoinOptimized::ExecuteRegularBlockwiseNLJ(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk, BlockedBlockwiseNLJGlobalState &gstate,
    BlockedBlockwiseNLJOperatorState &state) const {

	DataChunk *intermediate_chunk = &chunk;
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
		intermediate_chunk = &state.intermediate_chunk;
		intermediate_chunk->Reset();
	}

	idx_t result_count = 0;
	bool found_match[STANDARD_VECTOR_SIZE] = {false};

	do {
		auto result = state.cross_product.Execute(input, *intermediate_chunk);
		if (result == OperatorResultType::NEED_MORE_INPUT) {
			// exhausted input, have to pull new LHS chunk
			if (state.left_outer.Enabled()) {
				state.left_outer.ConstructLeftJoinResult(input, *intermediate_chunk);
				state.left_outer.Reset();
			}

			if (join_type == JoinType::SEMI) {
				PhysicalJoin::ConstructSemiJoinResult(input, chunk, found_match);
			}
			if (join_type == JoinType::ANTI) {
				PhysicalJoin::ConstructAntiJoinResult(input, chunk, found_match);
			}

			return OperatorResultType::NEED_MORE_INPUT;
		}

		// now perform the computation
		result_count = state.executor.SelectExpression(*intermediate_chunk, state.match_sel);

		// handle anti and semi joins with different logic
		if (result_count > 0) {
			if (join_type == JoinType::ANTI || join_type == JoinType::SEMI) {
				if (state.cross_product.ScanLHS()) {
					found_match[state.cross_product.PositionInChunk()] = true;
				} else {
					for (idx_t i = 0; i < result_count; i++) {
						found_match[state.match_sel.get_index(i)] = true;
					}
				}
				intermediate_chunk->Reset();
				result_count = 0;
			} else {
				// set the match flags
				if (!state.cross_product.ScanLHS()) {
					state.left_outer.SetMatches(state.match_sel, result_count);
					gstate.right_outer.SetMatch(state.cross_product.ScanPosition() +
					                            state.cross_product.PositionInChunk());
				} else {
					state.left_outer.SetMatch(state.cross_product.PositionInChunk());
					gstate.right_outer.SetMatches(state.match_sel, result_count, state.cross_product.ScanPosition());
				}
				intermediate_chunk->Slice(state.match_sel, result_count);
			}
		} else {
			intermediate_chunk->Reset();
		}
	} while (result_count == 0);

	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Blocked Blockwise NLJ Execution (RHS exceeds memory)
//===--------------------------------------------------------------------===//
OperatorResultType PhysicalBlockwiseNLJoinOptimized::ExecuteBlockedBlockwiseNLJ(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk, BlockedBlockwiseNLJGlobalState &gstate,
    BlockedBlockwiseNLJOperatorState &state) const {
	using Phase = BlockedBlockwiseNLJOperatorState::Phase;

	switch (state.current_phase) {

	case Phase::ACCUMULATE_LHS: {
		// Try to add to buffer (no condition evaluation for blockwise NLJ)
		if (state.TryAddLHSChunk(input)) {
			return OperatorResultType::NEED_MORE_INPUT;
		}

		// Buffer full! Transition to processing
		state.current_phase = Phase::PROCESS_BLOCKS;
		state.rhs_block_start_row = 0;
		state.rhs_block_end_row = 0;
		state.lhs_scan_initialized = false;
		state.rhs_scan_initialized = false;
		state.pending_input = true;

		// Pin the entire LHS buffer before processing
		state.PinLHSBuffer();

		DUCKDB_EXPLICIT_FALLTHROUGH;
	}

	case Phase::PROCESS_BLOCKS: {
		return ProcessBlockedBlockwiseJoin(context, input, chunk, gstate, state);
	}

	case Phase::DRAIN_FINAL_RESULTS: {
		if (state.DrainResultBuffer(chunk)) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}

		// Buffer drained, reset and go back to accumulating
		state.ResetResultBuffer();
		state.ResetLHSBuffer();
		state.current_phase = Phase::ACCUMULATE_LHS;
		state.rhs_block_start_row = 0;
		state.rhs_block_end_row = 0;

		// If we have a pending input chunk, buffer it now
		if (state.pending_input) {
			state.pending_input = false;
			state.TryAddLHSChunk(input);
		}

		return OperatorResultType::NEED_MORE_INPUT;
	}

	case Phase::FINISHED:
		return OperatorResultType::FINISHED;
	}

	return OperatorResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Process Blocked Blockwise Join - Core blocked execution logic
//===--------------------------------------------------------------------===//
OperatorResultType PhysicalBlockwiseNLJoinOptimized::ProcessBlockedBlockwiseJoin(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk, BlockedBlockwiseNLJGlobalState &gstate,
    BlockedBlockwiseNLJOperatorState &state) const {

	auto &allocator = Allocator::Get(context.client);

	while (true) {
		//--- Step 0: Check if we're draining buffered results ---
		if (state.draining_results) {
			if (state.DrainResultBuffer(chunk)) {
				return OperatorResultType::HAVE_MORE_OUTPUT;
			}
			state.draining_results = false;
		}

		//--- Step 1: Ensure we have an RHS block defined ---
		if (!state.rhs_scan_initialized) {
			if (!ComputeNextRHSBlock(context, gstate, state)) {
				// No more RHS blocks - this LHS buffer is done
				if (state.result_buffer.Count() > 0) {
					state.current_phase = BlockedBlockwiseNLJOperatorState::Phase::DRAIN_FINAL_RESULTS;
					if (state.DrainResultBuffer(chunk)) {
						return OperatorResultType::HAVE_MORE_OUTPUT;
					}
				}

				state.ResetLHSBuffer();
				state.current_phase = BlockedBlockwiseNLJOperatorState::Phase::ACCUMULATE_LHS;
				state.rhs_block_start_row = 0;
				state.rhs_block_end_row = 0;

				if (state.pending_input) {
					state.pending_input = false;
					state.TryAddLHSChunk(input);
				}

				return OperatorResultType::NEED_MORE_INPUT;
			}

			// Initialize RHS scan at block start
			gstate.right_chunks.InitializeScan(state.rhs_scan_state);

			// Skip to block start position
			DataChunk skip_chunk;
			skip_chunk.Initialize(allocator, gstate.right_chunks.Types());
			idx_t skipped = 0;
			while (skipped < state.rhs_block_start_row) {
				if (!gstate.right_chunks.Scan(state.rhs_scan_state, skip_chunk)) {
					break;
				}
				skipped += skip_chunk.size();
			}

			state.rhs_scan_initialized = true;
		}

		//--- Step 2: Ensure LHS scan initialized ---
		if (!state.lhs_scan_initialized) {
			state.lhs_payload_buffer.InitializeScan(state.lhs_payload_scan);
			state.lhs_scan_initialized = true;

			if (!state.lhs_payload_buffer.Scan(state.lhs_payload_scan, state.lhs_payload_chunk)) {
				state.rhs_scan_initialized = false;
				state.rhs_block_start_row = state.rhs_block_end_row;
				continue;
			}
			// C_read: first LHS chunk scanned
			{
				auto &buffer_pool = BufferManager::GetBufferManager(context.client).GetBufferPool();
				if (buffer_pool.IsProfilingEnabled() && buffer_pool.GetMetrics()) {
					const_cast<BufferPoolMetrics *>(buffer_pool.GetMetrics())->transfer_read_rounds++;
				}
			}
			state.current_lhs_position = 0;
			state.current_rhs_position = 0;

			// Get first RHS chunk in this block
			if (state.rhs_scan_state.current_row_index < state.rhs_block_end_row) {
				gstate.right_chunks.Scan(state.rhs_scan_state, state.rhs_chunk);
				// C_read: first RHS chunk scanned
				{
					auto &buffer_pool = BufferManager::GetBufferManager(context.client).GetBufferPool();
					if (buffer_pool.IsProfilingEnabled() && buffer_pool.GetMetrics()) {
						const_cast<BufferPoolMetrics *>(buffer_pool.GetMetrics())->transfer_read_rounds++;
					}
				}
			}
		}

		//--- Step 3: Generate cross product batches and filter ---
		if (state.rhs_scan_state.current_row_index <= state.rhs_block_end_row && state.rhs_chunk.size() > 0) {

			// Process in batches for efficiency
			idx_t batch_size = STANDARD_VECTOR_SIZE;
			state.intermediate_chunk.Reset();
			idx_t output_idx = 0;

			// Build cross product batch
			idx_t start_lhs_pos = state.current_lhs_position;
			idx_t start_rhs_pos = state.current_rhs_position;

			for (idx_t l = start_lhs_pos; l < state.lhs_payload_chunk.size() && output_idx < batch_size; l++) {
				for (idx_t r = (l == start_lhs_pos ? start_rhs_pos : 0);
				     r < state.rhs_chunk.size() && output_idx < batch_size; r++) {

					// Copy LHS row
					for (idx_t col = 0; col < state.lhs_payload_chunk.ColumnCount(); col++) {
						ConstantVector::Reference(state.intermediate_chunk.data[col], state.lhs_payload_chunk.data[col],
						                          l, state.lhs_payload_chunk.size());
					}

					// Copy RHS row
					idx_t lhs_col_count = state.lhs_payload_chunk.ColumnCount();
					for (idx_t col = 0; col < state.rhs_chunk.ColumnCount(); col++) {
						ConstantVector::Reference(state.intermediate_chunk.data[lhs_col_count + col],
						                          state.rhs_chunk.data[col], r, state.rhs_chunk.size());
					}

					output_idx++;

					// Update position for next iteration
					state.current_rhs_position = r + 1;
					if (state.current_rhs_position >= state.rhs_chunk.size()) {
						state.current_rhs_position = 0;
						state.current_lhs_position = l + 1;
					}
				}
			}

			if (output_idx > 0) {
				state.intermediate_chunk.SetCardinality(output_idx);

				// Evaluate expression on batch
				idx_t result_count = state.executor.SelectExpression(state.intermediate_chunk, state.match_sel);

				if (result_count > 0) {
					// Update outer join markers
					// Note: For blocked mode, we need to track matches differently
					// For now, we focus on INNER join correctness

					// Build result chunk from matches
					state.result_build_chunk.Reset();
					state.result_build_chunk.Slice(state.intermediate_chunk, state.match_sel, result_count);

					// Try to add to result buffer
					if (!state.TryAddResultChunk(state.result_build_chunk)) {
						// Buffer full! Increment write round counter - output buffer flush
						auto &buffer_manager = BufferManager::GetBufferManager(context.client);
						auto &buffer_pool = buffer_manager.GetBufferPool();
						if (buffer_pool.IsProfilingEnabled() && buffer_pool.GetMetrics()) {
							auto *metrics = const_cast<BufferPoolMetrics *>(buffer_pool.GetMetrics());
							metrics->transfer_write_rounds++;
						}

						// Drain buffer first
						state.DrainResultBuffer(chunk);

						// Add current results to now-empty buffer
						state.result_buffer.Append(state.result_build_chunk);
						state.result_buffer_bytes = BlockedBlockwiseNLJOperatorState::EstimateChunkSize(state.result_build_chunk);

						state.draining_results = true;
						return OperatorResultType::HAVE_MORE_OUTPUT;
					}
				}
			}

			// Check if we exhausted current chunks
			if (state.current_lhs_position >= state.lhs_payload_chunk.size()) {
				state.current_lhs_position = 0;
				state.current_rhs_position = 0;

				// Move to next RHS chunk in block
				if (state.rhs_scan_state.current_row_index < state.rhs_block_end_row) {
					if (gstate.right_chunks.Scan(state.rhs_scan_state, state.rhs_chunk)) {
						// C_read: next RHS chunk scanned
						{
							auto &buffer_pool = BufferManager::GetBufferManager(context.client).GetBufferPool();
							if (buffer_pool.IsProfilingEnabled() && buffer_pool.GetMetrics()) {
								const_cast<BufferPoolMetrics *>(buffer_pool.GetMetrics())->transfer_read_rounds++;
							}
						}
						continue;
					}
				}

				// Exhausted RHS block for current LHS chunk - get next LHS chunk
				if (state.lhs_payload_buffer.Scan(state.lhs_payload_scan, state.lhs_payload_chunk)) {
					// C_read: next LHS chunk scanned
					{
						auto &buffer_pool = BufferManager::GetBufferManager(context.client).GetBufferPool();
						if (buffer_pool.IsProfilingEnabled() && buffer_pool.GetMetrics()) {
							const_cast<BufferPoolMetrics *>(buffer_pool.GetMetrics())->transfer_read_rounds++;
						}
					}
					// Reset RHS scan to block start
					gstate.right_chunks.InitializeScan(state.rhs_scan_state);

					DataChunk skip_chunk;
					skip_chunk.Initialize(allocator, gstate.right_chunks.Types());
					idx_t skipped = 0;
					while (skipped < state.rhs_block_start_row) {
						if (!gstate.right_chunks.Scan(state.rhs_scan_state, skip_chunk)) {
							break;
						}
						skipped += skip_chunk.size();
					}

					if (state.rhs_scan_state.current_row_index < state.rhs_block_end_row) {
						gstate.right_chunks.Scan(state.rhs_scan_state, state.rhs_chunk);
						// C_read: first RHS chunk after LHS advance
						{
							auto &buffer_pool = BufferManager::GetBufferManager(context.client).GetBufferPool();
							if (buffer_pool.IsProfilingEnabled() && buffer_pool.GetMetrics()) {
								const_cast<BufferPoolMetrics *>(buffer_pool.GetMetrics())->transfer_read_rounds++;
							}
						}
					}
					continue;
				}

				// Exhausted all LHS chunks for this RHS block - move to next RHS block
				state.lhs_scan_initialized = false;
				state.rhs_scan_initialized = false;
				state.rhs_block_start_row = state.rhs_block_end_row;
				continue;
			}

			continue;
		}

		// No more RHS in current block
		if (state.lhs_payload_buffer.Scan(state.lhs_payload_scan, state.lhs_payload_chunk)) {
			// C_read: next LHS chunk scanned (alternative path)
			{
				auto &buffer_pool = BufferManager::GetBufferManager(context.client).GetBufferPool();
				if (buffer_pool.IsProfilingEnabled() && buffer_pool.GetMetrics()) {
					const_cast<BufferPoolMetrics *>(buffer_pool.GetMetrics())->transfer_read_rounds++;
				}
			}
			state.current_lhs_position = 0;
			state.current_rhs_position = 0;

			// Reset RHS scan to block start
			gstate.right_chunks.InitializeScan(state.rhs_scan_state);

			DataChunk skip_chunk;
			skip_chunk.Initialize(allocator, gstate.right_chunks.Types());
			idx_t skipped = 0;
			while (skipped < state.rhs_block_start_row) {
				if (!gstate.right_chunks.Scan(state.rhs_scan_state, skip_chunk)) {
					break;
				}
				skipped += skip_chunk.size();
			}

			if (state.rhs_scan_state.current_row_index < state.rhs_block_end_row) {
				gstate.right_chunks.Scan(state.rhs_scan_state, state.rhs_chunk);
				// C_read: first RHS chunk (alternative path)
				{
					auto &buffer_pool = BufferManager::GetBufferManager(context.client).GetBufferPool();
					if (buffer_pool.IsProfilingEnabled() && buffer_pool.GetMetrics()) {
						const_cast<BufferPoolMetrics *>(buffer_pool.GetMetrics())->transfer_read_rounds++;
					}
				}
			}
			continue;
		}

		// Exhausted LHS buffer - move to next RHS block
		state.lhs_scan_initialized = false;
		state.rhs_scan_initialized = false;
		state.rhs_block_start_row = state.rhs_block_end_row;
	}
}

//===--------------------------------------------------------------------===//
// Compute Next RHS Block
//===--------------------------------------------------------------------===//
bool PhysicalBlockwiseNLJoinOptimized::ComputeNextRHSBlock(ExecutionContext &context, BlockedBlockwiseNLJGlobalState &gstate,
                                                           BlockedBlockwiseNLJOperatorState &state) const {
	idx_t total_rhs_rows = gstate.right_chunks.Count();

	if (state.rhs_block_start_row >= total_rhs_rows) {
		state.ReleaseRHSBlockHandles();
		return false;
	}

	// Compute how many rows fit in rhs_budget
	idx_t avg_row_bytes =
	    (gstate.rhs_total_bytes > 0 && total_rhs_rows > 0) ? gstate.rhs_total_bytes / total_rhs_rows : 64;

	idx_t bytes = 0;
	idx_t row_idx = state.rhs_block_start_row;
	idx_t chunk_count = 0;

	idx_t start_chunk_idx = state.rhs_block_start_row / STANDARD_VECTOR_SIZE;

	while (row_idx < total_rhs_rows) {
		idx_t chunk_rows = MinValue<idx_t>(STANDARD_VECTOR_SIZE, total_rhs_rows - row_idx);
		idx_t chunk_bytes = chunk_rows * avg_row_bytes;

		if (bytes > 0 && bytes + chunk_bytes > state.rhs_budget_bytes) {
			break;
		}
		bytes += chunk_bytes;
		row_idx += chunk_rows;
		chunk_count++;
	}

	state.rhs_block_end_row = row_idx;

	idx_t end_chunk_idx = start_chunk_idx + chunk_count;
	state.PinRHSChunkRange(gstate.right_chunks, start_chunk_idx, end_chunk_idx);

	return true;
}

//===--------------------------------------------------------------------===//
// Source Interface - for RIGHT/FULL OUTER join
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSourceState> PhysicalBlockwiseNLJoinOptimized::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<BlockedBlockwiseNLJGlobalScanState>(*this);
}

unique_ptr<LocalSourceState>
PhysicalBlockwiseNLJoinOptimized::GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const {
	return make_uniq<BlockedBlockwiseNLJLocalScanState>(*this, gstate.Cast<BlockedBlockwiseNLJGlobalScanState>());
}

SourceResultType PhysicalBlockwiseNLJoinOptimized::GetData(ExecutionContext &context, DataChunk &chunk,
                                                           OperatorSourceInput &input) const {
	D_ASSERT(PropagatesBuildSide(join_type));
	auto &sink = sink_state->Cast<BlockedBlockwiseNLJGlobalState>();
	auto &gstate = input.global_state.Cast<BlockedBlockwiseNLJGlobalScanState>();
	auto &lstate = input.local_state.Cast<BlockedBlockwiseNLJLocalScanState>();

	sink.right_outer.Scan(gstate.scan_state, lstate.scan_state, chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
