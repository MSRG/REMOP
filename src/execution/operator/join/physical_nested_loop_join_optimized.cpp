#include "duckdb/execution/operator/join/physical_nested_loop_join_optimized.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/nested_loop_join.hpp"
#include "duckdb/execution/operator/join/outer_join_marker.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Global Sink State - extends original with size tracking
//===--------------------------------------------------------------------===//
class BlockedNLJGlobalState : public GlobalSinkState {
public:
	explicit BlockedNLJGlobalState(ClientContext &context, const PhysicalNestedLoopJoinOptimized &op,
	                               const NLJPolicyConfig &config)
	    : right_payload_data(context, op.children[1]->types),
	      right_condition_data(context, op.GetJoinTypes()),
	      has_null(false),
	      right_outer(PropagatesBuildSide(op.join_type)),
	      use_blocked_mode(false),
	      rhs_budget_bytes(config.GetShortTableBufferBudget()),
	      lhs_budget_bytes(config.GetLongTableBufferBudget()),
	      rhs_total_bytes(0) {
	}

	mutex nj_lock;
	//! Materialized data of the RHS
	ColumnDataCollection right_payload_data;
	//! Materialized join condition of the RHS
	ColumnDataCollection right_condition_data;
	//! Whether or not the RHS of the nested loop join has NULL values
	atomic<bool> has_null;
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

class BlockedNLJLocalSinkState : public LocalSinkState {
public:
	explicit BlockedNLJLocalSinkState(ClientContext &context, const vector<JoinCondition> &conditions)
	    : rhs_executor(context) {
		vector<LogicalType> condition_types;
		for (auto &cond : conditions) {
			rhs_executor.AddExpression(*cond.right);
			condition_types.push_back(cond.right->return_type);
		}
		right_condition.Initialize(Allocator::Get(context), condition_types);
	}

	DataChunk right_condition;
	ExpressionExecutor rhs_executor;
};

static bool ScanAlignedRHSChunk(const ColumnDataCollection &payload_data, const ColumnDataCollection &condition_data,
                                ColumnDataScanState &payload_scan, ColumnDataScanState &condition_scan,
                                DataChunk &payload_chunk, DataChunk &condition_chunk, const char *scan_stage) {
	auto has_payload = payload_data.Scan(payload_scan, payload_chunk);
	auto has_condition = condition_data.Scan(condition_scan, condition_chunk);
	if (has_payload != has_condition) {
		throw InternalException("Optimized NLJ blocked mode: payload/condition scan mismatch at " + string(scan_stage));
	}
	if (has_payload && payload_chunk.size() != condition_chunk.size()) {
		throw InternalException("Optimized NLJ blocked mode: payload/condition row count mismatch at " +
		                        string(scan_stage));
	}
	return has_payload;
}

static inline void BumpTransferReadRound(ClientContext &client) {
	auto &buffer_pool = BufferManager::GetBufferManager(client).GetBufferPool();
	if (buffer_pool.IsProfilingEnabled() && buffer_pool.GetMetrics()) {
		const_cast<BufferPoolMetrics *>(buffer_pool.GetMetrics())->transfer_read_rounds++;
	}
}

static inline void BumpTransferWriteRound(ClientContext &client) {
	auto &buffer_pool = BufferManager::GetBufferManager(client).GetBufferPool();
	if (buffer_pool.IsProfilingEnabled() && buffer_pool.GetMetrics()) {
		const_cast<BufferPoolMetrics *>(buffer_pool.GetMetrics())->transfer_write_rounds++;
	}
}

//===--------------------------------------------------------------------===//
// Operator State - for blocked execution
//===--------------------------------------------------------------------===//
class BlockedNLJOperatorState : public CachingOperatorState {
public:
	//! Execution phases for blocked mode
	enum class Phase {
		ACCUMULATE_LHS,       // Buffering LHS chunks
		PROCESS_BLOCKS,       // Processing LHS buffer × RHS blocks
		DRAIN_FINAL_RESULTS,  // Draining remaining results after blocks done
		FINISHED              // Done
	};

	BlockedNLJOperatorState(ClientContext &context, const PhysicalNestedLoopJoinOptimized &op,
	                        const vector<JoinCondition> &conditions, idx_t lhs_budget, idx_t rhs_budget,
	                        idx_t output_budget)
	    : // LHS buffer (buffer-managed ColumnDataCollection)
	      lhs_payload_buffer(context, op.children[0]->types),
	      lhs_condition_buffer(context, GetLHSConditionTypes(conditions)),
	      rhs_block_payload_buffer(context, op.children[1]->types),
	      rhs_block_condition_buffer(context, GetRHSConditionTypes(conditions)),
	      lhs_buffer_bytes(0),
	      lhs_budget_bytes(lhs_budget),
	      rhs_block_bytes(0),
	      // LHS condition executor
	      lhs_executor(context),
	      // State flags
	      fetch_next_left(true),
	      fetch_next_right(false),
	      left_tuple(0),
	      right_tuple(0),
	      // Blocked mode state
	      current_phase(Phase::ACCUMULATE_LHS),
	      rhs_budget_bytes(rhs_budget),
	      rhs_global_scan_initialized(false),
	      rhs_global_row_offset(0),
	      rhs_block_global_start_row(0),
	      lhs_scan_initialized(false),
	      rhs_scan_initialized(false),
	      pending_input(false),
	      pending_input_valid(false),
	      // Result buffer state
	      result_buffer(context, op.types),
		      result_buffer_bytes(0),
		      result_buffer_budget(output_budget),
		      result_scan_initialized(false),
		      draining_results(false),
		      pending_result_valid(false),
		      // Outer join marker
		      left_outer(IsLeftOuterJoin(op.join_type)) {

		vector<LogicalType> condition_types;
		for (auto &cond : conditions) {
			lhs_executor.AddExpression(*cond.left);
			condition_types.push_back(cond.left->return_type);
		}

		auto &allocator = Allocator::Get(context);
		// Scratch chunks for condition evaluation
		lhs_condition_temp.Initialize(allocator, condition_types);
		// Scratch chunks for scanning
		lhs_payload_chunk.Initialize(allocator, op.children[0]->types);
		lhs_condition_chunk.Initialize(allocator, condition_types);
		rhs_payload_chunk.Initialize(allocator, op.children[1]->types);
		rhs_condition_chunk.Initialize(allocator, condition_types);
		// For regular NLJ mode
		left_condition.Initialize(allocator, condition_types);
		right_condition.Initialize(allocator, condition_types);
			right_payload.Initialize(allocator, op.children[1]->types);
			pending_payload.Initialize(allocator, op.children[0]->types);
			pending_condition.Initialize(allocator, condition_types);
			pending_result_chunk.Initialize(allocator, op.types);
			left_outer.Initialize(STANDARD_VECTOR_SIZE);
			// Result buffer scratch chunk (output types = LHS types + RHS types)
			result_build_chunk.Initialize(allocator, op.types);
	}

	static vector<LogicalType> GetLHSConditionTypes(const vector<JoinCondition> &conditions) {
		vector<LogicalType> types;
		for (auto &cond : conditions) {
			types.push_back(cond.left->return_type);
		}
		return types;
	}

	static vector<LogicalType> GetRHSConditionTypes(const vector<JoinCondition> &conditions) {
		vector<LogicalType> types;
		for (auto &cond : conditions) {
			types.push_back(cond.right->return_type);
		}
		return types;
	}

	//--- LHS Buffer (ColumnDataCollection = buffer-managed) ---
	ColumnDataCollection lhs_payload_buffer;
	ColumnDataCollection lhs_condition_buffer;
	ColumnDataCollection rhs_block_payload_buffer;
	ColumnDataCollection rhs_block_condition_buffer;
	idx_t lhs_buffer_bytes;
	idx_t lhs_budget_bytes;
	idx_t rhs_block_bytes;

	//--- LHS Condition Evaluation ---
	ExpressionExecutor lhs_executor;
	DataChunk lhs_condition_temp;

	//--- Scanning State ---
	ColumnDataScanState lhs_payload_scan;
	ColumnDataScanState lhs_condition_scan;
	ColumnDataScanState rhs_payload_scan;
	ColumnDataScanState rhs_condition_scan;
	ColumnDataScanState rhs_global_payload_scan;
	ColumnDataScanState rhs_global_condition_scan;

	//--- Scratch Chunks for Scanning ---
	DataChunk lhs_payload_chunk;
	DataChunk lhs_condition_chunk;
	DataChunk rhs_payload_chunk;
	DataChunk rhs_condition_chunk;

	//--- Regular NLJ mode state (when RHS fits in memory) ---
	bool fetch_next_left;
	bool fetch_next_right;
	DataChunk left_condition;
	ColumnDataScanState condition_scan_state;
	ColumnDataScanState payload_scan_state;
	DataChunk right_condition;
	DataChunk right_payload;
	idx_t left_tuple;
	idx_t right_tuple;

	//--- Blocked Mode State ---
	Phase current_phase;
	idx_t rhs_budget_bytes;
	bool rhs_global_scan_initialized;
	idx_t rhs_global_row_offset;
	idx_t rhs_block_global_start_row;
	bool lhs_scan_initialized;
	bool rhs_scan_initialized;
	bool pending_input;
	bool pending_input_valid;
	DataChunk pending_payload;
	DataChunk pending_condition;

	//--- Result Buffer State ---
	//! Buffer for accumulating join results
	ColumnDataCollection result_buffer;
	//! Current size of result buffer in bytes
	idx_t result_buffer_bytes;
	//! Maximum budget for result buffer from policy
	idx_t result_buffer_budget;
	//! Scratch chunk for building results before buffering
	DataChunk result_build_chunk;
	//! Scan state for draining results
	ColumnDataScanState result_scan_state;
	//! Whether result scan is initialized
	bool result_scan_initialized;
	//! Whether we are currently draining results (mid-process)
	bool draining_results;
	//! Pending result chunk staged while draining existing buffered output
	DataChunk pending_result_chunk;
	bool pending_result_valid;

	//--- Outer Join Marker ---
	OuterJoinMarker left_outer;

	//--- Helper Methods ---

	//! Try to add an LHS chunk to the buffer, returns false if would exceed budget
	bool TryAddLHSChunk(DataChunk &payload, DataChunk &condition) {
		idx_t chunk_bytes = EstimateChunkSize(payload) + EstimateChunkSize(condition);
		// Always allow at least one chunk
		if (lhs_buffer_bytes > 0 && lhs_buffer_bytes + chunk_bytes > lhs_budget_bytes) {
			return false;
		}
		lhs_payload_buffer.Append(payload);
		lhs_condition_buffer.Append(condition);
		lhs_buffer_bytes += chunk_bytes;
		return true;
	}

	//! Reset the LHS buffer
	void ResetLHSBuffer() {
		// Reset the buffer
		lhs_payload_buffer.Reset();
		lhs_condition_buffer.Reset();
		lhs_buffer_bytes = 0;
		lhs_scan_initialized = false;
	}

	void ResetRHSBlockBuffer() {
		rhs_block_payload_buffer.Reset();
		rhs_block_condition_buffer.Reset();
		rhs_block_bytes = 0;
		rhs_scan_initialized = false;
	}

	//! Estimate chunk size in bytes
	static idx_t EstimateChunkSize(DataChunk &chunk) {
		if (chunk.size() == 0) {
			return 0;
		}
		constexpr idx_t VARLEN_EST_BYTES = 1024;
		// Rough estimate: count data bytes
		idx_t total_bytes = 0;
		for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			auto &vec = chunk.data[col_idx];
			auto &type = vec.GetType();
			idx_t type_size = GetTypeIdSize(type.InternalType());
			total_bytes += chunk.size() * type_size;
			// Add extra for variable-length types
			if (type.InternalType() == PhysicalType::VARCHAR) {
				// Use 1KB/cell to better match the eval join-key workload.
				total_bytes += chunk.size() * VARLEN_EST_BYTES;
			}
		}
		return total_bytes;
	}

	//--- Result Buffer Helper Methods ---

	//! Try to add result chunk to buffer, returns false if would exceed budget
	//! If false, caller should drain buffer first, then call again
	bool TryAddResultChunk(DataChunk &result_chunk) {
		idx_t chunk_bytes = EstimateChunkSize(result_chunk);
		// Always allow at least one chunk (avoid deadlock when buffer is empty)
		if (result_buffer_bytes > 0 && result_buffer_bytes + chunk_bytes > result_buffer_budget) {
			return false;  // Buffer full - need to drain first
		}
		result_buffer.Append(result_chunk);
		result_buffer_bytes += chunk_bytes;
		return true;
	}

	//! Drain one chunk from result buffer to output, returns true if more data remains
	bool DrainResultBuffer(DataChunk &output) {
		if (result_buffer.Count() == 0) {
			return false;
		}

		if (!result_scan_initialized) {
			result_buffer.InitializeScan(result_scan_state);
			result_scan_initialized = true;
		}

		if (result_buffer.Scan(result_scan_state, output)) {
			return true;  // More results to drain
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

	//! Build result chunk from matched LHS/RHS rows
	void BuildResultChunk(DataChunk &lhs_chunk, DataChunk &rhs_chunk,
	                       SelectionVector &lvector, SelectionVector &rvector,
	                       idx_t match_count) {
		result_build_chunk.Reset();
		// Slice LHS columns into result
		result_build_chunk.Slice(lhs_chunk, lvector, match_count);
		// Slice RHS columns into result (offset by LHS column count)
		result_build_chunk.Slice(rhs_chunk, rvector, match_count, lhs_chunk.ColumnCount());
		result_build_chunk.SetCardinality(match_count);
	}

	void CapturePendingInput(DataChunk &payload, DataChunk &condition) {
		pending_payload.Reset();
		pending_payload.Append(payload);
		pending_condition.Reset();
		pending_condition.Append(condition);
		pending_input = true;
		pending_input_valid = true;
	}

	void ConsumePendingInput() {
		if (!pending_input || !pending_input_valid) {
			return;
		}
		TryAddLHSChunk(pending_payload, pending_condition);
		pending_payload.Reset();
		pending_condition.Reset();
		pending_input = false;
		pending_input_valid = false;
	}

	void StagePendingResultChunk(DataChunk &result_chunk) {
		pending_result_chunk.Reset();
		pending_result_chunk.Append(result_chunk);
		pending_result_valid = true;
	}

	void MaterializePendingResultChunk() {
		if (!pending_result_valid) {
			return;
		}
		result_buffer.Append(pending_result_chunk);
		result_buffer_bytes = EstimateChunkSize(pending_result_chunk);
		pending_result_chunk.Reset();
		pending_result_valid = false;
	}

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, lhs_executor, "lhs_executor", 0);
	}
};

//===--------------------------------------------------------------------===//
// Source State - for RIGHT/FULL OUTER join
//===--------------------------------------------------------------------===//
class BlockedNLJGlobalScanState : public GlobalSourceState {
public:
	explicit BlockedNLJGlobalScanState(const PhysicalNestedLoopJoinOptimized &op) : op(op) {
		D_ASSERT(op.sink_state);
		auto &sink = op.sink_state->Cast<BlockedNLJGlobalState>();
		sink.right_outer.InitializeScan(sink.right_payload_data, scan_state);
	}

	const PhysicalNestedLoopJoinOptimized &op;
	OuterJoinGlobalScanState scan_state;

public:
	idx_t MaxThreads() override {
		auto &sink = op.sink_state->Cast<BlockedNLJGlobalState>();
		return sink.right_outer.MaxThreads();
	}
};

class BlockedNLJLocalScanState : public LocalSourceState {
public:
	explicit BlockedNLJLocalScanState(const PhysicalNestedLoopJoinOptimized &op,
	                                   BlockedNLJGlobalScanState &gstate) {
		D_ASSERT(op.sink_state);
		auto &sink = op.sink_state->Cast<BlockedNLJGlobalState>();
		sink.right_outer.InitializeScan(gstate.scan_state, scan_state);
	}

	OuterJoinLocalScanState scan_state;
};

//===--------------------------------------------------------------------===//
// Constructor
//===--------------------------------------------------------------------===//
PhysicalNestedLoopJoinOptimized::PhysicalNestedLoopJoinOptimized(LogicalOperator &op,
                                                                 unique_ptr<PhysicalOperator> left,
                                                                 unique_ptr<PhysicalOperator> right,
                                                                 vector<JoinCondition> cond, JoinType join_type,
                                                                 idx_t estimated_cardinality,
                                                                 shared_ptr<OperatorMemoryPolicy> policy_p)
    : PhysicalNestedLoopJoin(op, std::move(left), std::move(right), std::move(cond), join_type,
                             estimated_cardinality),
      policy(std::move(policy_p)) {
	type = PhysicalOperatorType::NESTED_LOOP_JOIN_OPTIMIZED;
	Printer::Print("PhysicalNestedLoopJoinOptimized: Initialized with memory-aware execution");
}

//===--------------------------------------------------------------------===//
// Sink Interface
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> PhysicalNestedLoopJoinOptimized::GetGlobalSinkState(ClientContext &context) const {
	auto config = policy->GetNestedLoopJoinConfig();
	return make_uniq<BlockedNLJGlobalState>(context, *this, config);
}

unique_ptr<LocalSinkState> PhysicalNestedLoopJoinOptimized::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<BlockedNLJLocalSinkState>(context.client, conditions);
}

SinkResultType PhysicalNestedLoopJoinOptimized::Sink(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<BlockedNLJGlobalState>();
	auto &nlj_state = input.local_state.Cast<BlockedNLJLocalSinkState>();

	nlj_state.right_condition.Reset();
	nlj_state.rhs_executor.Execute(chunk, nlj_state.right_condition);

	if (join_type == JoinType::MARK && !gstate.has_null) {
		if (HasNullValues(nlj_state.right_condition)) {
			gstate.has_null = true;
		}
	}

	lock_guard<mutex> nj_guard(gstate.nj_lock);
	gstate.right_payload_data.Append(chunk);
	gstate.right_condition_data.Append(nlj_state.right_condition);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalNestedLoopJoinOptimized::Combine(ExecutionContext &context,
                                                               OperatorSinkCombineInput &input) const {
	auto &state = input.local_state.Cast<BlockedNLJLocalSinkState>();
	auto &client_profiler = QueryProfiler::Get(context.client);

	context.thread.profiler.Flush(*this, state.rhs_executor, "rhs_executor", 1);
	client_profiler.Flush(context.thread.profiler);
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalNestedLoopJoinOptimized::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                          OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<BlockedNLJGlobalState>();

	// Initialize outer join marker
	gstate.right_outer.Initialize(gstate.right_payload_data.Count());

	// Check for empty RHS
	if (gstate.right_payload_data.Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Compute total RHS size
	gstate.rhs_total_bytes = gstate.right_payload_data.SizeInBytes() + gstate.right_condition_data.SizeInBytes();

	// Decide whether to use blocked mode
	// Use blocked mode if RHS exceeds the RHS budget
	if (gstate.rhs_budget_bytes > 0 && gstate.rhs_total_bytes > gstate.rhs_budget_bytes) {
		gstate.use_blocked_mode = true;
		Printer::Print("PhysicalNestedLoopJoinOptimized: Using BLOCKED mode (RHS=" +
		              to_string(gstate.rhs_total_bytes) + " bytes > budget=" +
		              to_string(gstate.rhs_budget_bytes) + " bytes)");
	} else {
		gstate.use_blocked_mode = false;
		Printer::Print("PhysicalNestedLoopJoinOptimized: Using REGULAR mode (RHS=" +
		              to_string(gstate.rhs_total_bytes) + " bytes <= budget=" +
		              to_string(gstate.rhs_budget_bytes) + " bytes)");
	}

	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator Interface
//===--------------------------------------------------------------------===//
unique_ptr<OperatorState> PhysicalNestedLoopJoinOptimized::GetOperatorState(ExecutionContext &context) const {
	auto config = policy->GetNestedLoopJoinConfig();
	return make_uniq<BlockedNLJOperatorState>(context.client, *this, conditions,
	                                          config.GetLongTableBufferBudget(),
	                                          config.GetShortTableBufferBudget(),
	                                          config.GetOutputBufferBudget());
}

bool PhysicalNestedLoopJoinOptimized::RequiresFinalExecute() const {
	// Blocked mode may retain a final under-full LHS batch that must be flushed at pipeline end.
	return true;
}

OperatorFinalizeResultType PhysicalNestedLoopJoinOptimized::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                                         GlobalOperatorState &gstate_p,
                                                                         OperatorState &state_p) const {
	auto &gstate = sink_state->Cast<BlockedNLJGlobalState>();
	auto &state = state_p.Cast<BlockedNLJOperatorState>();

	// First flush any cached chunk from CachingPhysicalOperator.
	if (state.cached_chunk) {
		chunk.Move(*state.cached_chunk);
		state.cached_chunk.reset();
		return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
	}

	if (!gstate.use_blocked_mode) {
		return OperatorFinalizeResultType::FINISHED;
	}

	// If source exhausted while we were still buffering LHS, process that buffered tail now.
	if (state.current_phase == BlockedNLJOperatorState::Phase::ACCUMULATE_LHS && state.lhs_payload_buffer.Count() > 0) {
		state.current_phase = BlockedNLJOperatorState::Phase::PROCESS_BLOCKS;
		state.ResetRHSBlockBuffer();
		state.lhs_scan_initialized = false;
		state.rhs_scan_initialized = false;
		BumpTransferReadRound(context.client);
	}

	if (state.current_phase == BlockedNLJOperatorState::Phase::PROCESS_BLOCKS) {
		DataChunk empty_input;
		empty_input.Initialize(Allocator::Get(context.client), children[0]->types);
		auto result = ProcessBlockedJoin(context, empty_input, chunk, gstate, state);
		if (result == OperatorResultType::HAVE_MORE_OUTPUT) {
			return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
		}
	}

	if (state.current_phase == BlockedNLJOperatorState::Phase::DRAIN_FINAL_RESULTS) {
		if (state.DrainResultBuffer(chunk)) {
			return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
		}
		state.ResetResultBuffer();
		state.ResetLHSBuffer();
		state.ResetRHSBlockBuffer();
		state.current_phase = BlockedNLJOperatorState::Phase::ACCUMULATE_LHS;
	}

	return OperatorFinalizeResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helper: Estimate chunk size
//===--------------------------------------------------------------------===//
idx_t PhysicalNestedLoopJoinOptimized::EstimateChunkSize(DataChunk &chunk) {
	return BlockedNLJOperatorState::EstimateChunkSize(chunk);
}

//===--------------------------------------------------------------------===//
// Main Execution Entry Point
//===--------------------------------------------------------------------===//
OperatorResultType PhysicalNestedLoopJoinOptimized::ExecuteInternal(ExecutionContext &context, DataChunk &input,
                                                                    DataChunk &chunk, GlobalOperatorState &gstate_p,
                                                                    OperatorState &state_p) const {
	auto &gstate = sink_state->Cast<BlockedNLJGlobalState>();
	auto &state = state_p.Cast<BlockedNLJOperatorState>();

	// Handle empty RHS
	if (gstate.right_payload_data.Count() == 0) {
		if (!EmptyResultIfRHSIsEmpty()) {
			ConstructEmptyJoinResult(join_type, gstate.has_null, input, chunk);
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::FINISHED;
		}
	}

	// Handle simple joins (SEMI, ANTI, MARK) - these don't need blocked execution
	// because they only scan RHS once per LHS chunk and return at most STANDARD_VECTOR_SIZE rows
	switch (join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
		return ExecuteSimpleJoin(context, input, chunk, gstate, state);
	case JoinType::LEFT:
	case JoinType::INNER:
	case JoinType::OUTER:
	case JoinType::RIGHT:
		// Choose execution path based on mode for complex joins
		if (!gstate.use_blocked_mode) {
			return ExecuteRegularNLJ(context, input, chunk, gstate, state);
		} else {
			return ExecuteBlockedNLJ(context, input, chunk, gstate, state);
		}
	default:
		throw NotImplementedException("Unimplemented type " + JoinTypeToString(join_type) + " for optimized nested loop join!");
	}
}

//===--------------------------------------------------------------------===//
// Simple Join Execution (SEMI, ANTI, MARK)
//===--------------------------------------------------------------------===//
OperatorResultType PhysicalNestedLoopJoinOptimized::ExecuteSimpleJoin(ExecutionContext &context, DataChunk &input,
                                                                       DataChunk &chunk, BlockedNLJGlobalState &gstate,
                                                                       BlockedNLJOperatorState &state) const {
	// Resolve the left join condition for the current chunk
	state.left_condition.Reset();
	state.lhs_executor.Execute(input, state.left_condition);

	// Find matches
	bool found_match[STANDARD_VECTOR_SIZE] = {false};
	NestedLoopJoinMark::Perform(state.left_condition, gstate.right_condition_data, found_match, conditions);

	// Construct result based on join type
	switch (join_type) {
	case JoinType::MARK:
		PhysicalJoin::ConstructMarkJoinResult(state.left_condition, input, chunk, found_match, gstate.has_null);
		break;
	case JoinType::SEMI:
		PhysicalJoin::ConstructSemiJoinResult(input, chunk, found_match);
		break;
	case JoinType::ANTI:
		PhysicalJoin::ConstructAntiJoinResult(input, chunk, found_match);
		break;
	default:
		throw NotImplementedException("Unimplemented type for simple nested loop join!");
	}

	return OperatorResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Regular NLJ Execution (RHS fits in memory)
//===--------------------------------------------------------------------===//
OperatorResultType PhysicalNestedLoopJoinOptimized::ExecuteRegularNLJ(ExecutionContext &context, DataChunk &input,
                                                                      DataChunk &chunk, BlockedNLJGlobalState &gstate,
                                                                      BlockedNLJOperatorState &state) const {
	// This replicates the original NLJ ResolveComplexJoin logic
	// For INNER join (we focus on this first)

	idx_t match_count;
	do {
		if (state.fetch_next_right) {
			state.left_tuple = 0;
			state.right_tuple = 0;
			state.fetch_next_right = false;

			if (gstate.right_condition_data.Scan(state.condition_scan_state, state.right_condition)) {
				if (!gstate.right_payload_data.Scan(state.payload_scan_state, state.right_payload)) {
					throw InternalException("Optimized NLJ: payload and conditions are unaligned!");
				}
			} else {
				// Exhausted all RHS chunks
				state.fetch_next_left = true;
				if (state.left_outer.Enabled()) {
					state.left_outer.ConstructLeftJoinResult(input, chunk);
					state.left_outer.Reset();
				}
				return OperatorResultType::NEED_MORE_INPUT;
			}
		}

		if (state.fetch_next_left) {
			state.left_condition.Reset();
			state.lhs_executor.Execute(input, state.left_condition);

			state.left_tuple = 0;
			state.right_tuple = 0;
			gstate.right_condition_data.InitializeScan(state.condition_scan_state);
			gstate.right_condition_data.Scan(state.condition_scan_state, state.right_condition);

			gstate.right_payload_data.InitializeScan(state.payload_scan_state);
			gstate.right_payload_data.Scan(state.payload_scan_state, state.right_payload);
			state.fetch_next_left = false;
		}

		// Perform the join comparison
		SelectionVector lvector(STANDARD_VECTOR_SIZE), rvector(STANDARD_VECTOR_SIZE);
		match_count = NestedLoopJoinInner::Perform(state.left_tuple, state.right_tuple, state.left_condition,
		                                           state.right_condition, lvector, rvector, conditions);

		if (match_count > 0) {
			state.left_outer.SetMatches(lvector, match_count);
			gstate.right_outer.SetMatches(rvector, match_count, state.condition_scan_state.current_row_index);

			chunk.Slice(input, lvector, match_count);
			chunk.Slice(state.right_payload, rvector, match_count, input.ColumnCount());
		}

		if (state.right_tuple >= state.right_condition.size()) {
			state.fetch_next_right = true;
		}
	} while (match_count == 0);

	return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Blocked NLJ Execution (RHS exceeds memory)
//===--------------------------------------------------------------------===//
OperatorResultType PhysicalNestedLoopJoinOptimized::ExecuteBlockedNLJ(ExecutionContext &context, DataChunk &input,
                                                                       DataChunk &chunk, BlockedNLJGlobalState &gstate,
                                                                       BlockedNLJOperatorState &state) const {
	using Phase = BlockedNLJOperatorState::Phase;

	switch (state.current_phase) {

	case Phase::ACCUMULATE_LHS: {
		// Evaluate LHS conditions for this input chunk
		state.lhs_condition_temp.Reset();
		state.lhs_executor.Execute(input, state.lhs_condition_temp);

		// Try to add to buffer
		if (state.TryAddLHSChunk(input, state.lhs_condition_temp)) {
			// Successfully buffered, request more LHS
			return OperatorResultType::NEED_MORE_INPUT;
		}

		// Buffer full! Transition to processing
		// Note: Current input chunk is NOT in buffer yet - we'll add it after processing
			state.current_phase = Phase::PROCESS_BLOCKS;
			state.ResetRHSBlockBuffer();
			state.lhs_scan_initialized = false;
			state.rhs_scan_initialized = false;
			state.CapturePendingInput(input, state.lhs_condition_temp);
			BumpTransferReadRound(context.client);

		// Fall through to process blocks
		DUCKDB_EXPLICIT_FALLTHROUGH;
	}

	case Phase::PROCESS_BLOCKS: {
		return ProcessBlockedJoin(context, input, chunk, gstate, state);
	}

	case Phase::DRAIN_FINAL_RESULTS: {
		// Draining residual results from buffer after all blocks processed
		if (state.DrainResultBuffer(chunk)) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}

		// Buffer drained, reset and go back to accumulating
		state.ResetResultBuffer();
		state.ResetLHSBuffer();
		state.ResetRHSBlockBuffer();
		state.current_phase = Phase::ACCUMULATE_LHS;

			// If we have a pending input chunk, buffer it now
			if (state.pending_input) {
				state.ConsumePendingInput();
			}

			return OperatorResultType::NEED_MORE_INPUT;
	}

	case Phase::FINISHED:
		return OperatorResultType::FINISHED;
	}

	return OperatorResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Process Blocked Join - Core blocked execution logic
//===--------------------------------------------------------------------===//
OperatorResultType PhysicalNestedLoopJoinOptimized::ProcessBlockedJoin(ExecutionContext &context, DataChunk &input,
                                                                        DataChunk &chunk, BlockedNLJGlobalState &gstate,
                                                                        BlockedNLJOperatorState &state) const {
	(void)input;
	while (true) {
		if (state.draining_results) {
			if (state.DrainResultBuffer(chunk)) {
				return OperatorResultType::HAVE_MORE_OUTPUT;
			}
			state.draining_results = false;
			state.MaterializePendingResultChunk();
		}

		if (!state.rhs_scan_initialized) {
			if (!ComputeNextRHSBlock(context, gstate, state)) {
				if (state.result_buffer.Count() > 0) {
					state.current_phase = BlockedNLJOperatorState::Phase::DRAIN_FINAL_RESULTS;
					if (state.DrainResultBuffer(chunk)) {
						return OperatorResultType::HAVE_MORE_OUTPUT;
					}
				}

				state.ResetResultBuffer();
				state.ResetLHSBuffer();
				state.ResetRHSBlockBuffer();
				state.rhs_global_scan_initialized = false;
				state.rhs_global_row_offset = 0;
				state.rhs_block_global_start_row = 0;
				state.current_phase = BlockedNLJOperatorState::Phase::ACCUMULATE_LHS;

				if (state.pending_input) {
					state.ConsumePendingInput();
				}
				return OperatorResultType::NEED_MORE_INPUT;
			}

			state.rhs_block_payload_buffer.InitializeScan(state.rhs_payload_scan);
			state.rhs_block_condition_buffer.InitializeScan(state.rhs_condition_scan);
			state.rhs_scan_initialized = true;
		}

		if (!state.lhs_scan_initialized) {
			state.lhs_payload_buffer.InitializeScan(state.lhs_payload_scan);
			state.lhs_condition_buffer.InitializeScan(state.lhs_condition_scan);
			state.lhs_scan_initialized = true;

			if (!state.lhs_payload_buffer.Scan(state.lhs_payload_scan, state.lhs_payload_chunk)) {
				state.lhs_scan_initialized = false;
				state.rhs_scan_initialized = false;
				state.ResetRHSBlockBuffer();
				continue;
			}
			if (!state.lhs_condition_buffer.Scan(state.lhs_condition_scan, state.lhs_condition_chunk)) {
				throw InternalException("Optimized NLJ blocked mode: LHS payload/condition scans are unaligned");
			}
			if (state.lhs_payload_chunk.size() != state.lhs_condition_chunk.size()) {
				throw InternalException("Optimized NLJ blocked mode: LHS payload/condition row count mismatch");
			}
			state.left_tuple = 0;
			state.right_tuple = 0;

			if (!ScanAlignedRHSChunk(state.rhs_block_payload_buffer, state.rhs_block_condition_buffer, state.rhs_payload_scan,
			                         state.rhs_condition_scan, state.rhs_payload_chunk, state.rhs_condition_chunk,
			                         "rhs_block_first_chunk")) {
				state.lhs_scan_initialized = false;
				state.rhs_scan_initialized = false;
				state.ResetRHSBlockBuffer();
				continue;
			}
		}

		if (state.rhs_condition_chunk.size() > 0) {
			SelectionVector lvector(STANDARD_VECTOR_SIZE), rvector(STANDARD_VECTOR_SIZE);
			idx_t match_count =
			    NestedLoopJoinInner::Perform(state.left_tuple, state.right_tuple, state.lhs_condition_chunk,
			                               state.rhs_condition_chunk, lvector, rvector, conditions);

			if (match_count > 0) {
				auto rhs_global_base = state.rhs_block_global_start_row + state.rhs_condition_scan.current_row_index;
				gstate.right_outer.SetMatches(rvector, match_count, rhs_global_base);

				state.BuildResultChunk(state.lhs_payload_chunk, state.rhs_payload_chunk, lvector, rvector, match_count);
				if (!state.TryAddResultChunk(state.result_build_chunk)) {
					BumpTransferWriteRound(context.client);

					state.StagePendingResultChunk(state.result_build_chunk);
					if (state.DrainResultBuffer(chunk)) {
						state.draining_results = true;
						return OperatorResultType::HAVE_MORE_OUTPUT;
					}
					state.MaterializePendingResultChunk();
				}
			}

			if (state.right_tuple >= state.rhs_condition_chunk.size()) {
				state.left_tuple = 0;
				state.right_tuple = 0;

				if (ScanAlignedRHSChunk(state.rhs_block_payload_buffer, state.rhs_block_condition_buffer,
				                        state.rhs_payload_scan, state.rhs_condition_scan, state.rhs_payload_chunk,
				                        state.rhs_condition_chunk, "rhs_block_next_chunk")) {
					continue;
				}

				if (state.lhs_payload_buffer.Scan(state.lhs_payload_scan, state.lhs_payload_chunk)) {
					if (!state.lhs_condition_buffer.Scan(state.lhs_condition_scan, state.lhs_condition_chunk)) {
						throw InternalException("Optimized NLJ blocked mode: LHS payload/condition scans are unaligned");
					}
					if (state.lhs_payload_chunk.size() != state.lhs_condition_chunk.size()) {
						throw InternalException("Optimized NLJ blocked mode: LHS payload/condition row count mismatch");
					}
					state.left_tuple = 0;
					state.right_tuple = 0;

					state.rhs_block_payload_buffer.InitializeScan(state.rhs_payload_scan);
					state.rhs_block_condition_buffer.InitializeScan(state.rhs_condition_scan);
					if (!ScanAlignedRHSChunk(state.rhs_block_payload_buffer, state.rhs_block_condition_buffer,
					                         state.rhs_payload_scan, state.rhs_condition_scan, state.rhs_payload_chunk,
					                         state.rhs_condition_chunk, "rhs_block_first_after_lhs_advance")) {
						state.lhs_scan_initialized = false;
						state.rhs_scan_initialized = false;
						state.ResetRHSBlockBuffer();
						continue;
					}
					continue;
				}

				state.lhs_scan_initialized = false;
				state.rhs_scan_initialized = false;
				state.ResetRHSBlockBuffer();
				continue;
			}
			continue;
		}

		if (state.lhs_payload_buffer.Scan(state.lhs_payload_scan, state.lhs_payload_chunk)) {
			if (!state.lhs_condition_buffer.Scan(state.lhs_condition_scan, state.lhs_condition_chunk)) {
				throw InternalException("Optimized NLJ blocked mode: LHS payload/condition scans are unaligned");
			}
			if (state.lhs_payload_chunk.size() != state.lhs_condition_chunk.size()) {
				throw InternalException("Optimized NLJ blocked mode: LHS payload/condition row count mismatch");
			}
			state.left_tuple = 0;
			state.right_tuple = 0;

			state.rhs_block_payload_buffer.InitializeScan(state.rhs_payload_scan);
			state.rhs_block_condition_buffer.InitializeScan(state.rhs_condition_scan);
			if (!ScanAlignedRHSChunk(state.rhs_block_payload_buffer, state.rhs_block_condition_buffer, state.rhs_payload_scan,
			                         state.rhs_condition_scan, state.rhs_payload_chunk, state.rhs_condition_chunk,
			                         "rhs_block_first_alternative_path")) {
				state.lhs_scan_initialized = false;
				state.rhs_scan_initialized = false;
				state.ResetRHSBlockBuffer();
				continue;
			}
			continue;
		}

		state.lhs_scan_initialized = false;
		state.rhs_scan_initialized = false;
		state.ResetRHSBlockBuffer();
	}
}

//===--------------------------------------------------------------------===//
// Compute Next RHS Block
//===--------------------------------------------------------------------===//
bool PhysicalNestedLoopJoinOptimized::ComputeNextRHSBlock(ExecutionContext &context, BlockedNLJGlobalState &gstate,
                                                          BlockedNLJOperatorState &state) const {
	auto total_rhs_rows = gstate.right_payload_data.Count();
	state.ResetRHSBlockBuffer();

	if (total_rhs_rows == 0) {
		return false;
	}
	if (!state.rhs_global_scan_initialized) {
		gstate.right_payload_data.InitializeScan(state.rhs_global_payload_scan);
		gstate.right_condition_data.InitializeScan(state.rhs_global_condition_scan);
		state.rhs_global_scan_initialized = true;
		state.rhs_global_row_offset = 0;
	}
	if (state.rhs_global_row_offset >= total_rhs_rows) {
		return false;
	}

	idx_t avg_row_bytes = 64;
	if (gstate.rhs_total_bytes > 0 && total_rhs_rows > 0) {
		avg_row_bytes = MaxValue<idx_t>(idx_t(1), gstate.rhs_total_bytes / total_rhs_rows);
	}

	idx_t target_rows = 1;
	if (state.rhs_budget_bytes > 0) {
		target_rows = MaxValue<idx_t>(idx_t(1), state.rhs_budget_bytes / avg_row_bytes);
	}
	auto remaining_rows = total_rhs_rows - state.rhs_global_row_offset;
	target_rows = MinValue<idx_t>(target_rows, remaining_rows);

	state.rhs_block_global_start_row = state.rhs_global_row_offset;

	idx_t loaded_rows = 0;
	idx_t loaded_bytes = 0;
	while (loaded_rows < target_rows) {
		if (!ScanAlignedRHSChunk(gstate.right_payload_data, gstate.right_condition_data, state.rhs_global_payload_scan,
		                         state.rhs_global_condition_scan, state.rhs_payload_chunk, state.rhs_condition_chunk,
		                         "rhs_global_block_fill")) {
			break;
		}

		state.rhs_block_payload_buffer.Append(state.rhs_payload_chunk);
		state.rhs_block_condition_buffer.Append(state.rhs_condition_chunk);

		loaded_rows += state.rhs_payload_chunk.size();
		loaded_bytes += BlockedNLJOperatorState::EstimateChunkSize(state.rhs_payload_chunk) +
		                BlockedNLJOperatorState::EstimateChunkSize(state.rhs_condition_chunk);
		state.rhs_global_row_offset += state.rhs_payload_chunk.size();
	}

	state.rhs_block_bytes = loaded_bytes;
	if (state.rhs_block_payload_buffer.Count() == 0) {
		return false;
	}

	BumpTransferReadRound(context.client);
	return true;
}

//===--------------------------------------------------------------------===//
// Source Interface - for RIGHT/FULL OUTER join
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSourceState> PhysicalNestedLoopJoinOptimized::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<BlockedNLJGlobalScanState>(*this);
}

unique_ptr<LocalSourceState> PhysicalNestedLoopJoinOptimized::GetLocalSourceState(ExecutionContext &context,
                                                                                   GlobalSourceState &gstate) const {
	return make_uniq<BlockedNLJLocalScanState>(*this, gstate.Cast<BlockedNLJGlobalScanState>());
}

SourceResultType PhysicalNestedLoopJoinOptimized::GetData(ExecutionContext &context, DataChunk &chunk,
                                                          OperatorSourceInput &input) const {
	D_ASSERT(PropagatesBuildSide(join_type));
	// Check if we need to scan any unmatched tuples from the RHS for the full/right outer join
	auto &sink = sink_state->Cast<BlockedNLJGlobalState>();
	auto &gstate = input.global_state.Cast<BlockedNLJGlobalScanState>();
	auto &lstate = input.local_state.Cast<BlockedNLJLocalScanState>();

	// If the LHS is exhausted in a FULL/RIGHT OUTER JOIN, we scan chunks we still need to output
	sink.right_outer.Scan(gstate.scan_state, lstate.scan_state, chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb
