//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_nested_loop_join_optimized.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/execution/operator_memory_policy.hpp"

namespace duckdb {

class BlockedNLJGlobalState;
class BlockedNLJOperatorState;
class BlockedNLJGlobalScanState;
class BlockedNLJLocalScanState;

//! PhysicalNestedLoopJoinOptimized is an optimized nested loop join that supports
//! blocked execution when both tables exceed available memory. It buffers LHS chunks
//! and iterates through RHS in memory-bounded blocks.
class PhysicalNestedLoopJoinOptimized : public PhysicalNestedLoopJoin {
public:
	PhysicalNestedLoopJoinOptimized(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
	                               unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond,
	                               JoinType join_type, idx_t estimated_cardinality,
	                               shared_ptr<OperatorMemoryPolicy> policy);

public:
	//! Sink Interface - overrides to track RHS size
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	//! Operator Interface - overrides for blocked execution
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorFinalizeResultType FinalExecute(ExecutionContext &context, DataChunk &chunk, GlobalOperatorState &gstate,
	                                        OperatorState &state) const override;
	bool RequiresFinalExecute() const override;

	//! Source Interface - overrides for RIGHT/FULL OUTER join
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk,
	                         OperatorSourceInput &input) const override;

protected:
	//! CachingOperator Interface - implements blocked execution
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;

private:
	//! Execute using regular NLJ logic (when RHS fits in memory)
	OperatorResultType ExecuteRegularNLJ(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                     BlockedNLJGlobalState &gstate, BlockedNLJOperatorState &state) const;

	//! Execute simple joins (SEMI, ANTI, MARK)
	OperatorResultType ExecuteSimpleJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                     BlockedNLJGlobalState &gstate, BlockedNLJOperatorState &state) const;

	//! Execute using blocked logic (when RHS exceeds memory)
	OperatorResultType ExecuteBlockedNLJ(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                     BlockedNLJGlobalState &gstate, BlockedNLJOperatorState &state) const;

	//! Process the blocked join - compare buffered LHS against RHS blocks
	OperatorResultType ProcessBlockedJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                      BlockedNLJGlobalState &gstate, BlockedNLJOperatorState &state) const;

	//! Compute the next RHS block boundaries based on budget
	bool ComputeNextRHSBlock(ExecutionContext &context, BlockedNLJGlobalState &gstate, BlockedNLJOperatorState &state) const;

	//! Helper to estimate chunk size in bytes
	static idx_t EstimateChunkSize(DataChunk &chunk);

private:
	shared_ptr<OperatorMemoryPolicy> policy;
};

} // namespace duckdb
