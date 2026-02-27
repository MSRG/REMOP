//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_blockwise_nl_join_optimized.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/join/physical_blockwise_nl_join.hpp"
#include "duckdb/execution/operator_memory_policy.hpp"

namespace duckdb {

class BlockedBlockwiseNLJGlobalState;
class BlockedBlockwiseNLJOperatorState;
class BlockedBlockwiseNLJGlobalScanState;
class BlockedBlockwiseNLJLocalScanState;

//! PhysicalBlockwiseNLJoinOptimized is an optimized blockwise nested loop join that supports
//! blocked execution when both tables exceed available memory. It buffers LHS chunks
//! and iterates through RHS in memory-bounded blocks, evaluating the arbitrary expression
//! on cross-product batches.
class PhysicalBlockwiseNLJoinOptimized : public PhysicalBlockwiseNLJoin {
public:
	PhysicalBlockwiseNLJoinOptimized(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
	                                 unique_ptr<PhysicalOperator> right, unique_ptr<Expression> condition,
	                                 JoinType join_type, idx_t estimated_cardinality,
	                                 shared_ptr<OperatorMemoryPolicy> policy);

public:
	//! Sink Interface - overrides to track RHS size
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	//! Operator Interface - overrides for blocked execution
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

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
	//! Execute using regular blockwise NLJ logic (when RHS fits in memory)
	OperatorResultType ExecuteRegularBlockwiseNLJ(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                              BlockedBlockwiseNLJGlobalState &gstate,
	                                              BlockedBlockwiseNLJOperatorState &state) const;

	//! Execute using blocked logic (when RHS exceeds memory)
	OperatorResultType ExecuteBlockedBlockwiseNLJ(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                              BlockedBlockwiseNLJGlobalState &gstate,
	                                              BlockedBlockwiseNLJOperatorState &state) const;

	//! Process the blocked join - compare buffered LHS against RHS blocks
	OperatorResultType ProcessBlockedBlockwiseJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                               BlockedBlockwiseNLJGlobalState &gstate,
	                                               BlockedBlockwiseNLJOperatorState &state) const;

	//! Compute the next RHS block boundaries based on budget
	bool ComputeNextRHSBlock(ExecutionContext &context, BlockedBlockwiseNLJGlobalState &gstate,
	                         BlockedBlockwiseNLJOperatorState &state) const;

	//! Helper to estimate chunk size in bytes
	static idx_t EstimateChunkSize(DataChunk &chunk);

private:
	shared_ptr<OperatorMemoryPolicy> policy;
};

} // namespace duckdb
