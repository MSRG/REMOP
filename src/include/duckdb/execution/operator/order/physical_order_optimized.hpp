//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/order/physical_order_optimized.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator_memory_policy.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/sort/kway_merge_sorter.hpp"

namespace duckdb {

//! Merge mode for the optimized order operator
enum class OrderMergeMode {
	TWO_WAY,    //! Use standard 2-way merge (current DuckDB behavior)
	K_WAY       //! Use k-way merge with configurable k
};

//! Global sink state for optimized order operator
//! This is a standalone class (not inheriting from OrderGlobalSinkState which is internal)
class OrderOptimizedGlobalSinkState : public GlobalSinkState {
public:
	OrderOptimizedGlobalSinkState(BufferManager &buffer_manager, const PhysicalOrder &order,
	                              RowLayout &payload_layout, shared_ptr<OperatorMemoryPolicy> policy_p);

	//! Global sort state
	GlobalSortState global_sort_state;

	//! Memory usage per thread
	idx_t memory_per_thread;

	//! The memory policy
	shared_ptr<OperatorMemoryPolicy> policy;

	//! Merge mode (2-way or k-way)
	OrderMergeMode merge_mode;

	//! K-way merge configuration
	KWayMergePolicyConfig kway_config;

	//! Whether to use k-way merge (can be set via PRAGMA)
	bool use_kway_merge;

	//! Shared k-way merge sorter for parallel merge tasks
	unique_ptr<KWayMergeSorter> kway_sorter;

	//! Round-level state for k-way merge scheduling across groups.
	vector<unique_ptr<SortedBlock>> kway_round_input_runs;
	vector<unique_ptr<SortedBlock>> kway_round_output_runs;
	idx_t kway_round_group_start;
	idx_t kway_round_k;
	bool kway_round_initialized;
};

class PhysicalOrderOptimized : public PhysicalOrder {
public:
	PhysicalOrderOptimized(vector<LogicalType> types, vector<BoundOrderByNode> orders,
	                       vector<idx_t> projections, idx_t estimated_cardinality,
	                       shared_ptr<OperatorMemoryPolicy> policy);

	//! Source interface (must use optimized sink state type)
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;
	idx_t GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	                    LocalSourceState &lstate) const override;

	//! Override to provide extended global sink state
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	//! Override to provide our own local sink state
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	//! Override Sink to use our own state
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;

	//! Override Combine to use our own state
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;

	//! Override Finalize to use k-way merge when appropriate
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	//! Schedule k-way merge tasks
	static void ScheduleKWayMergeTasks(Pipeline &pipeline, Event &event, OrderOptimizedGlobalSinkState &state);

	//! Schedule standard 2-way merge tasks (for fallback)
	static void Schedule2WayMergeTasks(Pipeline &pipeline, Event &event, OrderOptimizedGlobalSinkState &state);

private:
	shared_ptr<OperatorMemoryPolicy> policy;
};

} // namespace duckdb
