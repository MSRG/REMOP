#include "duckdb/execution/operator/order/physical_order_optimized.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/sort/kway_merge_sorter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include <sstream>

namespace duckdb {

//===--------------------------------------------------------------------===//
// OrderOptimizedGlobalSinkState
//===--------------------------------------------------------------------===//
OrderOptimizedGlobalSinkState::OrderOptimizedGlobalSinkState(BufferManager &buffer_manager, const PhysicalOrder &order,
                                                             RowLayout &payload_layout,
                                                             shared_ptr<OperatorMemoryPolicy> policy_p)
    : global_sort_state(buffer_manager, order.orders, payload_layout), memory_per_thread(0), policy(std::move(policy_p)),
      merge_mode(OrderMergeMode::K_WAY), use_kway_merge(true), kway_round_group_start(0), kway_round_k(2),
      kway_round_initialized(false) {
	// Get k-way merge configuration from policy
	if (policy) {
		kway_config = policy->GetKWayMergeConfig();
	}
}

//===--------------------------------------------------------------------===//
// Local sink state for optimized order (same as base class but using our global state type)
//===--------------------------------------------------------------------===//
class OrderOptimizedLocalSinkState : public LocalSinkState {
public:
	OrderOptimizedLocalSinkState(ClientContext &context, const PhysicalOrder &op) : key_executor(context) {
		// Initialize order clause expression executor and DataChunk
		vector<LogicalType> key_types;
		for (auto &order : op.orders) {
			key_types.push_back(order.expression->return_type);
			key_executor.AddExpression(*order.expression);
		}
		auto &allocator = Allocator::Get(context);
		keys.Initialize(allocator, key_types);
		payload.Initialize(allocator, op.types);
	}

public:
	//! The local sort state
	LocalSortState local_sort_state;
	//! Key expression executor, and chunk to hold the vectors
	ExpressionExecutor key_executor;
	DataChunk keys;
	//! Payload chunk to hold the vectors
	DataChunk payload;
};

class OrderOptimizedGlobalSourceState : public GlobalSourceState {
public:
	explicit OrderOptimizedGlobalSourceState(OrderOptimizedGlobalSinkState &sink) : next_batch_index(0) {
		auto &global_sort_state = sink.global_sort_state;
		if (global_sort_state.sorted_blocks.empty()) {
			total_batches = 0;
		} else {
			D_ASSERT(global_sort_state.sorted_blocks.size() == 1);
			total_batches = global_sort_state.sorted_blocks[0]->payload_data->data_blocks.size();
		}
	}

	idx_t MaxThreads() override {
		return total_batches;
	}

public:
	atomic<idx_t> next_batch_index;
	idx_t total_batches;
};

class OrderOptimizedLocalSourceState : public LocalSourceState {
public:
	explicit OrderOptimizedLocalSourceState(OrderOptimizedGlobalSourceState &gstate) : batch_index(gstate.next_batch_index++) {
	}

public:
	idx_t batch_index;
	unique_ptr<PayloadScanner> scanner;
};

//===--------------------------------------------------------------------===//
// PhysicalOrderOptimized
//===--------------------------------------------------------------------===//
PhysicalOrderOptimized::PhysicalOrderOptimized(vector<LogicalType> types, vector<BoundOrderByNode> orders,
                                               vector<idx_t> projections, idx_t estimated_cardinality,
                                               shared_ptr<OperatorMemoryPolicy> policy_p)
    : PhysicalOrder(std::move(types), std::move(orders), std::move(projections), estimated_cardinality),
      policy(std::move(policy_p)) {
	type = PhysicalOperatorType::ORDER_BY_OPTIMIZED;
}

unique_ptr<GlobalSourceState> PhysicalOrderOptimized::GetGlobalSourceState(ClientContext &context) const {
	auto &sink = this->sink_state->Cast<OrderOptimizedGlobalSinkState>();
	return make_uniq<OrderOptimizedGlobalSourceState>(sink);
}

unique_ptr<LocalSourceState> PhysicalOrderOptimized::GetLocalSourceState(ExecutionContext &context,
                                                                          GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<OrderOptimizedGlobalSourceState>();
	return make_uniq<OrderOptimizedLocalSourceState>(gstate);
}

SourceResultType PhysicalOrderOptimized::GetData(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<OrderOptimizedGlobalSourceState>();
	auto &lstate = input.local_state.Cast<OrderOptimizedLocalSourceState>();

	if (lstate.scanner && lstate.scanner->Remaining() == 0) {
		lstate.batch_index = gstate.next_batch_index++;
		lstate.scanner = nullptr;
	}

	if (lstate.batch_index >= gstate.total_batches) {
		return SourceResultType::FINISHED;
	}

	if (!lstate.scanner) {
		auto &sink = this->sink_state->Cast<OrderOptimizedGlobalSinkState>();
		auto &global_sort_state = sink.global_sort_state;
		lstate.scanner = make_uniq<PayloadScanner>(global_sort_state, lstate.batch_index, true);
	}

	lstate.scanner->Scan(chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

idx_t PhysicalOrderOptimized::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                            LocalSourceState &lstate_p) const {
	auto &lstate = lstate_p.Cast<OrderOptimizedLocalSourceState>();
	return lstate.batch_index;
}

unique_ptr<GlobalSinkState> PhysicalOrderOptimized::GetGlobalSinkState(ClientContext &context) const {
	// Get the payload layout from the return types
	RowLayout payload_layout;
	payload_layout.Initialize(types);

	auto state = make_uniq<OrderOptimizedGlobalSinkState>(BufferManager::GetBufferManager(context), *this,
	                                                      payload_layout, policy);

	// Set external flag (can be forced with PRAGMA)
	state->global_sort_state.external = ClientConfig::GetConfig(context).force_external;
	state->memory_per_thread = GetMaxThreadMemory(context);

	// Configure k-way merge based on memory budget
	if (state->policy) {
		state->kway_config = state->policy->GetKWayMergeConfig();
		// Set memory budget if not already set
		if (state->kway_config.memory_budget == 0) {
			state->kway_config.memory_budget = state->memory_per_thread * 4; // Use reasonable default
		}
	}

	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalOrderOptimized::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<OrderOptimizedLocalSinkState>(context.client, *this);
}

SinkResultType PhysicalOrderOptimized::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<OrderOptimizedGlobalSinkState>();
	auto &lstate = input.local_state.Cast<OrderOptimizedLocalSinkState>();

	auto &global_sort_state = gstate.global_sort_state;
	auto &local_sort_state = lstate.local_sort_state;

	// Initialize local state (if necessary)
	if (!local_sort_state.initialized) {
		local_sort_state.Initialize(global_sort_state, BufferManager::GetBufferManager(context.client));
	}

	// Obtain sorting columns
	auto &keys = lstate.keys;
	keys.Reset();
	lstate.key_executor.Execute(chunk, keys);

	auto &payload = lstate.payload;
	payload.ReferenceColumns(chunk, projections);

	// Sink the data into the local sort state
	keys.Verify();
	chunk.Verify();
	local_sort_state.SinkChunk(keys, payload);

	// When sorting data reaches a certain size, we sort it
	if (local_sort_state.SizeInBytes() >= gstate.memory_per_thread) {
		local_sort_state.Sort(global_sort_state, true);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalOrderOptimized::Combine(ExecutionContext &context,
                                                      OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<OrderOptimizedGlobalSinkState>();
	auto &lstate = input.local_state.Cast<OrderOptimizedLocalSinkState>();
	gstate.global_sort_state.AddLocalState(lstate.local_sort_state);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// K-Way Merge Task
//===--------------------------------------------------------------------===//
class PhysicalOrderKWayMergeTask : public ExecutorTask {
public:
	PhysicalOrderKWayMergeTask(shared_ptr<Event> event_p, ClientContext &context_p, OrderOptimizedGlobalSinkState &state)
	    : ExecutorTask(context_p, std::move(event_p)), state(state) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		D_ASSERT(state.kway_sorter);
		KWayPartition partition;
		while (state.kway_sorter->GetPartitioner()->GetNextPartition(partition)) {
			unique_ptr<SortedBlock> result_block;
			state.kway_sorter->MergePartition(partition, result_block);
			if (result_block) {
				state.kway_sorter->AddResultBlock(std::move(result_block), partition.partition_idx);
			}
		}

		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	OrderOptimizedGlobalSinkState &state;
};

//===--------------------------------------------------------------------===//
// 2-Way Merge Task (for fallback)
//===--------------------------------------------------------------------===//
class PhysicalOrder2WayMergeTask : public ExecutorTask {
public:
	PhysicalOrder2WayMergeTask(shared_ptr<Event> event_p, ClientContext &context, OrderOptimizedGlobalSinkState &state)
	    : ExecutorTask(context, std::move(event_p)), context(context), state(state) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		auto &global_sort_state = state.global_sort_state;
		auto &buffer_manager = BufferManager::GetBufferManager(context);

		// Use standard merge sorter (2-way)
		MergeSorter merge_sorter(global_sort_state, buffer_manager);
		merge_sorter.PerformInMergeRound();

		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	ClientContext &context;
	OrderOptimizedGlobalSinkState &state;
};

//===--------------------------------------------------------------------===//
// K-Way Merge Event
//===--------------------------------------------------------------------===//
class OrderKWayMergeEvent : public BasePipelineEvent {
public:
	OrderKWayMergeEvent(OrderOptimizedGlobalSinkState &gstate_p, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), gstate(gstate_p) {
	}

	OrderOptimizedGlobalSinkState &gstate;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();
		auto &global_sort_state = gstate.global_sort_state;
		auto &buffer_manager = BufferManager::GetBufferManager(context);

		if (!gstate.kway_round_initialized) {
			gstate.kway_round_input_runs.clear();
			gstate.kway_round_output_runs.clear();
			gstate.kway_round_input_runs = std::move(global_sort_state.sorted_blocks);
			global_sort_state.sorted_blocks.clear();
			gstate.kway_round_group_start = 0;

			const idx_t num_runs = gstate.kway_round_input_runs.size();
			const idx_t row_width = global_sort_state.payload_layout.GetRowWidth();
			gstate.kway_round_k = gstate.kway_config.ComputeOptimalK(num_runs, row_width);
			gstate.kway_round_initialized = true;
#ifndef NDEBUG
			Printer::Print("[KWAY_DEBUG] Round start: num_runs=" + std::to_string(num_runs) +
			               " round_k=" + std::to_string(gstate.kway_round_k));
			#endif
		}

		const idx_t num_runs = gstate.kway_round_input_runs.size();
		const idx_t group_start = gstate.kway_round_group_start;
		const idx_t group_end = MinValue(group_start + gstate.kway_round_k, num_runs);
		D_ASSERT(group_start < group_end);

		vector<SortedBlock *> merge_runs;
		merge_runs.reserve(group_end - group_start);
		for (idx_t i = group_start; i < group_end; i++) {
			merge_runs.push_back(gstate.kway_round_input_runs[i].get());
		}

		gstate.kway_sorter = make_uniq<KWayMergeSorter>(global_sort_state, buffer_manager, gstate.kway_config);

		// Partition size is bounded by per-thread input-buffer budget (in rows).
		// This keeps each merge partition aligned with policy-managed input memory.
		idx_t partition_size = global_sort_state.block_capacity;
		const idx_t group_k = merge_runs.size();
		if (group_k > 0) {
			const idx_t per_run_input_budget = gstate.kway_config.GetPerRunInputBuffer(group_k);
			idx_t bytes_per_row = global_sort_state.sort_layout.entry_size + global_sort_state.payload_layout.GetRowWidth();
			if (!global_sort_state.sort_layout.all_constant) {
				bytes_per_row += global_sort_state.sort_layout.blob_layout.GetRowWidth();
			}
			if (bytes_per_row > 0 && per_run_input_budget > 0) {
				const idx_t rows_per_run = MaxValue<idx_t>(1, per_run_input_budget / bytes_per_row);
				const idx_t budget_partition_rows = MaxValue<idx_t>(1, rows_per_run * group_k);
				partition_size = MinValue(partition_size, budget_partition_rows);
			}
		}
		partition_size = MaxValue<idx_t>(partition_size, 1);
		gstate.kway_sorter->InitializePartitioner(merge_runs, partition_size);
	#ifndef NDEBUG
		{
			std::ostringstream all_runs_ss;
			all_runs_ss << "[";
			for (idx_t i = 0; i < num_runs; i++) {
				if (i > 0) {
					all_runs_ss << ",";
				}
				all_runs_ss << gstate.kway_round_input_runs[i]->Count();
			}
			all_runs_ss << "]";
			Printer::Print("[KWAY_DEBUG] Schedule begin: num_runs=" + std::to_string(num_runs) +
			               " round_k=" + std::to_string(gstate.kway_round_k) + " group_run_range=[" +
			               std::to_string(group_start) + "," + std::to_string(group_end) +
			               ") all_run_counts=" + all_runs_ss.str());
		}
	#endif

		// Schedule tasks for this group based on partition count and available threads.
		const idx_t num_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
		const idx_t total_partitions = gstate.kway_sorter->GetPartitioner()->NumPartitions();
		idx_t tasks = MaxValue<idx_t>(1, MinValue(num_threads, MaxValue<idx_t>(1, total_partitions)));
	#ifndef NDEBUG
		Printer::Print("[KWAY_DEBUG] Schedule tasks: total_partitions=" + std::to_string(total_partitions) +
		               " num_threads=" + std::to_string(num_threads) + " tasks=" + std::to_string(tasks));
	#endif

		vector<shared_ptr<Task>> merge_tasks;
		for (idx_t i = 0; i < tasks; i++) {
			merge_tasks.push_back(make_uniq<PhysicalOrderKWayMergeTask>(shared_from_this(), context, gstate));
		}
		SetTasks(std::move(merge_tasks));
	}

	void FinishEvent() override {
		auto &global_sort_state = gstate.global_sort_state;
		auto &context = pipeline->GetClientContext();
		auto &buffer_manager = BufferManager::GetBufferManager(context);

		D_ASSERT(gstate.kway_sorter);
		gstate.kway_sorter->FinalizeParallelMerge();

		vector<unique_ptr<SortedBlock>> group_fragments;
		group_fragments = std::move(global_sort_state.sorted_blocks);
		global_sort_state.sorted_blocks.clear();
		D_ASSERT(!group_fragments.empty());

		auto group_run = make_uniq<SortedBlock>(buffer_manager, global_sort_state);
		group_run->AppendSortedBlocks(group_fragments);
		gstate.kway_round_output_runs.push_back(std::move(group_run));
		gstate.kway_sorter.reset();

		const idx_t num_runs = gstate.kway_round_input_runs.size();
		gstate.kway_round_group_start = MinValue(gstate.kway_round_group_start + gstate.kway_round_k, num_runs);

		if (gstate.kway_round_group_start < num_runs) {
			PhysicalOrderOptimized::ScheduleKWayMergeTasks(*pipeline, *this, gstate);
			return;
		}

		// End of round: swap round outputs into global sorted blocks.
		gstate.kway_round_input_runs.clear();
		global_sort_state.sorted_blocks = std::move(gstate.kway_round_output_runs);
		gstate.kway_round_output_runs.clear();
		gstate.kway_round_group_start = 0;
		gstate.kway_round_initialized = false;

#ifndef NDEBUG
		Printer::Print("[KWAY_DEBUG] Round complete: new_num_runs=" +
		               std::to_string(global_sort_state.sorted_blocks.size()));
#endif

		// Continue with next k-way round until a single run remains.
		if (global_sort_state.sorted_blocks.size() > 1) {
			PhysicalOrderOptimized::ScheduleKWayMergeTasks(*pipeline, *this, gstate);
		}
	}
};

//===--------------------------------------------------------------------===//
// 2-Way Merge Event (for fallback)
//===--------------------------------------------------------------------===//
class Order2WayMergeEvent : public BasePipelineEvent {
public:
	Order2WayMergeEvent(OrderOptimizedGlobalSinkState &gstate_p, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), gstate(gstate_p) {
	}

	OrderOptimizedGlobalSinkState &gstate;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();
		auto &global_sort_state = gstate.global_sort_state;

		// Assign merge tasks based on number of pairs to be merged
		idx_t num_pairs = global_sort_state.sorted_blocks.size() / 2;
		idx_t num_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
		idx_t tasks = MaxValue<idx_t>(1, MinValue<idx_t>(num_threads, num_pairs));

		vector<shared_ptr<Task>> merge_tasks;
		for (idx_t i = 0; i < tasks; i++) {
			merge_tasks.push_back(make_uniq<PhysicalOrder2WayMergeTask>(shared_from_this(), context, gstate));
		}
		SetTasks(std::move(merge_tasks));
	}

	void FinishEvent() override {
		auto &global_sort_state = gstate.global_sort_state;

		// Finish the parallel merge round
		global_sort_state.CompleteMergeRound();

		// Check if more merge rounds are needed
		if (global_sort_state.sorted_blocks.size() > 1) {
			// Multiple blocks remaining: Schedule the next round
			PhysicalOrderOptimized::Schedule2WayMergeTasks(*pipeline, *this, gstate);
		}
	}
};

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalOrderOptimized::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                  OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<OrderOptimizedGlobalSinkState>();
	auto &global_sort_state = state.global_sort_state;

	if (global_sort_state.sorted_blocks.empty()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Prepare for merge sort phase (determines external sort, sets up block_capacity)
	global_sort_state.PrepareMergePhase();
	state.kway_round_initialized = false;
	state.kway_round_group_start = 0;
	state.kway_round_input_runs.clear();
	state.kway_round_output_runs.clear();
	state.kway_sorter.reset();

	// Decide merge mode based on configuration and data characteristics
	idx_t num_runs = global_sort_state.sorted_blocks.size();
#ifndef NDEBUG
	Printer::Print("[KWAY_DEBUG] Finalize: initial_num_runs=" + std::to_string(num_runs) +
	               " use_kway_merge=" + std::to_string(state.use_kway_merge ? 1 : 0));
#endif
	if (num_runs <= 1) {
		// No merge needed
		return SinkFinalizeType::READY;
	}

	// K-way merge is supported for both fixed and varlen layouts.
	const bool kway_supported = true;

	// Check if k-way merge should be used.
	// Use k-way if: explicitly enabled AND supported by data format AND more than 2 runs.
	if (state.use_kway_merge && kway_supported && num_runs > 2) {
		// Update k-way config with actual data characteristics
		state.kway_config.k = state.kway_config.ComputeOptimalK(num_runs, state.global_sort_state.payload_layout.GetRowWidth());
#ifndef NDEBUG
		Printer::Print("[KWAY_DEBUG] Finalize select path: K_WAY configured_k=" + std::to_string(state.kway_config.k) +
		               " budget=" + std::to_string(state.kway_config.memory_budget));
#endif

		// Schedule k-way merge
		ScheduleKWayMergeTasks(pipeline, event, state);
	} else {
		// Fall back to standard 2-way merge
		state.merge_mode = OrderMergeMode::TWO_WAY;
#ifndef NDEBUG
		std::string reason = "default";
		if (!state.use_kway_merge) {
			reason = "disabled";
		} else if (num_runs <= 2) {
			reason = "num_runs<=2";
		}
		Printer::Print("[KWAY_DEBUG] Finalize select path: TWO_WAY reason=" + reason);
#endif
		Schedule2WayMergeTasks(pipeline, event, state);
	}

	return SinkFinalizeType::READY;
}

void PhysicalOrderOptimized::ScheduleKWayMergeTasks(Pipeline &pipeline, Event &event,
                                                    OrderOptimizedGlobalSinkState &state) {
	auto new_event = make_shared_ptr<OrderKWayMergeEvent>(state, pipeline);
	event.InsertEvent(std::move(new_event));
}

void PhysicalOrderOptimized::Schedule2WayMergeTasks(Pipeline &pipeline, Event &event,
                                                    OrderOptimizedGlobalSinkState &state) {
	// MergeSorter expects per-round state (pair_idx/num_pairs/sorted_blocks_temp) to be initialized.
	state.global_sort_state.InitializeMergeRound();
	auto new_event = make_shared_ptr<Order2WayMergeEvent>(state, pipeline);
	event.InsertEvent(std::move(new_event));
}

} // namespace duckdb
