#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/order/physical_order_optimized.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator_memory_policy.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOrder &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);
	if (!op.orders.empty()) {
		vector<idx_t> projections;
		if (op.projections.empty()) {
			for (idx_t i = 0; i < plan->types.size(); i++) {
				projections.push_back(i);
			}
		} else {
			projections = std::move(op.projections);
		}
		unique_ptr<PhysicalOperator> order_op;
		auto &db_config = DBConfig::GetConfig(context);
		if (db_config.options.enable_operator_memory_optimizations) {
			auto &opts = db_config.options;
			idx_t operator_budget = opts.operator_memory_budget;
			if (operator_budget == 0) {
				operator_budget = BufferManager::GetBufferManager(context).GetQueryMaxMemory();
			}
			auto policy = make_shared_ptr<DefaultOperatorMemoryPolicy>(
			    operator_budget, opts.operator_sort_input_buffer_ratio, opts.operator_nlj_input_buffer_ratio,
			    opts.operator_nlj_short_table_buffer_ratio, opts.operator_kway_merge_k,
			    opts.operator_kway_merge_adaptive, opts.operator_kway_merge_input_buffer_ratio);
			order_op = make_uniq<PhysicalOrderOptimized>(op.types, std::move(op.orders), std::move(projections),
			                                             op.estimated_cardinality, std::move(policy));
		} else {
			order_op = make_uniq<PhysicalOrder>(op.types, std::move(op.orders), std::move(projections),
			                                    op.estimated_cardinality);
		}
		order_op->children.push_back(std::move(plan));
		plan = std::move(order_op);
	}
	return plan;
}

} // namespace duckdb
