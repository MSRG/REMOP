//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator_memory_policy.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/storage_info.hpp"
#include <cmath>

namespace duckdb {

struct NLJPolicyConfig {
	idx_t memory_budget = 0;
	idx_t target_chunk_bytes = 0;
	double input_buffer_ratio = 0.75;
	//! Inner-side ratio (current optimized NLJ maps RHS to inner/short side)
	double short_table_buffer_ratio = 0.5;
	// Derived: output_buffer_ratio = 1 - input_buffer_ratio
	// Derived: outer/long_table_buffer_ratio = 1 - short_table_buffer_ratio

	double GetOutputBufferRatio() const {
		return 1.0 - input_buffer_ratio;
	}
	double GetOuterTableBufferRatio() const {
		return 1.0 - short_table_buffer_ratio;
	}
	double GetInnerTableBufferRatio() const {
		return short_table_buffer_ratio;
	}
	double GetLongTableBufferRatio() const {
		return 1.0 - short_table_buffer_ratio;
	}

	//! Get the total input buffer budget (memory_budget * input_buffer_ratio)
	idx_t GetInputBufferBudget() const {
		if (memory_budget == 0) {
			return 0;
		}
		auto input_budget = RoundUpToPage(static_cast<idx_t>(std::floor(static_cast<double>(memory_budget) * input_buffer_ratio)));
		input_budget = MinValue<idx_t>(input_budget, memory_budget);

		// Keep at least one page for output when the total budget allows it.
		if (memory_budget >= GetPageSize() && memory_budget - input_budget < GetPageSize()) {
			input_budget = memory_budget - GetPageSize();
		}
		return input_budget;
	}

	//! Get the short table (RHS) buffer budget: floor(memory_budget * input_ratio * short_ratio)
	idx_t GetShortTableBufferBudget() const {
		auto input_budget = GetInputBufferBudget();
		if (input_budget == 0) {
			return 0;
		}
		auto short_budget = RoundUpToPage(static_cast<idx_t>(std::floor(static_cast<double>(input_budget) * short_table_buffer_ratio)));
		short_budget = MinValue<idx_t>(short_budget, input_budget);

		// Keep at least one page for the long table side when the input budget allows it.
		if (input_budget >= GetPageSize()) {
			auto long_budget = (input_budget > short_budget) ? (input_budget - short_budget) : 0;
			if (long_budget < GetPageSize()) {
				short_budget = input_budget - GetPageSize();
			}
		}
		return short_budget;
	}

	//! Get the long table (LHS) buffer budget: floor(mem_budget * input_ratio) - short_buffer_budget
	idx_t GetLongTableBufferBudget() const {
		idx_t input_budget = GetInputBufferBudget();
		idx_t short_budget = GetShortTableBufferBudget();
		return (input_budget > short_budget) ? (input_budget - short_budget) : 0;
	}

	//! Get the output buffer budget (memory_budget * output_buffer_ratio)
	idx_t GetOutputBufferBudget() const {
		auto input_budget = GetInputBufferBudget();
		return (memory_budget > input_budget) ? (memory_budget - input_budget) : 0;
	}

private:
	static constexpr idx_t GetPageSize() {
		return Storage::BLOCK_ALLOC_SIZE;
	}
	static idx_t RoundUpToPage(idx_t bytes) {
		if (bytes == 0) {
			return 0;
		}
		auto page = GetPageSize();
		return ((bytes + page - 1) / page) * page;
	}
};

struct SortMergePolicyConfig {
	idx_t memory_budget = 0;
	double input_buffer_ratio = 0.5;
	// Derived: output_buffer_ratio = 1 - input_buffer_ratio

	double GetOutputBufferRatio() const {
		return 1.0 - input_buffer_ratio;
	}
};

//! Configuration for k-way merge sort
//! K can be any integer >= 2 (does NOT need to be power of 2)
struct KWayMergePolicyConfig {
	idx_t memory_budget = 0;           //! Total memory available for merge
	idx_t k = 4;                        //! Number of runs to merge at once (default 4)
	idx_t input_buffer_size = 0;        //! Per-run input buffer size in bytes (0 = auto-compute)
	idx_t output_buffer_size = 0;       //! Output buffer size in bytes (0 = auto-compute)
	double input_buffer_ratio = 0.67;   //! Fraction of memory for input buffers (default 67%)
	bool adaptive_k = true;             //! Auto-adjust k based on available memory
	idx_t min_buffer_rows = 2048;       //! Minimum rows per input buffer for efficiency

	//! Get output buffer ratio (1 - input_buffer_ratio)
	double GetOutputBufferRatio() const {
		return 1.0 - input_buffer_ratio;
	}

	//! Get total input buffer budget
	idx_t GetTotalInputBufferBudget() const {
		if (memory_budget == 0) {
			return 0;
		}
		auto input_budget = RoundUpToPage(static_cast<idx_t>(std::floor(static_cast<double>(memory_budget) * input_buffer_ratio)));
		input_budget = MinValue<idx_t>(input_budget, memory_budget);

		// Keep at least one page for output when the total budget allows it.
		if (memory_budget >= GetPageSize() && memory_budget - input_budget < GetPageSize()) {
			input_budget = memory_budget - GetPageSize();
		}
		return input_budget;
	}

	//! Get output buffer budget
	idx_t GetOutputBufferBudget() const {
		auto input_budget = GetTotalInputBufferBudget();
		return (memory_budget > input_budget) ? (memory_budget - input_budget) : 0;
	}

	//! Get per-run input buffer size (auto-computed if input_buffer_size == 0)
	idx_t GetPerRunInputBuffer(idx_t actual_k) const {
		if (input_buffer_size > 0) {
			return input_buffer_size;
		}
		if (actual_k == 0) {
			return 0;
		}
		return GetTotalInputBufferBudget() / actual_k;
	}

	//! Compute optimal k based on memory budget, number of runs, and row width
	//! Returns a value between 2 and min(num_runs, max_k)
	idx_t ComputeOptimalK(idx_t num_runs, idx_t row_width) const {
		if (num_runs <= 1) {
			return 2; // Minimum k
		}
		if (!adaptive_k) {
			// Use configured k, but cap at num_runs
			return MinValue(k, num_runs);
		}

		// Minimum buffer size = min_buffer_rows * row_width
		idx_t min_buffer_size = min_buffer_rows * row_width;
		if (min_buffer_size == 0) {
			min_buffer_size = 256 * 1024; // Default 256KB minimum
		}

		// Calculate maximum k that fits in memory
		// Memory = k * input_buffer + output_buffer
		idx_t input_budget = GetTotalInputBufferBudget();
		idx_t output_budget = GetOutputBufferBudget();

		// Ensure output buffer is at least min_buffer_size
		if (output_budget < min_buffer_size && memory_budget > min_buffer_size) {
			output_budget = min_buffer_size;
			input_budget = memory_budget - output_budget;
		}

		// Maximum k = input_budget / min_buffer_size
		idx_t max_k = (min_buffer_size > 0) ? (input_budget / min_buffer_size) : 64;
		if (max_k < 2) {
			max_k = 2; // Minimum k is 2 (degrades to 2-way merge)
		}

		// Optimal k = min(configured_k, max_k, num_runs)
		// Also cap at reasonable limit to avoid excessive comparison overhead
		idx_t optimal_k = MinValue(k, MinValue(max_k, MinValue(num_runs, (idx_t)64)));
		return MaxValue(optimal_k, (idx_t)2);
	}

	//! Check if we need to use partitioned merge (when data doesn't fit in memory)
	bool NeedsPartitionedMerge(idx_t total_data_size) const {
		return total_data_size > memory_budget;
	}

private:
	static constexpr idx_t GetPageSize() {
		return Storage::BLOCK_ALLOC_SIZE;
	}
	static idx_t RoundUpToPage(idx_t bytes) {
		if (bytes == 0) {
			return 0;
		}
		auto page = GetPageSize();
		return ((bytes + page - 1) / page) * page;
	}
};

class OperatorMemoryPolicy {
public:
	virtual ~OperatorMemoryPolicy() = default;

	virtual NLJPolicyConfig GetNestedLoopJoinConfig() const {
		return NLJPolicyConfig();
	}
	virtual NLJPolicyConfig GetBlockwiseNLJConfig() const {
		return NLJPolicyConfig();
	}
	virtual SortMergePolicyConfig GetSortMergeConfig() const {
		return SortMergePolicyConfig();
	}
	virtual KWayMergePolicyConfig GetKWayMergeConfig() const {
		return KWayMergePolicyConfig();
	}
};

class DefaultOperatorMemoryPolicy final : public OperatorMemoryPolicy {
public:
	DefaultOperatorMemoryPolicy(idx_t memory_budget_p = 0, double sort_input_buffer_ratio_p = 0.5,
	                            double nlj_input_buffer_ratio_p = 0.75, double nlj_short_table_buffer_ratio_p = 0.5,
	                            idx_t kway_k_p = 4, bool kway_adaptive_p = true,
	                            double kway_input_buffer_ratio_p = 0.67)
	    : nlj_config(InitNLJConfig(memory_budget_p, nlj_input_buffer_ratio_p, nlj_short_table_buffer_ratio_p)),
	      sort_config(InitSortConfig(memory_budget_p, sort_input_buffer_ratio_p)),
	      kway_config(InitKWayConfig(memory_budget_p, kway_k_p, kway_adaptive_p, kway_input_buffer_ratio_p)) {
	}

	NLJPolicyConfig GetNestedLoopJoinConfig() const override {
		return nlj_config;
	}

	NLJPolicyConfig GetBlockwiseNLJConfig() const override {
		return nlj_config;
	}

	SortMergePolicyConfig GetSortMergeConfig() const override {
		return sort_config;
	}

	KWayMergePolicyConfig GetKWayMergeConfig() const override {
		return kway_config;
	}

private:
	static double ClampRatio(double ratio) {
		if (ratio < 0.0) {
			return 0.0;
		}
		if (ratio > 1.0) {
			return 1.0;
		}
		return ratio;
	}

	static NLJPolicyConfig InitNLJConfig(idx_t memory_budget, double input_ratio, double short_ratio) {
		NLJPolicyConfig config;
		config.memory_budget = memory_budget;
		config.input_buffer_ratio = ClampRatio(input_ratio);
		config.short_table_buffer_ratio = ClampRatio(short_ratio);
		return config;
	}

	static SortMergePolicyConfig InitSortConfig(idx_t memory_budget, double input_ratio) {
		SortMergePolicyConfig config;
		config.memory_budget = memory_budget;
		config.input_buffer_ratio = ClampRatio(input_ratio);
		return config;
	}

	static KWayMergePolicyConfig InitKWayConfig(idx_t memory_budget, idx_t k, bool adaptive_k, double input_ratio) {
		KWayMergePolicyConfig config;
		config.memory_budget = memory_budget;
		config.k = MaxValue(k, (idx_t)2);
		config.adaptive_k = adaptive_k;
		config.input_buffer_ratio = ClampRatio(input_ratio);
		return config;
	}

private:
	NLJPolicyConfig nlj_config;
	SortMergePolicyConfig sort_config;
	KWayMergePolicyConfig kway_config;
};

} // namespace duckdb
