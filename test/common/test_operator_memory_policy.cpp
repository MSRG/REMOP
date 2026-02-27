#include "catch.hpp"
#include "duckdb/execution/operator_memory_policy.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test NLJPolicyConfig budget calculations", "[operator_memory_policy]") {
	
	SECTION("Default config has zero budgets") {
		NLJPolicyConfig config;
		REQUIRE(config.memory_budget == 0);
		REQUIRE(config.input_buffer_ratio == 0.5);
		REQUIRE(config.short_table_buffer_ratio == 0.5);
		REQUIRE(config.GetInputBufferBudget() == 0);
		REQUIRE(config.GetShortTableBufferBudget() == 0);
		REQUIRE(config.GetLongTableBufferBudget() == 0);
	}
	
	SECTION("Basic budget calculation with 1GB memory") {
		NLJPolicyConfig config;
		config.memory_budget = 1024 * 1024 * 1024; // 1GB
		config.input_buffer_ratio = 0.5;
		config.short_table_buffer_ratio = 0.5;
		
		// Input buffer = 1GB * 0.5 = 512MB
		idx_t expected_input = 512 * 1024 * 1024;
		REQUIRE(config.GetInputBufferBudget() == expected_input);
		
		// Short table (RHS) = 512MB * 0.5 = 256MB
		idx_t expected_short = 256 * 1024 * 1024;
		REQUIRE(config.GetShortTableBufferBudget() == expected_short);
		
		// Long table (LHS) = 512MB - 256MB = 256MB
		idx_t expected_long = expected_input - expected_short;
		REQUIRE(config.GetLongTableBufferBudget() == expected_long);
	}
	
	SECTION("Asymmetric ratios") {
		NLJPolicyConfig config;
		config.memory_budget = 1000000; // 1MB
		config.input_buffer_ratio = 0.8;  // 80% for input
		config.short_table_buffer_ratio = 0.3;  // 30% of input for short table
		
		// Input buffer = 1MB * 0.8 = 800KB
		idx_t expected_input = static_cast<idx_t>(1000000 * 0.8);
		REQUIRE(config.GetInputBufferBudget() == expected_input);
		
		// Short table = 800KB * 0.3 = 240KB
		idx_t expected_short = static_cast<idx_t>(1000000 * 0.8 * 0.3);
		REQUIRE(config.GetShortTableBufferBudget() == expected_short);
		
		// Long table = 800KB - 240KB = 560KB
		idx_t expected_long = expected_input - expected_short;
		REQUIRE(config.GetLongTableBufferBudget() == expected_long);
	}
	
	SECTION("Edge case: all input to short table") {
		NLJPolicyConfig config;
		config.memory_budget = 1000000;
		config.input_buffer_ratio = 0.5;
		config.short_table_buffer_ratio = 1.0; // 100% to short table
		
		idx_t expected_input = 500000;
		idx_t expected_short = 500000;
		idx_t expected_long = 0;
		
		REQUIRE(config.GetInputBufferBudget() == expected_input);
		REQUIRE(config.GetShortTableBufferBudget() == expected_short);
		REQUIRE(config.GetLongTableBufferBudget() == expected_long);
	}
	
	SECTION("Edge case: all input to long table") {
		NLJPolicyConfig config;
		config.memory_budget = 1000000;
		config.input_buffer_ratio = 0.5;
		config.short_table_buffer_ratio = 0.0; // 0% to short table
		
		idx_t expected_input = 500000;
		idx_t expected_short = 0;
		idx_t expected_long = 500000;
		
		REQUIRE(config.GetInputBufferBudget() == expected_input);
		REQUIRE(config.GetShortTableBufferBudget() == expected_short);
		REQUIRE(config.GetLongTableBufferBudget() == expected_long);
	}
	
	SECTION("Derived ratios are correct") {
		NLJPolicyConfig config;
		config.input_buffer_ratio = 0.7;
		config.short_table_buffer_ratio = 0.4;
		
		REQUIRE(config.GetOutputBufferRatio() == Approx(0.3));
		REQUIRE(config.GetLongTableBufferRatio() == Approx(0.6));
	}
	
	SECTION("Small memory budget") {
		NLJPolicyConfig config;
		config.memory_budget = 100; // Very small
		config.input_buffer_ratio = 0.5;
		config.short_table_buffer_ratio = 0.5;
		
		REQUIRE(config.GetInputBufferBudget() == 50);
		REQUIRE(config.GetShortTableBufferBudget() == 25);
		REQUIRE(config.GetLongTableBufferBudget() == 25);
	}
}

TEST_CASE("Test SortMergePolicyConfig", "[operator_memory_policy]") {
	
	SECTION("Default config") {
		SortMergePolicyConfig config;
		REQUIRE(config.memory_budget == 0);
		REQUIRE(config.input_buffer_ratio == 0.5);
		REQUIRE(config.GetOutputBufferRatio() == 0.5);
	}
	
	SECTION("Custom ratios") {
		SortMergePolicyConfig config;
		config.input_buffer_ratio = 0.75;
		REQUIRE(config.GetOutputBufferRatio() == Approx(0.25));
	}
}

TEST_CASE("Test DefaultOperatorMemoryPolicy", "[operator_memory_policy]") {
	
	SECTION("Returns default configs") {
		DefaultOperatorMemoryPolicy policy;
		
		auto nlj_config = policy.GetNestedLoopJoinConfig();
		REQUIRE(nlj_config.memory_budget == 0);
		REQUIRE(nlj_config.input_buffer_ratio == 0.5);
		
		auto blockwise_config = policy.GetBlockwiseNLJConfig();
		REQUIRE(blockwise_config.memory_budget == 0);
		
		auto sort_merge_config = policy.GetSortMergeConfig();
		REQUIRE(sort_merge_config.memory_budget == 0);
	}
}
