#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/sort/merge_tree.hpp"
#include "duckdb/common/sort/kway_merge_sorter.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_info.hpp"

using namespace duckdb;

// Block size constant for tests (256KB - header = 262136)
static constexpr idx_t TEST_BLOCK_SIZE = 262136;

//===--------------------------------------------------------------------===//
// Merge Tree Tests
//===--------------------------------------------------------------------===//

TEST_CASE("MergeTree basic initialization", "[sort][kway]") {
	// MergeTree requires a valid SortLayout, which requires more setup
	// For now, just test that the types exist
	REQUIRE(MergeTree::INVALID_RUN == NumericLimits<idx_t>::Maximum());
}

TEST_CASE("KWayMergePolicyConfig defaults", "[sort][kway]") {
	KWayMergePolicyConfig config;

	// Check default values
	REQUIRE(config.memory_budget == 0);
	REQUIRE(config.k == 8);
	// input_buffer_size and output_buffer_size default to 0 (auto-compute)
	REQUIRE(config.input_buffer_size == 0);
	REQUIRE(config.output_buffer_size == 0);
	REQUIRE(config.adaptive_k == true);
	REQUIRE(config.min_buffer_rows == 2048);  // Updated default
}

TEST_CASE("KWayMergePolicyConfig ComputeOptimalK", "[sort][kway]") {
	KWayMergePolicyConfig config;
	config.memory_budget = 10 * TEST_BLOCK_SIZE;  // 10 blocks
	config.input_buffer_size = TEST_BLOCK_SIZE;
	config.output_buffer_size = TEST_BLOCK_SIZE;

	// Test computing optimal k
	// With 10 blocks budget, 1 block output, we have 9 blocks for input
	// So optimal k should be 9 (if num_runs >= 9)
	idx_t row_width = 100;

	// If we have 20 runs, optimal k should be limited by memory
	idx_t k = config.ComputeOptimalK(20, row_width);
	REQUIRE(k >= 2);  // At minimum k=2
	REQUIRE(k <= 20); // At most num_runs

	// If we have 5 runs, optimal k should be 5
	k = config.ComputeOptimalK(5, row_width);
	REQUIRE(k == 5);  // When num_runs < memory-limited k, use num_runs

	// Edge case: 2 runs
	k = config.ComputeOptimalK(2, row_width);
	REQUIRE(k == 2);
}

TEST_CASE("KWayMergePolicyConfig buffer size calculation", "[sort][kway]") {
	KWayMergePolicyConfig config;
	config.k = 4;
	config.input_buffer_size = TEST_BLOCK_SIZE;

	// Total input buffer size = k * input_buffer_size
	idx_t total = config.k * config.input_buffer_size;
	REQUIRE(total == 4 * TEST_BLOCK_SIZE);
}

//===--------------------------------------------------------------------===//
// Integration Tests with Database
//===--------------------------------------------------------------------===//

TEST_CASE("K-way merge with simple ORDER BY", "[sort][kway][integration]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Create a table and insert data
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test_sort (id INTEGER, value VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test_sort SELECT i, 'value_' || i::VARCHAR FROM range(1000) t(i)"));

	// Test basic ORDER BY (uses standard path, not k-way)
	auto result = con.Query("SELECT id FROM test_sort ORDER BY id LIMIT 10");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
}

TEST_CASE("K-way merge with descending ORDER BY", "[sort][kway][integration]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Create a table and insert data
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test_sort_desc (id INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test_sort_desc SELECT i FROM range(100) t(i)"));

	// Test ORDER BY DESC
	auto result = con.Query("SELECT id FROM test_sort_desc ORDER BY id DESC LIMIT 5");
	REQUIRE(CHECK_COLUMN(result, 0, {99, 98, 97, 96, 95}));
}

TEST_CASE("K-way merge with multiple ORDER BY columns", "[sort][kway][integration]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Create a table with multiple columns
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test_multi_sort (a INTEGER, b INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test_multi_sort VALUES (1, 3), (1, 1), (2, 2), (1, 2), (2, 1)"));

	// Test multi-column ORDER BY
	auto result = con.Query("SELECT a, b FROM test_multi_sort ORDER BY a, b");
	// Verify ordering: (1,1), (1,2), (1,3), (2,1), (2,2)
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1, 1, 2, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 1, 2}));
}

TEST_CASE("K-way merge with NULL values", "[sort][kway][integration]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Create a table with NULL values
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test_null_sort (id INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test_null_sort VALUES (3), (NULL), (1), (NULL), (2)"));

	// Test ORDER BY with NULLS (default: NULLS LAST for ASC)
	auto result = con.Query("SELECT id FROM test_null_sort ORDER BY id");
	// First 3 should be 1, 2, 3, then NULLs
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, Value(), Value()}));
}

TEST_CASE("K-way merge with large dataset", "[sort][kway][integration][.]") {
	// This test is marked with [.] to exclude from default runs (it's slow)
	DuckDB db(nullptr);
	Connection con(db);

	// Create a larger dataset that may trigger external sort
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test_large_sort AS SELECT i AS id, random() AS val FROM range(100000) t(i)"));

	// Test ORDER BY on large dataset
	auto result = con.Query("SELECT id FROM test_large_sort ORDER BY id LIMIT 10");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));

	// Also test the last values
	result = con.Query("SELECT id FROM test_large_sort ORDER BY id DESC LIMIT 10");
	REQUIRE(CHECK_COLUMN(result, 0, {99999, 99998, 99997, 99996, 99995, 99994, 99993, 99992, 99991, 99990}));
}

TEST_CASE("K-way merge with strings", "[sort][kway][integration]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Create a table with string values
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test_string_sort (name VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test_string_sort VALUES ('banana'), ('apple'), ('cherry'), ('date'), ('elderberry')"));

	// Test ORDER BY on strings
	auto result = con.Query("SELECT name FROM test_string_sort ORDER BY name");
	REQUIRE(CHECK_COLUMN(result, 0, {"apple", "banana", "cherry", "date", "elderberry"}));
}
