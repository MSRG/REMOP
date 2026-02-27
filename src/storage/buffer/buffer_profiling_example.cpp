//===----------------------------------------------------------------------===//
// Buffer Manager Profiling Example
//===----------------------------------------------------------------------===//
#include "duckdb.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"
#include <iostream>

using namespace duckdb;
using namespace std;

int main() {
	DuckDB db(nullptr);
	Connection con(db);

	// Get buffer manager
	auto &buffer_manager = StandardBufferManager::GetBufferManager(con.context->db->GetDatabase());

	// Enable profiling
	buffer_manager.SetProfiling(true);
	cout << "Buffer manager profiling enabled: " << buffer_manager.IsProfilingEnabled() << endl;

	// Run some queries that will trigger buffer manager activity
	con.Query("CREATE TABLE test AS SELECT range AS id, 'data_' || range AS val FROM range(10000)");
	con.Query("SELECT COUNT(*) FROM test");
	con.Query("SELECT * FROM test WHERE id < 100 ORDER BY id");

	// Get profiling report
	cout << "\n" << buffer_manager.GetProfilingReport() << endl;

	// Reset metrics
	buffer_manager.ResetProfilingMetrics();
	cout << "\nMetrics reset. Running more queries...\n" << endl;

	// Run more queries
	con.Query("SELECT * FROM test WHERE id > 9000 ORDER BY val");
	con.Query("DROP TABLE test");

	// Get updated report
	cout << buffer_manager.GetProfilingReport() << endl;

	// Disable profiling
	buffer_manager.SetProfiling(false);
	cout << "\nBuffer manager profiling disabled: " << !buffer_manager.IsProfilingEnabled() << endl;

	return 0;
}
