#include "UnitTest.h"
#include "../Source/Utility/Lock.h"

using namespace vl;
using namespace vl::database;
using namespace vl::collections;

extern WString GetTempFolder();
#define TEMP_DIR GetTempFolder()+
#define KB *1024
#define MB *1024*1024

TEST_CASE(Utility_Lock_Registering)
{
	BufferManager bm(4 KB, 1024);
	auto sourceA = bm.LoadMemorySource();
	BufferSource sourceB{2};

	LockManager lm(&bm);
	BufferTable tableA{1};
	BufferTable tableB{2};
	BufferTransaction transA{1};
	BufferTransaction transB{2};

	// Registering table will fail using unexisting source
	TEST_ASSERT(lm.RegisterTable(tableA, sourceB) == false);

	// Registering table will success using existing source
	TEST_ASSERT(lm.RegisterTable(tableA, sourceA) == true);

	// Registering table twice will fail
	TEST_ASSERT(lm.RegisterTable(tableA, sourceA) == false);

	// Unregistering unexisting table will fail
	TEST_ASSERT(lm.UnregisterTable(tableB) == false);

	// Unregistering existing table will success
	TEST_ASSERT(lm.UnregisterTable(tableA) == true);
	TEST_ASSERT(lm.UnregisterTable(tableB) == false);
	TEST_ASSERT(lm.RegisterTable(tableA, sourceA) == true);

	// Registering transaction ibce will success
	TEST_ASSERT(lm.RegisterTransaction(transA, 0) == true);

	// Registering transaction twice will fail
	TEST_ASSERT(lm.RegisterTransaction(transA, 0) == false);

	// Unregistering unexisting transaction will fail
	TEST_ASSERT(lm.UnregisterTransaction(transB) == false);

	// Unregistering existing transaction will success
	TEST_ASSERT(lm.UnregisterTransaction(transA) == true);
	TEST_ASSERT(lm.UnregisterTransaction(transB) == false);
	TEST_ASSERT(lm.RegisterTransaction(transA, 0) == true);

}
