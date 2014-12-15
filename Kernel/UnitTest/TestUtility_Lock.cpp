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

#define INIT_LOCK_MANAGER									\
	BufferManager bm(4 KB, 1024);							\
	auto source = bm.LoadMemorySource();					\
	LockManager lm(&bm);									\
	BufferTable tableA{1}, tableB{2};						\
	BufferTransaction transA{1}, transB{2};					\
	BufferTask taskA{1}, taskB{2};							\
	TEST_ASSERT(lm.RegisterTable(tableA, source) == true);	\
	TEST_ASSERT(lm.RegisterTable(tableB, source) == true);	\
	TEST_ASSERT(lm.RegisterTransaction(transA, 0) == true);	\
	TEST_ASSERT(lm.RegisterTransaction(transB, 0) == true);	\

#define TABLE LockTargetType::Table
#define PAGE LockTargetType::Page
#define ROW LockTargetType::Row
#define SHARED LockTargetAccess::SharedRead
#define XREAD LockTargetAccess::ExclusiveRead
#define XWRITE LockTargetAccess::ExclusiveWrite

TEST_CASE(Utility_Lock_Table)
{
	INIT_LOCK_MANAGER;
	LockOwner lo, loA, loB;
	LockTarget lt, ltA, ltB;
	LockResult lr, lrA, lrB;
	
	// Lock invalid table will fail
	lo = {transA, taskA};
	lt = {SHARED, BufferTable::Invalid()}; 
	TEST_ASSERT(lm.AcquireLock(lo, lt, lr) == false);

	// Lock using invalid transaction will fail
	lo = {BufferTransaction::Invalid(), taskA};
	lt = {SHARED, tableA}; 
	TEST_ASSERT(lm.AcquireLock(lo, lt, lr) == false);

	// Lock using invalid task will fail
	lo = {transA, BufferTask::Invalid()};
	lt = {SHARED, tableA}; 
	TEST_ASSERT(lm.AcquireLock(lo, lt, lr) == false);

	// Unlock unexisting lock will fail
	lo = {transA, taskA};
	lt = {SHARED, tableA}; 
	TEST_ASSERT(lm.ReleaseLock(lo, lt, lr) == false);

	// Lock shared using valid arguments will success
	
	// Lock any access twice will fail
	
	// Unlock existing lock will success
	
	// Lock exclusive read using valid arguments will success
	
	// Lock any access twice will fail
	
	// Unlock existing lock will success
	
	// Lock exclusive write using valid arguments will success
	
	// Lock any access twice will fail
	
	// Unlock existing lock will success
	
	// Lock shared and shared will success
	
	// Lock shared and exclusive read will fail
	
	// Lock shared and exclusive write will fail
	
	// Unlock existing lock will success
	
	// Lock exclusive read and shared will fail
	
	// Lock exclusive read and exclusive read will success
	
	// Lock exclusive read and exclusive write will fail
	
	// Unlock existing lock will success
	
	// Lock exclusive write and shared will fail
	
	// Lock exclusive write and exclusive read will fail
	
	// Lock exvlusive write and exclusive write will fail
	
	// Unlock existing lock will success
}

TEST_CASE(Utility_Lock_Page)
{
	INIT_LOCK_MANAGER;
}

TEST_CASE(Utility_Lock_Row)
{
	INIT_LOCK_MANAGER;
}

TEST_CASE(Utility_Lock_Hierarchy)
{
	INIT_LOCK_MANAGER;
}
