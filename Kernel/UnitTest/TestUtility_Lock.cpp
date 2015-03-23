#include "UnitTest.h"
#include "../Source/Utility/Lock.h"

#define LOCK_TYPES ((vl::vint)vl::database::LockTargetAccess::NumbersOfLockTypes)

using namespace vl;
using namespace vl::database;
using namespace vl::collections;

const bool lockCompatibility
	[LOCK_TYPES] // Request
	[LOCK_TYPES] // Existing
= {
	{true,	true,	true,	true,	true,	false},
	{true,	true,	true,	false,	false,	false},
	{true,	true,	false,	false,	false,	false},
	{true,	false,	false,	true,	false,	false},
	{true,	false,	false,	false,	false,	false},
	{false,	false,	false,	false,	false,	false},
};

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
	BufferPage pageA = bm.AllocatePage(source);				\
	BufferPage pageB = bm.AllocatePage(source);				\
	TEST_ASSERT(lm.RegisterTable(tableA, source) == true);	\
	TEST_ASSERT(lm.RegisterTable(tableB, source) == true);	\
	TEST_ASSERT(lm.RegisterTransaction(transA, 0) == true);	\
	TEST_ASSERT(lm.RegisterTransaction(transB, 0) == true);	\

#define TABLE LockTargetType::Table
#define PAGE LockTargetType::Page
#define ROW LockTargetType::Row
#define SLOCK LockTargetAccess::Shared
#define XLOCK LockTargetAccess::Exclusive

TEST_CASE(Utility_Lock_Table)
{
	INIT_LOCK_MANAGER;
	BufferTransaction lo, loA, loB;
	LockTarget lt, ltA, ltB;
	LockResult lr, lrA, lrB;
	
	// Lock invalid table will fail
	lo = transA;
	lt = {SLOCK, BufferTable::Invalid()}; 
	TEST_ASSERT(lm.AcquireLock(lo, lt, lr) == false);

	// Lock using invalid transaction will fail
	lo = BufferTransaction::Invalid();
	lt = {SLOCK, tableA}; 
	TEST_ASSERT(lm.AcquireLock(lo, lt, lr) == false);

	// Unlock unexisting lock will fail
	lo = transA;
	lt = {SLOCK, tableA}; 
	TEST_ASSERT(lm.ReleaseLock(lo, lt) == false);

	// Check lock compatiblity
	loA = transA;
	loB = transB;
	for (vint i = 0; i < LOCK_TYPES; i++)
	{
		ltA = {(LockTargetAccess)i, tableA};
		TEST_ASSERT(lm.AcquireLock(loA, ltA, lrA) == true);
		TEST_ASSERT(lrA.blocked == false);

		for (vint j = 0; j < LOCK_TYPES; j++)
		{
			ltB = {(LockTargetAccess)j, tableA};
			TEST_ASSERT(lm.AcquireLock(loB, ltB, lrB) == true);
			TEST_ASSERT(lrB.blocked == !lockCompatibility[j][i]);
			TEST_ASSERT(lm.ReleaseLock(loB, ltB) == true);
		}

		TEST_ASSERT(lm.ReleaseLock(loA, ltA) == true);
	}
	
	// Check unrelated lock
	for (vint i = 0; i < LOCK_TYPES; i++)
	{
		ltA = {(LockTargetAccess)i, tableA};
		TEST_ASSERT(lm.AcquireLock(loA, ltA, lrA) == true);
		TEST_ASSERT(lrA.blocked == false);

		for (vint j = 0; j < LOCK_TYPES; j++)
		{
			ltB = {(LockTargetAccess)j, tableB};
			TEST_ASSERT(lm.AcquireLock(loB, ltB, lrB) == true);
			TEST_ASSERT(lrB.blocked == false);
			TEST_ASSERT(lm.ReleaseLock(loB, ltB) == true);
		}

		TEST_ASSERT(lm.ReleaseLock(loA, ltA) == true);
	}

	// Ensure lock released
	TEST_ASSERT(lm.TableHasLocks(tableA) == false);
	TEST_ASSERT(lm.TableHasLocks(tableB) == false);
}

TEST_CASE(Utility_Lock_Page)
{
	INIT_LOCK_MANAGER;
	BufferTransaction lo, loA, loB;
	LockTarget lt, ltA, ltB;
	LockResult lr, lrA, lrB;
	
	// Lock invalid table or page will fail
	lo = transA;
	lt = {SLOCK, BufferTable::Invalid(), pageA}; 
	TEST_ASSERT(lm.AcquireLock(lo, lt, lr) == false);
	lt = {SLOCK, tableA, BufferPage::Invalid()}; 
	TEST_ASSERT(lm.AcquireLock(lo, lt, lr) == false);

	// Lock using invalid transaction will fail
	lo = BufferTransaction::Invalid();
	lt = {SLOCK, tableA, pageA}; 
	TEST_ASSERT(lm.AcquireLock(lo, lt, lr) == false);

	// Unlock unexisting lock will fail
	lo = transA;
	lt = {SLOCK, tableA, pageA}; 
	TEST_ASSERT(lm.ReleaseLock(lo, lt) == false);

	// Check lock compatiblity
	loA = transA;
	loB = transB;
	for (vint i = 0; i < LOCK_TYPES; i++)
	{
		ltA = {(LockTargetAccess)i, tableA, pageA};
		TEST_ASSERT(lm.AcquireLock(loA, ltA, lrA) == true);
		TEST_ASSERT(lrA.blocked == false);

		for (vint j = 0; j < LOCK_TYPES; j++)
		{
			ltB = {(LockTargetAccess)j, tableA, pageA};
			TEST_ASSERT(lm.AcquireLock(loB, ltB, lrB) == true);
			TEST_ASSERT(lrB.blocked == !lockCompatibility[j][i]);
			TEST_ASSERT(lm.ReleaseLock(loB, ltB) == true);
		}

		TEST_ASSERT(lm.ReleaseLock(loA, ltA) == true);
	}
	
	// Check unrelated lock
	for (vint i = 0; i < LOCK_TYPES; i++)
	{
		ltA = {(LockTargetAccess)i, tableA, pageA};
		TEST_ASSERT(lm.AcquireLock(loA, ltA, lrA) == true);
		TEST_ASSERT(lrA.blocked == false);

		for (vint j = 0; j < LOCK_TYPES; j++)
		{
			ltB = {(LockTargetAccess)j, tableA, pageB};
			TEST_ASSERT(lm.AcquireLock(loB, ltB, lrB) == true);
			TEST_ASSERT(lrB.blocked == false);
			TEST_ASSERT(lm.ReleaseLock(loB, ltB) == true);
		}

		TEST_ASSERT(lm.ReleaseLock(loA, ltA) == true);
	}

	for (vint i = 0; i < LOCK_TYPES; i++)
	{
		ltA = {(LockTargetAccess)i, tableA, pageA};
		TEST_ASSERT(lm.AcquireLock(loA, ltA, lrA) == true);
		TEST_ASSERT(lrA.blocked == false);

		for (vint j = 0; j < LOCK_TYPES; j++)
		{
			ltB = {(LockTargetAccess)j, tableB, pageA};
			TEST_ASSERT(lm.AcquireLock(loB, ltB, lrB) == true);
			TEST_ASSERT(lrB.blocked == false);
			TEST_ASSERT(lm.ReleaseLock(loB, ltB) == true);
		}

		TEST_ASSERT(lm.ReleaseLock(loA, ltA) == true);
	}

	// Ensure lock released
	TEST_ASSERT(lm.TableHasLocks(tableA) == false);
	TEST_ASSERT(lm.TableHasLocks(tableB) == false);
}

TEST_CASE(Utility_Lock_Row)
{
	INIT_LOCK_MANAGER;
	BufferPointer addA, addB;
	TEST_ASSERT(bm.EncodePointer(addA, pageA, 0));
	TEST_ASSERT(bm.EncodePointer(addB, pageB, 0));
	BufferTransaction lo, loA, loB;
	LockTarget lt, ltA, ltB;
	LockResult lr, lrA, lrB;
	
	// Lock invalid table or address will fail
	lo = transA;
	lt = {SLOCK, BufferTable::Invalid(), addA}; 
	TEST_ASSERT(lm.AcquireLock(lo, lt, lr) == false);
	lt = {SLOCK, tableA, BufferPointer::Invalid()}; 
	TEST_ASSERT(lm.AcquireLock(lo, lt, lr) == false);

	// Lock using invalid transaction will fail
	lo = BufferTransaction::Invalid();
	lt = {SLOCK, tableA, addA}; 
	TEST_ASSERT(lm.AcquireLock(lo, lt, lr) == false);

	// Unlock unexisting lock will fail
	lo = transA;
	lt = {SLOCK, tableA, addA}; 
	TEST_ASSERT(lm.ReleaseLock(lo, lt) == false);

	// Check lock compatiblity
	loA = transA;
	loB = transB;
	for (vint i = 0; i < LOCK_TYPES; i++)
	{
		ltA = {(LockTargetAccess)i, tableA, addA};
		TEST_ASSERT(lm.AcquireLock(loA, ltA, lrA) == true);
		TEST_ASSERT(lrA.blocked == false);

		for (vint j = 0; j < LOCK_TYPES; j++)
		{
			ltB = {(LockTargetAccess)j, tableA, addA};
			TEST_ASSERT(lm.AcquireLock(loB, ltB, lrB) == true);
			TEST_ASSERT(lrB.blocked == !lockCompatibility[j][i]);
			TEST_ASSERT(lm.ReleaseLock(loB, ltB) == true);
		}

		TEST_ASSERT(lm.ReleaseLock(loA, ltA) == true);
	}
	
	// Check unrelated lock
	for (vint i = 0; i < LOCK_TYPES; i++)
	{
		ltA = {(LockTargetAccess)i, tableA, addA};
		TEST_ASSERT(lm.AcquireLock(loA, ltA, lrA) == true);
		TEST_ASSERT(lrA.blocked == false);

		for (vint j = 0; j < LOCK_TYPES; j++)
		{
			ltB = {(LockTargetAccess)j, tableA, addB};
			TEST_ASSERT(lm.AcquireLock(loB, ltB, lrB) == true);
			TEST_ASSERT(lrB.blocked == false);
			TEST_ASSERT(lm.ReleaseLock(loB, ltB) == true);
		}

		TEST_ASSERT(lm.ReleaseLock(loA, ltA) == true);
	}

	for (vint i = 0; i < LOCK_TYPES; i++)
	{
		ltA = {(LockTargetAccess)i, tableA, addA};
		TEST_ASSERT(lm.AcquireLock(loA, ltA, lrA) == true);
		TEST_ASSERT(lrA.blocked == false);

		for (vint j = 0; j < LOCK_TYPES; j++)
		{
			ltB = {(LockTargetAccess)j, tableB, addA};
			TEST_ASSERT(lm.AcquireLock(loB, ltB, lrB) == true);
			TEST_ASSERT(lrB.blocked == false);
			TEST_ASSERT(lm.ReleaseLock(loB, ltB) == true);
		}

		TEST_ASSERT(lm.ReleaseLock(loA, ltA) == true);
	}

	// Ensure lock released
	TEST_ASSERT(lm.TableHasLocks(tableA) == false);
	TEST_ASSERT(lm.TableHasLocks(tableB) == false);
}

TEST_CASE(Utility_Lock_Hierarchy)
{
	INIT_LOCK_MANAGER;
}
