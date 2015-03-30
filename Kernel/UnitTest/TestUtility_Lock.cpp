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
	BufferTransaction transC{3}, transD{4};					\
	BufferPage pageA = bm.AllocatePage(source);				\
	BufferPage pageB = bm.AllocatePage(source);				\
	TEST_ASSERT(lm.RegisterTable(tableA, source) == true);	\
	TEST_ASSERT(lm.RegisterTable(tableB, source) == true);	\
	TEST_ASSERT(lm.RegisterTransaction(transA, 0) == true);	\
	TEST_ASSERT(lm.RegisterTransaction(transB, 0) == true);	\
	TEST_ASSERT(lm.RegisterTransaction(transC, 1) == true);	\
	TEST_ASSERT(lm.RegisterTransaction(transD, 1) == true);	\
	BufferTransaction lo;									\
	LockTarget lt;											\
	LockResult lr;											\

#define TABLE LockTargetType::Table
#define PAGE LockTargetType::Page
#define ROW LockTargetType::Row
#define SLOCK LockTargetAccess::Shared
#define XLOCK LockTargetAccess::Exclusive
#define ISLOCK LockTargetAccess::IntentShared
#define IXLOCK LockTargetAccess::IntentExclusive
#define SIXLOCK LockTargetAccess::SharedIntentExclusive
#define ULOCK LockTargetAccess::Update

namespace general_lock_testing
{
	void TestLock(
		LockManager& lm,
		BufferTransaction loA,
		BufferTransaction loB,
		Func<LockTarget(vint)> ltAGen,
		Func<LockTarget(vint)> ltBGen
		)
	{
		LockResult lr, lrA, lrB;

		// Check lock compatibility
		for (vint i = 0; i < LOCK_TYPES; i++)
		{
			auto ltA = ltAGen(i);
			TEST_ASSERT(lm.AcquireLock(loA, ltA, lrA) == true);
			TEST_ASSERT(lrA.blocked == false);

			for (vint j = 0; j < LOCK_TYPES; j++)
			{
				auto ltB = ltAGen(j);
				TEST_ASSERT(lm.AcquireLock(loB, ltB, lrB) == true);
				TEST_ASSERT(lrB.blocked == !lockCompatibility[j][i]);
				TEST_ASSERT(lm.ReleaseLock(loB, ltB) == true);
			}

			TEST_ASSERT(lm.ReleaseLock(loA, ltA) == true);
		}
		
		// Check unrelated lock
		for (vint i = 0; i < LOCK_TYPES; i++)
		{
			auto ltA = ltAGen(i);
			TEST_ASSERT(lm.AcquireLock(loA, ltA, lrA) == true);
			TEST_ASSERT(lrA.blocked == false);

			for (vint j = 0; j < LOCK_TYPES; j++)
			{
				auto ltB = ltBGen(j);
				TEST_ASSERT(lm.AcquireLock(loB, ltB, lrB) == true);
				TEST_ASSERT(lrB.blocked == false);
				TEST_ASSERT(lm.ReleaseLock(loB, ltB) == true);
			}

			TEST_ASSERT(lm.ReleaseLock(loA, ltA) == true);
		}

		// Check upgrade lock
		for (vint i = 0; i < LOCK_TYPES; i++)
		{
			for (vint j = 0; j < LOCK_TYPES; j++)
			{
				auto ltA = ltAGen(i);
				TEST_ASSERT(lm.AcquireLock(loA, ltA, lrA) == true);
				TEST_ASSERT(lrA.blocked == false);

				auto ltB = ltAGen(j);
				TEST_ASSERT(lm.UpgradeLock(loA, ltA, ltB.access, lrB) == true);
				TEST_ASSERT(lrB.blocked == false);
				TEST_ASSERT(lm.ReleaseLock(loA, ltB) == true);
			}
		}

		// Check upgrade lock compatibility
		for (vint i = 0; i < LOCK_TYPES; i++)
		{
			auto ltA = ltAGen(i);
			TEST_ASSERT(lm.AcquireLock(loA, ltA, lrA) == true);
			TEST_ASSERT(lrA.blocked == false);

			for (vint j = 0; j < LOCK_TYPES; j++)
			{
				for (vint k = 0; k < LOCK_TYPES; k++)
				{
					auto ltB = ltAGen(j);
					TEST_ASSERT(lm.AcquireLock(loB, ltB, lrB) == true);
					TEST_ASSERT(lrB.blocked == !lockCompatibility[j][i]);

					auto lt = ltAGen(k);
					if (lrB.blocked)
					{
						TEST_ASSERT(lm.UpgradeLock(loB, ltB, lt.access, lr) == false);
						TEST_ASSERT(lm.ReleaseLock(loB, ltB) == true);
					}
					else
					{
						TEST_ASSERT(lm.UpgradeLock(loB, ltB, lt.access, lr) == true);
						TEST_ASSERT(lr.blocked == !lockCompatibility[k][i]);
						TEST_ASSERT(lm.ReleaseLock(loB, lt) == true);
					}
				}
			}

			TEST_ASSERT(lm.ReleaseLock(loA, ltA) == true);
		}
	}
}
using namespace general_lock_testing;

TEST_CASE(Utility_Lock_Table)
{
	INIT_LOCK_MANAGER;
	
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

	TestLock(
		lm,
		transA,
		transB,
		[=](vint a){ return LockTarget{(LockTargetAccess)a, tableA}; },
		[=](vint a){ return LockTarget{(LockTargetAccess)a, tableB}; }
		);
	
	// Ensure lock released
	TEST_ASSERT(lm.TableHasLocks(tableA) == false);
	TEST_ASSERT(lm.TableHasLocks(tableB) == false);
}

TEST_CASE(Utility_Lock_Page)
{
	INIT_LOCK_MANAGER;
	
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

	TestLock(
		lm,
		transA,
		transB,
		[=](vint a){ return LockTarget{(LockTargetAccess)a, tableA, pageA}; },
		[=](vint a){ return LockTarget{(LockTargetAccess)a, tableA, pageB}; }
		);

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

	TestLock(
		lm,
		transA,
		transB,
		[=](vint a){ return LockTarget{(LockTargetAccess)a, tableA, addA}; },
		[=](vint a){ return LockTarget{(LockTargetAccess)a, tableA, addB}; }
		);

	// Ensure lock released
	TEST_ASSERT(lm.TableHasLocks(tableA) == false);
	TEST_ASSERT(lm.TableHasLocks(tableB) == false);
}

TEST_CASE(Utility_Lock_PickTransaction)
{
	INIT_LOCK_MANAGER;

	LockTarget ltAS = {SLOCK, tableA};
	LockTarget ltAX = {XLOCK, tableA};
	LockTarget ltBS = {SLOCK, tableB};
	LockTarget ltBX = {XLOCK, tableB};

	// Importance (1)

	TEST_ASSERT(lm.AcquireLock(transA, ltAS, lr) == true);
	TEST_ASSERT(lr.blocked == false);
	TEST_ASSERT(lm.AcquireLock(transB, ltAX, lr) == true);
	TEST_ASSERT(lr.blocked == true);
	TEST_ASSERT(lm.AcquireLock(transC, ltAX, lr) == true);
	TEST_ASSERT(lr.blocked == true);
	TEST_ASSERT(lm.PickTransaction(lr) == BufferTransaction::Invalid());

	TEST_ASSERT(lm.ReleaseLock(transA, ltAS) == true);
	TEST_ASSERT(lm.PickTransaction(lr) == transC);
	TEST_ASSERT(lr.blocked == false);

	TEST_ASSERT(lm.ReleaseLock(transC, ltAX) == true);
	TEST_ASSERT(lm.PickTransaction(lr) == transB);
	TEST_ASSERT(lr.blocked == false);

	TEST_ASSERT(lm.ReleaseLock(transB, ltAX) == true);
	TEST_ASSERT(lm.TableHasLocks(tableA) == false);

	// Importance (2)

	TEST_ASSERT(lm.AcquireLock(transA, ltAS, lr) == true);
	TEST_ASSERT(lr.blocked == false);
	TEST_ASSERT(lm.AcquireLock(transC, ltAX, lr) == true);
	TEST_ASSERT(lr.blocked == true);
	TEST_ASSERT(lm.AcquireLock(transB, ltAX, lr) == true);
	TEST_ASSERT(lr.blocked == true);
	TEST_ASSERT(lm.PickTransaction(lr) == BufferTransaction::Invalid());

	TEST_ASSERT(lm.ReleaseLock(transA, ltAS) == true);
	TEST_ASSERT(lm.PickTransaction(lr) == transC);
	TEST_ASSERT(lr.blocked == false);

	TEST_ASSERT(lm.ReleaseLock(transC, ltAX) == true);
	TEST_ASSERT(lm.PickTransaction(lr) == transB);
	TEST_ASSERT(lr.blocked == false);

	TEST_ASSERT(lm.ReleaseLock(transB, ltAX) == true);
	TEST_ASSERT(lm.TableHasLocks(tableA) == false);

	// Deadlock
	
	TEST_ASSERT(lm.AcquireLock(transA, ltAS, lr) == true);
	TEST_ASSERT(lr.blocked == false);
	TEST_ASSERT(lm.AcquireLock(transB, ltBS, lr) == true);
	TEST_ASSERT(lr.blocked == false);
	TEST_ASSERT(lm.AcquireLock(transA, ltBX, lr) == true);
	TEST_ASSERT(lr.blocked == true);
	TEST_ASSERT(lm.AcquireLock(transB, ltAX, lr) == true);
	TEST_ASSERT(lr.blocked == true);
	TEST_ASSERT(lm.PickTransaction(lr) == BufferTransaction::Invalid());

	TEST_ASSERT(lm.ReleaseLock(transA, ltAS) == true);
	TEST_ASSERT(lm.ReleaseLock(transA, ltBX) == true);
	TEST_ASSERT(lm.PickTransaction(lr) == transB);

	TEST_ASSERT(lm.ReleaseLock(transB, ltBS) == true);
	TEST_ASSERT(lm.ReleaseLock(transB, ltAX) == true);
	TEST_ASSERT(lm.TableHasLocks(tableA) == false);
	TEST_ASSERT(lm.TableHasLocks(tableB) == false);
}

TEST_CASE(Utility_Lock_SimpleDeadlock)
{
	INIT_LOCK_MANAGER;

	LockTarget ltAS = {SLOCK, tableA};
	LockTarget ltAX = {XLOCK, tableA};
	LockTarget ltBS = {SLOCK, tableB};
	LockTarget ltBX = {XLOCK, tableB};
	
	// Deadlock
	
	TEST_ASSERT(lm.AcquireLock(transA, ltAS, lr) == true);
	TEST_ASSERT(lr.blocked == false);
	TEST_ASSERT(lm.AcquireLock(transB, ltBS, lr) == true);
	TEST_ASSERT(lr.blocked == false);
	TEST_ASSERT(lm.AcquireLock(transA, ltBX, lr) == true);
	TEST_ASSERT(lr.blocked == true);
	TEST_ASSERT(lm.AcquireLock(transB, ltAX, lr) == true);
	TEST_ASSERT(lr.blocked == true);
	TEST_ASSERT(lm.PickTransaction(lr) == BufferTransaction::Invalid());

	DeadlockInfo info;
	lm.DetectDeadlock(info);
	TEST_ASSERT(info.acquired.Count() == 2);
	TEST_ASSERT(info.acquired.Keys()[0] == transA);
	TEST_ASSERT(info.acquired.GetByIndex(0).Count() == 1);
	TEST_ASSERT(info.acquired.GetByIndex(0)[0] == ltAS);
	TEST_ASSERT(info.acquired.Keys()[1] == transB);
	TEST_ASSERT(info.acquired.GetByIndex(1).Count() == 1);
	TEST_ASSERT(info.acquired.GetByIndex(1)[0] == ltBS);
	TEST_ASSERT(info.pending.Count() == 2);
	TEST_ASSERT(info.pending.Keys()[0] == transA);
	TEST_ASSERT(info.pending.Values()[0] == ltBX);
	TEST_ASSERT(info.pending.Keys()[1] == transB);
	TEST_ASSERT(info.pending.Values()[1] == ltAX);
	TEST_ASSERT(info.rollbacks.Count() == 1);
	TEST_ASSERT(info.rollbacks[0] == transA || info.rollbacks[0] == transB);

	// Rollback

	auto rollback = info.rollbacks[0];
	TEST_ASSERT(lm.Rollback(rollback) == true);
	TEST_ASSERT(lm.ReleaseLock(transA, ltAS) == (transA != rollback));
	TEST_ASSERT(lm.ReleaseLock(transA, ltBX) == (transA != rollback));
	TEST_ASSERT(lm.ReleaseLock(transB, ltBS) == (transB != rollback));
	TEST_ASSERT(lm.ReleaseLock(transB, ltAX) == (transB != rollback));
	TEST_ASSERT(lm.TableHasLocks(tableA) == false);
	TEST_ASSERT(lm.TableHasLocks(tableB) == false);
}

TEST_CASE(Utility_Lock_MinimizedDeadlockInfo)
{
	INIT_LOCK_MANAGER;

	LockTarget ltAS = {SLOCK, tableA};
	LockTarget ltAX = {XLOCK, tableA};
	LockTarget ltBS = {SLOCK, tableB};
	LockTarget ltBX = {XLOCK, tableB};

	LockTarget ltCS = {SLOCK, tableA, pageA};
	LockTarget ltCX = {XLOCK, tableA, pageA};
	LockTarget ltDS = {SLOCK, tableB, pageB};
	LockTarget ltDX = {XLOCK, tableB, pageB};

	// Deadlock
	
	TEST_ASSERT(lm.AcquireLock(transA, ltAS, lr) == true);
	TEST_ASSERT(lr.blocked == false);
	TEST_ASSERT(lm.AcquireLock(transB, ltBS, lr) == true);
	TEST_ASSERT(lr.blocked == false);
	TEST_ASSERT(lm.AcquireLock(transA, ltCS, lr) == true);
	TEST_ASSERT(lr.blocked == false);
	TEST_ASSERT(lm.AcquireLock(transB, ltDS, lr) == true);
	TEST_ASSERT(lr.blocked == false);

	TEST_ASSERT(lm.AcquireLock(transA, ltBX, lr) == true);
	TEST_ASSERT(lr.blocked == true);
	TEST_ASSERT(lm.AcquireLock(transB, ltAX, lr) == true);
	TEST_ASSERT(lr.blocked == true);
	TEST_ASSERT(lm.AcquireLock(transC, ltCX, lr) == true);
	TEST_ASSERT(lr.blocked == true);
	TEST_ASSERT(lm.AcquireLock(transD, ltDX, lr) == true);
	TEST_ASSERT(lr.blocked == true);

	TEST_ASSERT(lm.PickTransaction(lr) == BufferTransaction::Invalid());

	DeadlockInfo info;
	lm.DetectDeadlock(info);
	TEST_ASSERT(info.acquired.Count() == 2);
	TEST_ASSERT(info.acquired.Keys()[0] == transA);
	TEST_ASSERT(info.acquired.GetByIndex(0).Count() == 1);
	TEST_ASSERT(info.acquired.GetByIndex(0)[0] == ltAS);
	TEST_ASSERT(info.acquired.Keys()[1] == transB);
	TEST_ASSERT(info.acquired.GetByIndex(1).Count() == 1);
	TEST_ASSERT(info.acquired.GetByIndex(1)[0] == ltBS);
	TEST_ASSERT(info.pending.Count() == 2);
	TEST_ASSERT(info.pending.Keys()[0] == transA);
	TEST_ASSERT(info.pending.Values()[0] == ltBX);
	TEST_ASSERT(info.pending.Keys()[1] == transB);
	TEST_ASSERT(info.pending.Values()[1] == ltAX);
	TEST_ASSERT(info.rollbacks.Count() == 1);
	TEST_ASSERT(info.rollbacks[0] == transA || info.rollbacks[0] == transB);

	// Rollback
	
	auto rollback = info.rollbacks[0];
	TEST_ASSERT(lm.Rollback(rollback) == true);
	TEST_ASSERT(lm.ReleaseLock(transA, ltAS) == (transA != rollback));
	TEST_ASSERT(lm.ReleaseLock(transA, ltBX) == (transA != rollback));
	TEST_ASSERT(lm.ReleaseLock(transA, ltCS) == (transA != rollback));
	TEST_ASSERT(lm.ReleaseLock(transB, ltBS) == (transB != rollback));
	TEST_ASSERT(lm.ReleaseLock(transB, ltAX) == (transB != rollback));
	TEST_ASSERT(lm.ReleaseLock(transB, ltDS) == (transB != rollback));
	TEST_ASSERT(lm.ReleaseLock(transC, ltCX) == true);
	TEST_ASSERT(lm.ReleaseLock(transD, ltDX) == true);
	TEST_ASSERT(lm.TableHasLocks(tableA) == false);
	TEST_ASSERT(lm.TableHasLocks(tableB) == false);
	
}
