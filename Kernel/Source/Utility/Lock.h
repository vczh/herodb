/***********************************************************************
Vczh Library++ 3.0
Developer: Zihan Chen(vczh)
Database::Utility

***********************************************************************/

#ifndef VCZH_DATABASE_UTILITY_LOCK
#define VCZH_DATABASE_UTILITY_LOCK

#include "Buffer.h"

namespace vl
{
	namespace database
	{
		enum class LockTargetType
		{
			Table,
			Page,
			Row,
		};

		enum class LockTargetAccess
		{
			IntentShared			= 0, // parent of Shared
			Shared					= 1, // reading the object
			Update					= 2, // intent to update the object, can upgrade to Shared ot Exclusive
			IntentExclusive			= 3, // parent of Exclusive or Update
			SharedIntentExclusive	= 4, // enable a transaction to acquire Shared and IntentExclusive at the same time
			Exclusive				= 5, // writing the object
			NumbersOfLockTypes		= 6,
		};

		struct LockTarget
		{
			LockTargetType		type			= LockTargetType::Table;
			LockTargetAccess	access			= LockTargetAccess::Shared;
			BufferTable			table			= BufferTable::Invalid();
			union
			{
				BufferPage		page;
				BufferPointer	address;
			};

			LockTarget()
			{
			}
			
			LockTarget(LockTargetAccess _access, BufferTable _table)
				:type(LockTargetType::Table)
				,access(_access)
				,table(_table)
			{
			}
			
			LockTarget(LockTargetAccess _access, BufferTable _table, BufferPage _page)
				:type(LockTargetType::Page)
				,access(_access)
				,table(_table)
				,page(_page)
			{
			}
			
			LockTarget(LockTargetAccess _access, BufferTable _table, BufferPointer _address)
				:type(LockTargetType::Row)
				,access(_access)
				,table(_table)
				,address(_address)
			{
			}

			static bool Compare(const LockTarget& a, const LockTarget& b)
			{
				return a.type == b.type
					&& a.access == b.access
					&& a.table.index == b.table.index
					&& (
						(a.type == LockTargetType::Table) ||
						(a.type == LockTargetType::Page && a.page.index == b.page.index) ||
						(a.type == LockTargetType::Row && a.address.index == b.address.index)
					   )
					;
			}

			bool operator==(const LockTarget& b)const { return Compare(*this, b) == true; }
			bool operator!=(const LockTarget& b)const { return Compare(*this, b) != true; }
		};

		struct LockResult
		{
			bool				blocked			= true;
			void*				lockedAddress	= nullptr;
		};

		struct DeadlockInfo
		{
			typedef collections::List<Ptr<DeadlockInfo>>							List;
			typedef collections::Group<BufferTransaction::IndexType, LockTarget>	TransactionGroup;

			TransactionGroup	involvedTransactions;
			BufferTransaction	rollbackTransaction;
		};

		class LockManager : public Object
		{
		protected:
			struct TableInfo
			{
				BufferTable			table;
				BufferSource		source;
			};

			struct TransInfo
			{
				BufferTransaction	trans;
				vuint64_t			importance;
			};

			typedef collections::Dictionary<BufferTable, Ptr<TableInfo>>			TableMap;
			typedef collections::Dictionary<BufferTransaction, Ptr<TransInfo>>		TransMap;

			BufferManager*		bm;
			SpinLock			lock;
			TableMap			tables;
			TransMap			transactions;

		protected:
			typedef collections::SortedList<BufferTransaction>						TransSet;

			template<typename T>
			struct ObjectLockInfo
			{
				typedef T		ObjectType;

				SpinLock		lock;
				T				object;
				volatile vint	intentAcquireCounter = 0;
				TransSet		owners[(vint)LockTargetAccess::NumbersOfLockTypes];

				ObjectLockInfo(const T& _object)
					:object(_object)
				{
				}

				void IncIntent()
				{
					INCRC(&intentAcquireCounter);
				}

				void DecIntent()
				{
					DECRC(&intentAcquireCounter);
				}

				bool IntentedToAcquire()
				{
					return intentAcquireCounter > 0;
				}

				bool IsEmpty()
				{
					for (vint i = 0; i < (vint)LockTargetAccess::NumbersOfLockTypes; i++)
					{
						if (owners[i].Count() > 0)
						{
							return false;
						}
					}
					return true;
				}
			};
			
			struct RowLockInfo : ObjectLockInfo<vuint64_t>
			{
				RowLockInfo(vuint64_t offset)
					:ObjectLockInfo<vuint64_t>(offset)
				{
				}
			};

			typedef collections::Dictionary<vuint64_t, Ptr<RowLockInfo>>			RowLockMap;

			struct PageLockInfo : ObjectLockInfo<BufferPage>
			{
				RowLockMap		rowLocks;

				PageLockInfo(const BufferPage& page)
					:ObjectLockInfo<BufferPage>(page)
				{
				}
			};

			typedef collections::Dictionary<BufferPage, Ptr<PageLockInfo>>			PageLockMap;

			struct TableLockInfo : ObjectLockInfo<BufferTable>
			{
				PageLockMap		pageLocks;

				TableLockInfo(const BufferTable& table)
					:ObjectLockInfo<BufferTable>(table)
				{
				}
			};

			typedef collections::Array<Ptr<TableLockInfo>>							TableLockArray;
			typedef collections::Dictionary<BufferTransaction, LockTarget>			PendingLockMap;

			TableLockArray		tableLocks;
			PendingLockMap		pendingLocks;

			template<typename TInfo>
			bool				AcquireObjectLock(Ptr<TInfo> lockInfo, BufferTransaction owner, LockTargetAccess access);
			template<typename TInfo>
			bool				ReleaseObjectLock(Ptr<TInfo> lockInfo, BufferTransaction owner, LockTargetAccess access);
			bool				CheckInput(BufferTransaction owner, const LockTarget& target);
			bool				AddPendingLock(BufferTransaction owner, const LockTarget& target);
			bool				RemovePendingLock(BufferTransaction owner, const LockTarget& target);
		public:
			LockManager(BufferManager* _bm);
			~LockManager();

			bool				RegisterTable(BufferTable table, BufferSource source);
			bool				UnregisterTable(BufferTable table);
			bool				RegisterTransaction(BufferTransaction trans, vuint64_t importance);
			bool				UnregisterTransaction(BufferTransaction trans);

			bool				AcquireLock(BufferTransaction owner, const LockTarget& target, LockResult& result);
			bool				ReleaseLock(BufferTransaction owner, const LockTarget& target);

			BufferTransaction	PickTransaction(LockResult& result);
			void				DetectDeadlock(DeadlockInfo::List& infos);
			bool				Rollback(BufferTransaction trans);
		};
	}
}

#endif
