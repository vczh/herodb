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
		struct LockOwner
		{
			BufferTransaction	transaction		= BufferTransaction::Invalid();
			BufferTask			task			= BufferTask::Invalid();

			LockOwner()
			{
			}

			LockOwner(BufferTransaction _transaction, BufferTask _task)
				:transaction(_transaction)
				,task(_task)
			{
			}

			static bool Compare(const LockOwner& a, const LockOwner& b)
			{
				return a.transaction.index == b.transaction.index
					&& a.task.index == b.task.index
					;
			}

			bool operator==(const LockOwner& b)const { return Compare(*this, b) == true; }
			bool operator!=(const LockOwner& b)const { return Compare(*this, b) == true; }
		};

		enum class LockTargetType
		{
			Table,
			Page,
			Row,
		};

		enum class LockTargetAccess
		{
			Shared,
			Exclusive,
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
			bool operator!=(const LockTarget& b)const { return Compare(*this, b) == true; }
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

			typedef collections::Dictionary<BufferTable::IndexType, Ptr<TableInfo>>			TableMap;
			typedef collections::Dictionary<BufferTransaction::IndexType, Ptr<TransInfo>>	TransMap;

			BufferManager*		bm;
			SpinLock			lock;
			TableMap			tables;
			TransMap			transactions;

		protected:
			typedef collections::Group<BufferTransaction::IndexType, BufferTask::IndexType>	LockOwnerGroup;

			template<typename T>
			struct ObjectLockInfo
			{
				typedef T		ObjectType;

				SpinLock		lock;
				T				object;
				LockOwnerGroup	sharedOwner;
				volatile vint	xWriteCounter = 0;
				LockOwner		xWriteOwner;

				ObjectLockInfo(const T& _object)
					:object(_object)
				{
				}
			};

			struct TableLockInfo : ObjectLockInfo<BufferTable>
			{
				TableLockInfo(const BufferTable& table)
					:ObjectLockInfo<BufferTable>(table)
				{
				}
			};

			struct PendingLockInfo
			{
				LockOwner		owner;
				LockTarget		target;

				PendingLockInfo(const LockOwner& _owner, const LockTarget& _target)
					:owner(_owner)
					,target(_target)
				{
				}
			};

			typedef collections::Array<Ptr<TableLockInfo>>									TableLockArray;
			typedef collections::Group<BufferTransaction::IndexType, Ptr<PendingLockInfo>>	PendingLockGroup;

			TableLockArray		tableLocks;
			PendingLockGroup	pendingLocks;

			template<typename TInfo>
			bool				AcquireObjectLock(Ptr<TInfo> lockInfo, const LockOwner& owner, LockTargetAccess access, LockResult& result);
			template<typename TInfo>
			bool				ReleaseObjectLock(Ptr<TInfo> lockInfo, const LockOwner& owner, LockTargetAccess access);
			bool				CheckInput(const LockOwner& owner, const LockTarget& target);
			bool				AddPendingLock(const LockOwner& owner, const LockTarget& target);
			bool				RemovePendingLock(const LockOwner& owner, const LockTarget& target);
		public:
			LockManager(BufferManager* _bm);
			~LockManager();

			bool				RegisterTable(BufferTable table, BufferSource source);
			bool				UnregisterTable(BufferTable table);
			bool				RegisterTransaction(BufferTransaction trans, vuint64_t importance);
			bool				UnregisterTransaction(BufferTransaction trans);

			bool				AcquireLock(const LockOwner& owner, const LockTarget& target, LockResult& result);
			bool				ReleaseLock(const LockOwner& owner, const LockTarget& target);

			BufferTask			PickTask(LockResult& result);
			void				DetectDeadlock(DeadlockInfo::List& infos);
			bool				Rollback(BufferTransaction trans);
		};
	}
}

#endif
