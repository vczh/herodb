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
			LockTargetAccess	access			= LockTargetAccess::SharedRead;
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
		public:
			LockManager(BufferManager* _bm);
			~LockManager();

			bool				RegisterTable(BufferTable table, BufferSource source);
			bool				UnregisterTable(BufferTable table);
			bool				RegisterTransaction(BufferTransaction trans, vuint64_t importance);
			bool				UnregisterTransaction(BufferTransaction trans);

			bool				AcquireLock(const LockOwner& owner, const LockTarget& target, LockResult& result);
			bool				ReleaseLock(const LockOwner& owner, const LockTarget& target, const LockResult& result);

			BufferTask			PickTask(LockResult& result);
			void				DetectDeadlock(DeadlockInfo::List& infos);
			bool				Rollback(BufferTransaction trans);
		};
	}
}

#endif
