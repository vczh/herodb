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
		};

		enum class LockTargetType
		{
			Table,
			Page,
			Row,
		};

		enum class LockTargetAccess
		{
			SharedRead,
			ExclusiveRead,
			ExclusiveWrite,
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
		public:
			LockManager(BufferManager* _bm);
			~LockManager();

			bool			RegisterTable(BufferTable table, BufferSource source);
			bool			UnregisterTable(BufferTable table);
			bool			RegisterTransaction(BufferTransaction trans, vuint64_t importance);
			bool			UnregisterTransaction(BufferTransaction trans);

			bool			AcquireLock(const LockOwner& owner, const LockTarget& target, LockResult& result);
			bool			ReleaseLock(const LockOwner& owner, const LockTarget& target, const LockResult& result);

			BufferTask		PickTask(LockResult& result);
			void			DetectDeadlock(DeadlockInfo::List& infos);
			bool			Rollback(Transaction trans);
		};
	}
}

#endif
