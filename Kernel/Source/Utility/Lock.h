/***********************************************************************
Vczh Library++ 3.0
Developer: Zihan Chen(vczh)
Database::Utility

***********************************************************************/

#ifndef VCZH_DATABASE_UTILITY_LOCK
#define VCZH_DATABASE_UTILITY_LOCK

#include "Common.h"

namespace vl
{
	namespace database
	{
		struct LockOwner
		{
			BufferTransaction	transaction;
			BufferTask			task;
			bool				exclusive;
		};

		enum class LockTargetType
		{
			Table,
			Page,
			Row,
		};

		struct LockTarget
		{
			LockTargetType		type;
			BufferTable			table;
			union
			{
				BufferPage		page;
				BufferPointer	address;
			};
		};

		struct DeadlockInfo
		{
			typedef collections::List<Ptr<DeadlockInfo>>	List;

			collections::List<BufferTransaction>	involvedTransactions;
			BufferTransaction						rollbackTransaction;
			WString									debugMessage;
		};

		class LockManager : public Object
		{
		public:
			LockManager();
			~LockManager();

			bool			RegisterTable(BufferTable table);
			bool			UnregisterTable(BufferTable table);
			bool			RegisterTransaction(BufferTransaction trans, vuint64_t importance);
			bool			UnregisterTransaction(BufferTransaction trans);
			bool			AcquireLock(const LockOwner& owner, const LockTarget& target, bool& blocked);
			bool			ReleaseLock(const LockOwner& owner, const LockTarget& target);
			BufferTask		PickTask();
			void			DetectDeadlock(DeadlockInfo::List& infos);
		};
	}
}

#endif
