#include "Lock.h"

namespace vl
{
	namespace database
	{
/***********************************************************************
LockManager
***********************************************************************/

		template<typename TInfo>
		bool LockManager::AcquireObjectLock(
			Ptr<TInfo> lockInfo,
			const LockOwner& owner,
			LockTargetAccess access,
			LockResult& result
			)
		{
			switch (access)
			{
			case LockTargetAccess::Shared:
				{
					if (lockInfo->xWriteOwner.transaction.IsValid())
					{
						result.blocked = true;
						return true;
					}
					vint index = lockInfo->sharedOwner.Keys().IndexOf(owner.transaction.index);
					if (index != -1)
					{
						if (lockInfo->sharedOwner.GetByIndex(index).Contains(owner.task.index))
						{
							return false;
						}
					}
					lockInfo->sharedOwner.Add(owner.transaction.index, owner.task.index);
					result.blocked = false;
					return true;
				}
				break;
			case LockTargetAccess::Exclusive:
				{
					if (lockInfo->xWriteOwner.transaction.IsValid())
					{
						result.blocked = true;
					}
					if (lockInfo->sharedOwner.Count() > 0)
					{
						return false;
					}
					lockInfo->xWriteOwner = owner;
					return true;
				}
				break;
			}
			return false;
		}

		template<typename TInfo>
		bool LockManager::ReleaseObjectLock(
			Ptr<TInfo> lockInfo,
			const LockOwner& owner,
			LockTargetAccess access,
			const LockResult& result
			)
		{
			switch (access)
			{
			case LockTargetAccess::Shared:
				{
					vint index = lockInfo->sharedOwner.Keys().IndexOf(owner.transaction.index);
					if (index == -1)
					{
						return false;
					}
					return lockInfo->sharedOwner.Remove(owner.transaction.index, owner.task.index);
				}
				break;
			case LockTargetAccess::Exclusive:
				{
					if (lockInfo->xWriteOwner.transaction.index != owner.transaction.index ||
						lockInfo->xWriteOwner.task.index != owner.task.index)
					{
						return false;
					}
					lockInfo->xWriteOwner = LockOwner();
					return true;
				}
				break;
			}
			return false;
		}
		
		bool LockManager::CheckInput(const LockOwner& owner, const LockTarget& target)
		{
			if (!owner.transaction.IsValid()) return false;
			if (!owner.task.IsValid()) return false;
			if (!target.table.IsValid()) return false;
			switch (target.type)
			{
			case LockTargetType::Page:
				if (!target.page.IsValid()) return false;
				break;
			case LockTargetType::Row:
				if (!target.address.IsValid()) return false;
				break;
			}

			if (!transactions.Keys().Contains(owner.transaction.index))
			{
				return false;
			}
			if (!tables.Keys().Contains(target.table.index))
			{
				return false;
			}

			return true;
		}

		LockManager::LockManager(BufferManager* _bm)
			:bm(_bm)
		{
		}

		LockManager::~LockManager()
		{
		}

		bool LockManager::RegisterTable(BufferTable table, BufferSource source)
		{
			SPIN_LOCK(lock)
			{
				if (tables.Keys().Contains(table.index))
				{
					return false;
				}

				if (!bm->GetIndexPage(source).IsValid())
				{
					return false;
				}

				auto info = MakePtr<TableInfo>();
				info->table = table;
				info->source = source;
				tables.Add(table.index, info);
			}
			return true;
		}

		bool LockManager::UnregisterTable(BufferTable table)
		{
			SPIN_LOCK(lock)
			{
				if (!tables.Keys().Contains(table.index))
				{
					return false;
				}

				tables.Remove(table.index);
			}
			return true;
		}

		bool LockManager::RegisterTransaction(BufferTransaction trans, vuint64_t importance)
		{
			SPIN_LOCK(lock)
			{
				if (transactions.Keys().Contains(trans.index))
				{
					return false;
				}

				auto info = MakePtr<TransInfo>();
				info->trans = trans;
				info->importance = importance;
				transactions.Add(trans.index, info);
			}
			return true;
		}

		bool LockManager::UnregisterTransaction(BufferTransaction trans)
		{
			SPIN_LOCK(lock)
			{
				if (!transactions.Keys().Contains(trans.index))
				{
					return false;
				}

				transactions.Remove(trans.index);
			}
			return true;
		}

		bool LockManager::AcquireLock(const LockOwner& owner, const LockTarget& target, LockResult& result)
		{
			if (!CheckInput(owner, target)) return false;
			Ptr<TableLockInfo> tableLockInfo;

			SPIN_LOCK(lock)
			{
				if (tableLocks.Count() <= target.table.index)
				{
					tableLocks.Resize(target.table.index + 1);
				}

				tableLockInfo = tableLocks[target.table.index];
				if (!tableLockInfo)
				{
					tableLockInfo = new TableLockInfo(target.table);
					tableLocks[target.table.index] = tableLockInfo;
				}
			}

			SPIN_LOCK(tableLockInfo->lock)
			{
				switch (target.type)
				{
				case LockTargetType::Table:
					return AcquireObjectLock(tableLockInfo, owner, target.access, result);
				case LockTargetType::Page:
					case LockTargetType::Row:
					return false;
				}
			}

			return false;
		}

		bool LockManager::ReleaseLock(const LockOwner& owner, const LockTarget& target, const LockResult& result)
		{
			if (!CheckInput(owner, target)) return false;
			Ptr<TableLockInfo> tableLockInfo;
			SPIN_LOCK(lock)
			{
				if (tableLocks.Count() <= target.table.index)
				{
					return false;
				}

				tableLockInfo = tableLocks[target.table.index];
				if (!tableLockInfo)
				{
					return false;
				}
			}

			SPIN_LOCK(tableLockInfo->lock)
			{
				switch (target.type)
				{
				case LockTargetType::Table:
					return ReleaseObjectLock(tableLockInfo, owner, target.access, result);
				case LockTargetType::Page:
				case LockTargetType::Row:
					return false;
				}
			}
			return false;
		}

		BufferTask LockManager::PickTask(LockResult& result)
		{
			return BufferTask::Invalid();
		}

		void LockManager::DetectDeadlock(DeadlockInfo::List& infos)
		{
		}

		bool LockManager::Rollback(BufferTransaction trans)
		{
			return false;
		}
	}
}
