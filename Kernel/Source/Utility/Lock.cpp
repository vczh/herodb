#include "Lock.h"

namespace vl
{
	namespace database
	{
		using namespace collections;
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
					vint index = lockInfo->sharedOwners.Keys().IndexOf(owner.transaction.index);
					if (index != -1)
					{
						if (lockInfo->sharedOwners.GetByIndex(index).Contains(owner.task.index))
						{
							return false;
						}
					}

					if (lockInfo->exclusiveOwner.transaction.IsValid())
					{
						result.blocked = true;
						return true;
					}
					lockInfo->sharedOwners.Add(owner.transaction.index, owner.task.index);
					result.blocked = false;
					return true;
				}
				break;
			case LockTargetAccess::Exclusive:
				{
					if (lockInfo->exclusiveOwner.transaction.IsValid())
					{
						if (lockInfo->exclusiveOwner == owner)
						{
							return false;
						}
					}

					if (lockInfo->exclusiveOwner.transaction.IsValid() ||
						lockInfo->sharedOwners.Count() > 0)
					{
						result.blocked = true;
						return true;
					}
					lockInfo->exclusiveOwner = owner;
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
			LockTargetAccess access
			)
		{
			switch (access)
			{
			case LockTargetAccess::Shared:
				{
					vint index = lockInfo->sharedOwners.Keys().IndexOf(owner.transaction.index);
					if (index == -1)
					{
						return false;
					}
					return lockInfo->sharedOwners.Remove(owner.transaction.index, owner.task.index);
				}
				break;
			case LockTargetAccess::Exclusive:
				{
					if (lockInfo->exclusiveOwner != owner)
					{
						return false;
					}
					lockInfo->exclusiveOwner = LockOwner();
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


		bool LockManager::AddPendingLock(const LockOwner& owner, const LockTarget& target)
		{
			vint index = pendingLocks.Keys().IndexOf(owner.transaction.index);
			if (index !=-1)
			{
				FOREACH(Ptr<PendingLockInfo>, info, pendingLocks.GetByIndex(index))
				{
					if (info->owner == owner && info->target == target)
					{
						return false;
					}
				}
			}

			pendingLocks.Add(owner.transaction.index, new PendingLockInfo(owner, target));
			return true;
		}

		bool LockManager::RemovePendingLock(const LockOwner& owner, const LockTarget& target)
		{
			vint index = pendingLocks.Keys().IndexOf(owner.transaction.index);
			if (index != -1)
			{
				FOREACH(Ptr<PendingLockInfo>, info, pendingLocks.GetByIndex(index))
				{
					if (info->owner == owner && info->target == target)
					{
						pendingLocks.Remove(owner.transaction.index, info.Obj());
						return true;
					}
				}	
			}
			return false;
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
			Ptr<PageLockInfo> pageLockInfo;

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
					if (!AcquireObjectLock(tableLockInfo, owner, target.access, result))
					{
						return false;
					}
					if (result.blocked && !AddPendingLock(owner, target))
					{
						return false;
					}
					return true;
				case LockTargetType::Page:
					{
						vint index = tableLockInfo->pageLocks.Keys().IndexOf(target.page.index);
						if (index == -1)
						{
							pageLockInfo = new PageLockInfo(target.page);
							tableLockInfo->pageLocks.Add(target.page.index, pageLockInfo);
						}
						else
						{
							pageLockInfo = tableLockInfo->pageLocks.Values()[index];
						}
					}
					break;
				case LockTargetType::Row:
					return false;
				}
			}

			SPIN_LOCK(pageLockInfo->lock)
			{
				switch (target.type)
				{
				case LockTargetType::Page:
					{
						bool acquired = AcquireObjectLock(pageLockInfo, owner, target.access, result);
						if (!acquired)
						{
							return false;
						}
						else if (result.blocked)
						{
							return AddPendingLock(owner, target);
						}
						else
						{
							return true;
						}
					}
					return true;
				case LockTargetType::Row:
					return false;
				}
			}

			return false;
		}

		bool LockManager::ReleaseLock(const LockOwner& owner, const LockTarget& target)
		{
			if (!CheckInput(owner, target)) return false;
			Ptr<TableLockInfo> tableLockInfo;
			Ptr<PageLockInfo> pageLockInfo;
			
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
					return ReleaseObjectLock(tableLockInfo, owner, target.access) || RemovePendingLock(owner, target);
				case LockTargetType::Page:
					{
						vint index = tableLockInfo->pageLocks.Keys().IndexOf(target.page.index);
						if (index == -1)
						{
							return false;
						}
						pageLockInfo = tableLockInfo->pageLocks.Values()[index];
					}
					break;
				case LockTargetType::Row:
					return false;
				}
			}

			SPIN_LOCK(pageLockInfo->lock)
			{
				switch (target.type)
				{
				case LockTargetType::Page:
					if(ReleaseObjectLock(pageLockInfo, owner, target.access))
					{
						return true;
					}
					else
					{
						return RemovePendingLock(owner, target);
					}
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
