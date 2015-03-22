#include "Lock.h"

namespace vl
{
	namespace database
	{
/***********************************************************************
LockManager
***********************************************************************/

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

			Ptr<TableLockInfo> tableLockInfo;
			SPIN_LOCK(lock)
			{
				if (!transactions.Keys().Contains(owner.transaction.index))
				{
					return false;
				}
				if (!tables.Keys().Contains(target.table.index))
				{
					return false;
				}

				if (tableLocks.Count() <= target.table.index)
				{
					tableLocks.Resize(target.table.index + 1);
				}

				tableLockInfo = tableLocks[target.table.index];
				if (!tableLockInfo)
				{
					tableLockInfo = new TableLockInfo;
					tableLockInfo->table = target.table;
					tableLocks[target.table.index] = tableLockInfo;
				}
			}

			SPIN_LOCK(tableLockInfo->lock)
			{
				switch (target.type)
				{
				case LockTargetType::Table:
					{
						switch (target.access)
						{
						case LockTargetAccess::Shared:
							{
								if (tableLockInfo->tableExclusiveOwner.transaction.IsValid())
								{
									return false;
								}
								vint index = tableLockInfo->tableSharedOwner.Keys().IndexOf(owner.transaction.index);
								if (index != -1)
								{
									if (tableLockInfo->tableSharedOwner.GetByIndex(index).Contains(owner.task.index))
									{
										return false;
									}
								}
								tableLockInfo->tableSharedOwner.Add(owner.transaction.index, owner.task.index);
								return true;
							}
							break;
						case LockTargetAccess::Exclusive:
							{
								if (tableLockInfo->tableExclusiveOwner.transaction.IsValid())
								{
									return false;
								}
								if (tableLockInfo->tableSharedOwner.Count() > 0)
								{
									return false;
								}
								tableLockInfo->tableExclusiveOwner = owner;
								return true;
							}
							break;
						}
					}
					break;
				case LockTargetType::Page:
				case LockTargetType::Row:
					return false;
				}
			}
			return false;
		}

		bool LockManager::ReleaseLock(const LockOwner& owner, const LockTarget& target, const LockResult& result)
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

			Ptr<TableLockInfo> tableLockInfo;
			SPIN_LOCK(lock)
			{
				if (!transactions.Keys().Contains(owner.transaction.index))
				{
					return false;
				}
				if (!tables.Keys().Contains(target.table.index))
				{
					return false;
				}

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
					{
						switch (target.access)
						{
						case LockTargetAccess::Shared:
							{
								vint index = tableLockInfo->tableSharedOwner.Keys().IndexOf(owner.transaction.index);
								if (index == -1)
								{
									return false;
								}
								return tableLockInfo->tableSharedOwner.Remove(owner.transaction.index, owner.task.index);
							}
							break;
						case LockTargetAccess::Exclusive:
							{
								if (tableLockInfo->tableExclusiveOwner.transaction.index != owner.transaction.index ||
									tableLockInfo->tableExclusiveOwner.task.index != owner.task.index)
								{
									return false;
								}
								tableLockInfo->tableExclusiveOwner = LockOwner();
								return true;
							}
							break;
						}
					}
					break;
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
