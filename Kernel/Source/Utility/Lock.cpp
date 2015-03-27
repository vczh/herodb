#include "Lock.h"

namespace vl
{
	namespace database
	{
		using namespace collections;

#define LOCK_TYPES ((vint)LockTargetAccess::NumbersOfLockTypes)

/***********************************************************************
LockManager (ObjectLock)
***********************************************************************/

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

		template<typename TInfo>
		bool LockManager::AcquireObjectLock(
			Ptr<TInfo> lockInfo,
			BufferTransaction owner,
			LockTargetAccess access
			)
		{
			for(vint i = 0; i < LOCK_TYPES; i++)
			{
				if (lockCompatibility[(vint)access][i] == false)
				{
					const auto& owners = lockInfo->owners[i];
					if (owners.Count() > 0)
					{
						return false;
					}
				}
			}

			auto& owners = lockInfo->owners[(vint)access];
			owners.Add(owner);
			return true;
		}

		template<typename TInfo>
		bool LockManager::ReleaseObjectLock(
			Ptr<TInfo> lockInfo,
			BufferTransaction owner,
			LockTargetAccess access
			)
		{
			auto& owners = lockInfo->owners[(vint)access];
			return owners.Remove(owner);
		}
		
		bool LockManager::CheckInput(BufferTransaction owner, const LockTarget& target)
		{
			if (!owner.IsValid()) return false;
			if (!target.table.IsValid()) return false;
			switch (target.type)
			{
			case LockTargetType::Page:
				if (!target.page.IsValid()) return false;
				break;
			case LockTargetType::Row:
				if (!target.address.IsValid()) return false;
				break;
			default:;
			}

			if (!transactions.Keys().Contains(owner))
			{
				return false;
			}
			if (!tables.Keys().Contains(target.table))
			{
				return false;
			}

			return true;
		}


		bool LockManager::AddPendingLock(BufferTransaction owner, const LockTarget& target)
		{
			vint index = pendingLocks.Keys().IndexOf(owner);
			if (index != -1)
			{
				if (pendingLocks.Values()[index] == target)
				{
					return false;
				}
			}

			pendingLocks.Add(owner, target);
			return true;
		}

		bool LockManager::RemovePendingLock(BufferTransaction owner, const LockTarget& target)
		{
			vint index = pendingLocks.Keys().IndexOf(owner);
			if (index != -1)
			{
				if (pendingLocks.Values()[index] == target)
				{
					pendingLocks.Remove(owner);
					return true;
				}
			}
			return false;
		}

/***********************************************************************
LockManager (Template)
***********************************************************************/

		bool LockManager::OperateObjectLock(
			BufferTransaction owner,
			const LockTarget& target,
			LockResult& result,
			TableLockHandler tableLockHandler,
			PageLockHandler pageLockHandler,
			RowLockHandler rowLockHandler,
			bool createLockInfo
			)
		{
			if (!CheckInput(owner, target)) return false;
			if (pendingLocks.Keys().Contains(owner)) return false;
			Ptr<TableLockInfo> tableLockInfo;
			Ptr<PageLockInfo> pageLockInfo;
			Ptr<RowLockInfo> rowLockInfo;
			BufferPage targetPage;
			vuint64_t targetOffset = ~(vuint64_t)0;

			SPIN_LOCK(lock)
			{
				if (tableLocks.Count() <= target.table.index)
				{
					if (!createLockInfo)
					{
						return false;
					}
					tableLocks.Resize(target.table.index + 1);
				}

				tableLockInfo = tableLocks[target.table.index];
				if (!tableLockInfo)
				{
					if (!createLockInfo)
					{
						return false;
					}
					tableLockInfo = new TableLockInfo(target.table);
					tableLocks[target.table.index] = tableLockInfo;
				}
			}

			SPIN_LOCK(tableLockInfo->lock)
			{
				switch (target.type)
				{
				case LockTargetType::Table:
					return (this->*tableLockHandler)(owner, target, result, tableLockInfo);
				case LockTargetType::Page:
					targetPage = target.page;
					break;
				case LockTargetType::Row:
					CHECK_ERROR(bm->DecodePointer(target.address, targetPage, targetOffset), L"vl::database::LockManager::AcquireLock(BufferTransaction, const LockTarget&, LockResult&)#Internal error: Unable to decode row pointer.");
					break;
				}

				vint index = tableLockInfo->pageLocks.Keys().IndexOf(targetPage);
				if (index == -1)
				{
					if (!createLockInfo)
					{
						return false;
					}
					pageLockInfo = new PageLockInfo(target.page);
					tableLockInfo->pageLocks.Add(targetPage, pageLockInfo);
				}
				else
				{
					pageLockInfo = tableLockInfo->pageLocks.Values()[index];
				}
				if (createLockInfo)
				{
					pageLockInfo->IncIntent();
				}
			}

			SPIN_LOCK(pageLockInfo->lock)
			{
				switch (target.type)
				{
				case LockTargetType::Page:
					{
						bool success = (this->*pageLockHandler)(owner, target, result, pageLockInfo);
						if (createLockInfo)
						{
							pageLockInfo->DecIntent();
						}
						return success;
					}
				case LockTargetType::Row:
					{
						vint index = pageLockInfo->rowLocks.Keys().IndexOf(targetOffset);
						if (index == -1)
						{
							if (!createLockInfo)
							{
								return false;
							}
							rowLockInfo = new RowLockInfo(targetOffset);
							pageLockInfo->rowLocks.Add(targetOffset, rowLockInfo);
						}
						else
						{
							rowLockInfo = pageLockInfo->rowLocks.Values()[index];
						}
						if (createLockInfo)
						{
							pageLockInfo->DecIntent();
							rowLockInfo->IncIntent();
						}
					}
					break;
				default:;
				}
			}

			SPIN_LOCK(rowLockInfo->lock)
			{
				switch (target.type)
				{
				case LockTargetType::Row:
					{
						bool success = (this->*rowLockHandler)(owner, target, result, rowLockInfo);
						if (createLockInfo)
						{
							rowLockInfo->DecIntent();
						}
						return success;
					}
				default:;
				}
			}

			return false;
		}

/***********************************************************************
LockManager (Acquire)
***********************************************************************/

		bool LockManager::AcquireTableLock(BufferTransaction owner, const LockTarget& target, LockResult& result, Ptr<TableLockInfo> lockInfo)
		{
			return false;
		}

		bool LockManager::AcquireRowLock(BufferTransaction owner, const LockTarget& target, LockResult& result, Ptr<TableLockInfo> lockInfo)
		{
			return false;
		}

		bool LockManager::AcquirePageLock(BufferTransaction owner, const LockTarget& target, LockResult& result, Ptr<TableLockInfo> lockInfo)
		{
			return false;
		}

/***********************************************************************
LockManager (Release)
***********************************************************************/

		bool LockManager::ReleaseTableLock(BufferTransaction owner, const LockTarget& target, LockResult& result, Ptr<TableLockInfo> lockInfo)
		{
			return false;
		}

		bool LockManager::ReleaseRowLock(BufferTransaction owner, const LockTarget& target, LockResult& result, Ptr<TableLockInfo> lockInfo)
		{
			return false;
		}

		bool LockManager::ReleasePageLock(BufferTransaction owner, const LockTarget& target, LockResult& result, Ptr<TableLockInfo> lockInfo)
		{
			return false;
		}

/***********************************************************************
LockManager (Upgrade)
***********************************************************************/

		bool LockManager::UpgradeTableLock(BufferTransaction owner, const LockTarget& target, LockResult& result, Ptr<TableLockInfo> lockInfo)
		{
			return false;
		}

		bool LockManager::UpgradeRowLock(BufferTransaction owner, const LockTarget& target, LockResult& result, Ptr<TableLockInfo> lockInfo)
		{
			return false;
		}

		bool LockManager::UpgradePageLock(BufferTransaction owner, const LockTarget& target, LockResult& result, Ptr<TableLockInfo> lockInfo)
		{
			return false;
		}

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
				if (tables.Keys().Contains(table))
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
				tables.Add(table, info);
			}
			return true;
		}

		bool LockManager::UnregisterTable(BufferTable table)
		{
			SPIN_LOCK(lock)
			{
				if (!tables.Keys().Contains(table))
				{
					return false;
				}

				tables.Remove(table);
			}
			return true;
		}

		bool LockManager::RegisterTransaction(BufferTransaction trans, vuint64_t importance)
		{
			SPIN_LOCK(lock)
			{
				if (transactions.Keys().Contains(trans))
				{
					return false;
				}

				auto info = MakePtr<TransInfo>();
				info->trans = trans;
				info->importance = importance;
				transactions.Add(trans, info);
			}
			return true;
		}

		bool LockManager::UnregisterTransaction(BufferTransaction trans)
		{
			SPIN_LOCK(lock)
			{
				if (!transactions.Keys().Contains(trans))
				{
					return false;
				}

				transactions.Remove(trans);
			}
			return true;
		}

		bool LockManager::AcquireLock(BufferTransaction owner, const LockTarget& target, LockResult& result)
		{
			if (!CheckInput(owner, target)) return false;
			if (pendingLocks.Keys().Contains(owner)) return false;
			Ptr<TableLockInfo> tableLockInfo;
			Ptr<PageLockInfo> pageLockInfo;
			Ptr<RowLockInfo> rowLockInfo;
			BufferPage targetPage;
			vuint64_t targetOffset = ~(vuint64_t)0;

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
					if (AcquireObjectLock(tableLockInfo, owner, target.access))
					{
						result.blocked = false;
						return true;
					}
					else
					{
						result.blocked = true;
						return AddPendingLock(owner, target);
					}
				case LockTargetType::Page:
					targetPage = target.page;
					break;
				case LockTargetType::Row:
					CHECK_ERROR(bm->DecodePointer(target.address, targetPage, targetOffset), L"vl::database::LockManager::AcquireLock(BufferTransaction, const LockTarget&, LockResult&)#Internal error: Unable to decode row pointer.");
					break;
				}

				vint index = tableLockInfo->pageLocks.Keys().IndexOf(targetPage);
				if (index == -1)
				{
					pageLockInfo = new PageLockInfo(target.page);
					tableLockInfo->pageLocks.Add(targetPage, pageLockInfo);
				}
				else
				{
					pageLockInfo = tableLockInfo->pageLocks.Values()[index];
				}
				pageLockInfo->IncIntent();
			}

			SPIN_LOCK(pageLockInfo->lock)
			{
				switch (target.type)
				{
				case LockTargetType::Page:
					{
						bool success = true;
						if (AcquireObjectLock(pageLockInfo, owner, target.access))
						{
							result.blocked = false;
						}
						else
						{
							result.blocked = true;
							success = AddPendingLock(owner, target);
						}
						pageLockInfo->DecIntent();
						return success;
					}
				case LockTargetType::Row:
					{
						vint index = pageLockInfo->rowLocks.Keys().IndexOf(targetOffset);
						if (index == -1)
						{
							rowLockInfo = new RowLockInfo(targetOffset);
							pageLockInfo->rowLocks.Add(targetOffset, rowLockInfo);
						}
						else
						{
							rowLockInfo = pageLockInfo->rowLocks.Values()[index];
						}
						pageLockInfo->DecIntent();
						rowLockInfo->IncIntent();
					}
					break;
				default:;
				}
			}

			SPIN_LOCK(rowLockInfo->lock)
			{
				switch (target.type)
				{
				case LockTargetType::Row:
					{
						bool success = true;
						if (AcquireObjectLock(rowLockInfo, owner, target.access))
						{
							result.blocked = false;
						}
						else
						{
							result.blocked = true;
							success = AddPendingLock(owner, target);
						}
						rowLockInfo->DecIntent();
						return success;
					}
				default:;
				}
			}

			return false;
		}

		bool LockManager::ReleaseLock(BufferTransaction owner, const LockTarget& target)
		{
			if (!CheckInput(owner, target)) return false;
			Ptr<TableLockInfo> tableLockInfo;
			Ptr<PageLockInfo> pageLockInfo;
			Ptr<RowLockInfo> rowLockInfo;
			BufferPage targetPage;
			vuint64_t targetOffset = ~(vuint64_t)0;
			
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
					if (ReleaseObjectLock(tableLockInfo, owner, target.access))
					{
						return true;
					}
					else
					{
						return RemovePendingLock(owner, target);
					}
				case LockTargetType::Page:
					targetPage = target.page;
					break;
				case LockTargetType::Row:
					CHECK_ERROR(bm->DecodePointer(target.address, targetPage, targetOffset), L"vl::database::LockManager::AcquireLock(BufferTransaction, const LockTarget&, LockResult&)#Internal error: Unable to decode row pointer.");
					break;
				};

				vint index = tableLockInfo->pageLocks.Keys().IndexOf(targetPage);
				if (index == -1)
				{
					return false;
				}
				pageLockInfo = tableLockInfo->pageLocks.Values()[index];
			}

			SPIN_LOCK(pageLockInfo->lock)
			{
				switch (target.type)
				{
				case LockTargetType::Page:
					{
						bool success = true;
						if (!ReleaseObjectLock(pageLockInfo, owner, target.access))
						{
							success = RemovePendingLock(owner, target);
						}

						if (pageLockInfo->IsEmpty())
						{
							SPIN_LOCK(tableLockInfo->lock)
							{
								if (!pageLockInfo->IntentedToAcquire())
								{
									tableLockInfo->pageLocks.Remove(targetPage);
								}
							}
						}

						return success;
					}
				case LockTargetType::Row:
					{
						vint index = pageLockInfo->rowLocks.Keys().IndexOf(targetOffset);
						if (index == -1)
						{
							return false;
						}
						rowLockInfo = pageLockInfo->rowLocks.Values()[index];
					}
					break;
				default:;
				}
			}

			SPIN_LOCK(rowLockInfo->lock)
			{
				switch (target.type)
				{
				case LockTargetType::Row:
					{
						bool success = true;
						if (!ReleaseObjectLock(rowLockInfo, owner, target.access))
						{
							success = RemovePendingLock(owner, target);
						}

						if (rowLockInfo->IsEmpty())
						{
							SPIN_LOCK(pageLockInfo->lock)
							{
								if (!rowLockInfo->IntentedToAcquire())
								{
									pageLockInfo->rowLocks.Remove(targetOffset);
								}

								if (pageLockInfo->IsEmpty())
								{
									SPIN_LOCK(tableLockInfo->lock)
									{
										if (!pageLockInfo->IntentedToAcquire())
										{
											tableLockInfo->pageLocks.Remove(targetPage);
										}
									}
								}
							}
						}

						return success;
					}
				default:;
				}
			}

			return false;
		}

		bool LockManager::UpgradeLock(BufferTransaction owner, const LockTarget& oldTarget, LockTargetAccess newAccess, LockResult& result)
		{
			return false;
		}

		bool LockManager::TableHasLocks(BufferTable table)
		{
			if (!table.IsValid()) return false;

			Ptr<TableLockInfo> tableLockInfo;
			SPIN_LOCK(lock)
			{
				if (tableLocks.Count() <= table.index)
				{
					return false;
				}
				tableLockInfo = tableLocks[table.index];
			}

			SPIN_LOCK(tableLockInfo->lock)
			{
				return tableLockInfo->IntentedToAcquire() || !tableLockInfo->IsEmpty();
			}
			return false;
		}

		BufferTransaction LockManager::PickTransaction(LockResult& result)
		{
			return BufferTransaction::Invalid();
		}

		void LockManager::DetectDeadlock(DeadlockInfo::List& infos)
		{
		}

		bool LockManager::Rollback(BufferTransaction trans)
		{
			return false;
		}
		
#undef LOCK_TYPES
	}
}
