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
		bool LockManager::AcquireObjectLockUnsafe(
			Ptr<TInfo> lockInfo,
			Ptr<TransInfo> owner,
			const LockTarget& target
			)
		{
			vint access = (vint)target.access;
			for(vint i = 0; i < LOCK_TYPES; i++)
			{
				if (lockCompatibility[access][i] == false)
				{
					if (lockInfo->acquiredLocks[i] > 0)
					{
						return false;
					}
				}
			}

			lockInfo->acquiredLocks[access]++;
			owner->acquiredLocks.Add(target);
			return true;
		}

		template<typename TInfo>
		bool LockManager::ReleaseObjectLockUnsafe(
			Ptr<TInfo> lockInfo,
			Ptr<TransInfo> owner,
			const LockTarget& target
			)
		{
			auto index = owner->acquiredLocks.IndexOf(target);
			if (index < 0)
			{
				return false;
			}

			auto result = --lockInfo->acquiredLocks[(vint)target.access];
			CHECK_ERROR(result >= 0, L"vl::database::LockManager::ReleaseObjectLockUnsafe(Ptr<TInfo>, Ptr<TransInfo>, const LockTarget&)#Internal error: TInfo::acquiredLocks is corrupted.");
			return owner->acquiredLocks.RemoveAt(index);
		}
		
		Ptr<LockManager::TransInfo> LockManager::CheckInputUnsafe(BufferTransaction owner, const LockTarget& target)
		{
			if (!owner.IsValid()) return nullptr;
			if (!target.table.IsValid()) return nullptr;
			switch (target.type)
			{
			case LockTargetType::Page:
				if (!target.page.IsValid()) return nullptr;
				break;
			case LockTargetType::Row:
				if (!target.address.IsValid()) return nullptr;
				break;
			default:;
			}

			if (!tables.Keys().Contains(target.table))
			{
				return nullptr;
			}

			vint index = transactions.Keys().IndexOf(owner);
			if (index == -1)
			{
				return nullptr;
			}

			return transactions.Values()[index];;
		}


		bool LockManager::AddPendingLockUnsafe(Ptr<TransInfo> owner, const LockTarget& target)
		{
			if (owner->pendingLock.IsValid())
			{
				return false;
			}

			Ptr<PendingInfo> pendingInfo;
			vint index = pendings.Keys().IndexOf(owner->importance);
			if (index == -1)
			{
				pendingInfo = new PendingInfo;
				pendings.Add(owner->importance, pendingInfo);
			}
			else
			{
				pendingInfo = pendings.Values()[index];
			}

			index = pendingInfo->transactions.IndexOf(owner->trans);
			if (index != -1)
			{
				return false;
			}
			pendingInfo->transactions.Add(owner->trans);
			owner->pendingLock = target;
			return true;
		}

		bool LockManager::RemovePendingLockUnsafe(Ptr<TransInfo> owner, const LockTarget& target)
		{
			if (!owner->pendingLock.IsValid() || owner->pendingLock != target)
			{
				return false;
			}

			vint index = pendings.Keys().IndexOf(owner->importance);
			if (index == -1)
			{
				return false;
			}
			auto pendingInfo = pendings.Values()[index];

			index = pendingInfo->transactions.IndexOf(owner->trans);
			if (index == -1)
			{
				return false;
			}

			pendingInfo->transactions.RemoveAt(index);
			owner->pendingLock = LockTarget();
			return true;
		}

/***********************************************************************
LockManager (Template)
***********************************************************************/

		const LockTarget& GetLockTarget(const LockTarget& target)
		{
			return target;
		}

		template<typename... T>
		const LockTarget& GetLockTarget(Tuple<const LockTarget&, T...> arguments)
		{
			return arguments.f0;
		}

		template<typename TArgs>
		bool LockManager::OperateObjectLock(
			BufferTransaction owner,
			TArgs arguments,
			TableLockHandler<TArgs> tableLockHandler,
			PageLockHandler<TArgs> pageLockHandler,
			RowLockHandler<TArgs> rowLockHandler,
			bool createLockInfo,
			bool checkPendingLock
			)
		{
			const LockTarget& target = GetLockTarget(arguments);
			Ptr<TableLockInfo> tableLockInfo;
			Ptr<PageLockInfo> pageLockInfo;
			Ptr<RowLockInfo> rowLockInfo;
			BufferPage targetPage;
			vuint64_t targetOffset = ~(vuint64_t)0;
			vint index = -1;

			///////////////////////////////////////////////////////////
			// Check Input
			///////////////////////////////////////////////////////////

			auto transInfo = CheckInputUnsafe(owner, target);
			if (!transInfo) return false;
			if (checkPendingLock && transInfo->pendingLock.IsValid())
			{
				return false;
			}

			///////////////////////////////////////////////////////////
			// Find TableLock
			///////////////////////////////////////////////////////////

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

			///////////////////////////////////////////////////////////
			// Process TableLock
			///////////////////////////////////////////////////////////

			switch (target.type)
			{
			case LockTargetType::Table:
				return (this->*tableLockHandler)(transInfo, arguments, tableLockInfo);
			case LockTargetType::Page:
				targetPage = target.page;
				break;
			case LockTargetType::Row:
				CHECK_ERROR(bm->DecodePointer(target.address, targetPage, targetOffset), L"vl::database::LockManager::OperateObjectLock(BufferTransaction, const LockTarget&, LockResult&)#Internal error: Unable to decode row pointer.");
				break;
			}

			///////////////////////////////////////////////////////////
			// Find PageLock
			///////////////////////////////////////////////////////////

			index = tableLockInfo->pageLocks.Keys().IndexOf(targetPage);
			if (index == -1)
			{
				if (!createLockInfo)
				{
					return false;
				}
				pageLockInfo = new PageLockInfo(targetPage);
				tableLockInfo->pageLocks.Add(targetPage, pageLockInfo);
			}
			else
			{
				pageLockInfo = tableLockInfo->pageLocks.Values()[index];
			}

			///////////////////////////////////////////////////////////
			// Process PageLock
			///////////////////////////////////////////////////////////

			if (target.type == LockTargetType::Page)
			{
				return (this->*pageLockHandler)(transInfo, arguments, tableLockInfo, pageLockInfo);
			}

			///////////////////////////////////////////////////////////
			// Find RowLock
			///////////////////////////////////////////////////////////

			index = pageLockInfo->rowLocks.Keys().IndexOf(targetOffset);
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

			///////////////////////////////////////////////////////////
			// Process RowLock
			///////////////////////////////////////////////////////////

			if (target.type == LockTargetType::Row)
			{
				return (this->*rowLockHandler)(transInfo, arguments, tableLockInfo, pageLockInfo, rowLockInfo);
			}

			return false;
		}

/***********************************************************************
LockManager (Acquire)
***********************************************************************/
		template<typename TLockInfo>
		bool LockManager::AcquireGeneralLock(Ptr<TransInfo> owner, AcquireLockArgs arguments, Ptr<TLockInfo> lockInfo)
		{
			const LockTarget& target = arguments.f0;
			LockResult& result = arguments.f1;
			bool addPendingLock = arguments.f2;

			if (AcquireObjectLockUnsafe(lockInfo, owner, target))
			{
				result.blocked = false;
				return true;
			}
			else
			{
				result.blocked = true;
				return !addPendingLock || AddPendingLockUnsafe(owner, target);
			}
		}

		bool LockManager::AcquireTableLock(Ptr<TransInfo> owner, AcquireLockArgs arguments, Ptr<TableLockInfo> tableLockInfo)
		{
			return AcquireGeneralLock(owner, arguments, tableLockInfo);
		}

		bool LockManager::AcquirePageLock(Ptr<TransInfo> owner, AcquireLockArgs arguments, Ptr<TableLockInfo> tableLockInfo, Ptr<PageLockInfo> pageLockInfo)
		{
			return AcquireGeneralLock(owner, arguments, pageLockInfo);
		}

		bool LockManager::AcquireRowLock(Ptr<TransInfo> owner, AcquireLockArgs arguments, Ptr<TableLockInfo> tableLockInfo, Ptr<PageLockInfo> pageLockInfo, Ptr<RowLockInfo> rowLockInfo)
		{
			return AcquireGeneralLock(owner, arguments, rowLockInfo);
		}

/***********************************************************************
LockManager (Release)
***********************************************************************/

		bool LockManager::ReleaseTableLock(Ptr<TransInfo> owner, ReleaseLockArgs arguments, Ptr<TableLockInfo> tableLockInfo)
		{
			const LockTarget& target = arguments;

			if (ReleaseObjectLockUnsafe(tableLockInfo, owner, target))
			{
				return true;
			}
			else
			{
				return RemovePendingLockUnsafe(owner, target);
			}
		}

		bool LockManager::ReleasePageLock(Ptr<TransInfo> owner, ReleaseLockArgs arguments, Ptr<TableLockInfo> tableLockInfo, Ptr<PageLockInfo> pageLockInfo)
		{
			const LockTarget& target = arguments;

			if (ReleaseObjectLockUnsafe(pageLockInfo, owner, target))
			{
				if (pageLockInfo->IsEmpty())
				{
					tableLockInfo->pageLocks.Remove(pageLockInfo->object);
				}
				return true;
			}
			else
			{
				return RemovePendingLockUnsafe(owner, target);
			}
		}

		bool LockManager::ReleaseRowLock(Ptr<TransInfo> owner, ReleaseLockArgs arguments, Ptr<TableLockInfo> tableLockInfo, Ptr<PageLockInfo> pageLockInfo, Ptr<RowLockInfo> rowLockInfo)
		{
			const LockTarget& target = arguments;

			bool success = true;
			if (ReleaseObjectLockUnsafe(rowLockInfo, owner, target))
			{
				if (rowLockInfo->IsEmpty())
				{
					pageLockInfo->rowLocks.Remove(rowLockInfo->object);
					if (pageLockInfo->IsEmpty())
					{
						tableLockInfo->pageLocks.Remove(pageLockInfo->object);
					}
				}
				return true;
			}
			else
			{
				return RemovePendingLockUnsafe(owner, target);
			}
		}

/***********************************************************************
LockManager (Upgrade)
***********************************************************************/

		template<typename TLockInfo>
		bool LockManager::UpgradeGeneralLock(Ptr<TransInfo> owner, UpgradeLockArgs arguments, Ptr<TLockInfo> lockInfo)
		{
			const LockTarget& oldTarget = arguments.f0;
			LockTargetAccess newAccess = arguments.f1;
			LockResult& result = arguments.f2;

			if (!ReleaseObjectLockUnsafe(lockInfo, owner, oldTarget))
			{
				return false;
			}
			else
			{
				LockTarget newTarget = oldTarget;
				newTarget.access = newAccess;
				AcquireLockArgs newArguments(newTarget, result, true);
				return AcquireGeneralLock(owner, newArguments, lockInfo);
			}
		}

		bool LockManager::UpgradeTableLock(Ptr<TransInfo> owner, UpgradeLockArgs arguments, Ptr<TableLockInfo> tableLockInfo)
		{
			return UpgradeGeneralLock(owner, arguments, tableLockInfo);
		}

		bool LockManager::UpgradePageLock(Ptr<TransInfo> owner, UpgradeLockArgs arguments, Ptr<TableLockInfo> tableLockInfo, Ptr<PageLockInfo> pageLockInfo)
		{
			return UpgradeGeneralLock(owner, arguments, pageLockInfo);
		}

		bool LockManager::UpgradeRowLock(Ptr<TransInfo> owner, UpgradeLockArgs arguments, Ptr<TableLockInfo> tableLockInfo, Ptr<PageLockInfo> pageLockInfo, Ptr<RowLockInfo> rowLockInfo)
		{
			return UpgradeGeneralLock(owner, arguments, rowLockInfo);
		}

/***********************************************************************
LockManager (ctor/dtor)
***********************************************************************/

		LockManager::LockManager(BufferManager* _bm)
			:bm(_bm)
		{
		}

		LockManager::~LockManager()
		{
		}

/***********************************************************************
LockManager (Registration)
***********************************************************************/

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
				auto index = transactions.Keys().IndexOf(trans);
				if (index == -1)
				{
					return false;
				}

				auto transInfo = transactions.Values()[index];
				if (transInfo->acquiredLocks.Count() > 0 || transInfo->pendingLock.IsValid())
				{
					return false;
				}
				if (!transactions.Keys().Contains(trans))
				{
					return false;
				}

				transactions.Remove(trans);
			}
			return true;
		}

/***********************************************************************
LockManager (Lock Operation)
***********************************************************************/

		bool LockManager::AcquireLock(BufferTransaction owner, const LockTarget& target, LockResult& result)
		{
			bool success = false;
			SPIN_LOCK(lock)
			{
				AcquireLockArgs arguments(target, result, true);
				success = OperateObjectLock<AcquireLockArgs>(
					owner,
					arguments,
					&LockManager::AcquireTableLock,
					&LockManager::AcquirePageLock,
					&LockManager::AcquireRowLock,
					true,
					true
					);
			}
			return success;
		}

		bool LockManager::ReleaseLock(BufferTransaction owner, const LockTarget& target)
		{
			bool success = false;
			SPIN_LOCK(lock)
			{
				ReleaseLockArgs arguments = target;
				LockResult result;
				success = OperateObjectLock<ReleaseLockArgs>(
					owner,
					arguments,
					&LockManager::ReleaseTableLock,
					&LockManager::ReleasePageLock,
					&LockManager::ReleaseRowLock,
					false,
					false
					);
			}
			return success;
		}

		bool LockManager::UpgradeLock(BufferTransaction owner, const LockTarget& oldTarget, LockTargetAccess newAccess, LockResult& result)
		{
			bool success = false;
			SPIN_LOCK(lock)
			{
				UpgradeLockArgs arguments(oldTarget, newAccess, result);
				success = OperateObjectLock<UpgradeLockArgs>(
					owner,
					arguments,
					&LockManager::UpgradeTableLock,
					&LockManager::UpgradePageLock,
					&LockManager::UpgradeRowLock,
					false,
					true
					);
			}
			return success;
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
				return tableLockInfo && !tableLockInfo->IsEmpty();
			}
			return false;
		}

/***********************************************************************
LockManager (Scheduler)
***********************************************************************/

		BufferTransaction LockManager::PickTransaction(LockResult& result)
		{
			SPIN_LOCK(lock)
			{
				for (vint i = pendings.Count() - 1; i >= 0; i--)
				{
					auto pendingInfo = pendings.Values()[i];
					auto stopIndex = pendingInfo->lastTryIndex;
					if (stopIndex == -1)
					{
						stopIndex = pendingInfo->transactions.Count() - 1;
					}

					do
					{
						pendingInfo->lastTryIndex = (pendingInfo->lastTryIndex + 1) % pendingInfo->transactions.Count();
						auto trans = pendingInfo->transactions[pendingInfo->lastTryIndex];
						auto transInfo = transactions[trans];
						CHECK_ERROR(transInfo->pendingLock.IsValid(), L"vl::database::LockManager::PickTransaction(LockResult&)#Internal error: Field pendings is corrupted.");

						AcquireLockArgs arguments(transInfo->pendingLock, result, false);
						bool success = OperateObjectLock<AcquireLockArgs>(
							transInfo->trans,
							arguments,
							&LockManager::AcquireTableLock,
							&LockManager::AcquirePageLock,
							&LockManager::AcquireRowLock,
							true,
							false
							);
						CHECK_ERROR(success, L"vl::database::LockManager::PickTransaction(LockResult&)#Internal error: Wrong arguments provided to acquire lock.");

						if (!result.blocked)
						{
							transInfo->pendingLock = LockTarget();
							pendingInfo->transactions.RemoveAt(pendingInfo->lastTryIndex);
							pendingInfo->lastTryIndex--;

							if (pendingInfo->transactions.Count() == 0)
							{
								pendings.Remove(pendings.Keys()[i]);
							}
							return transInfo->trans;
						}
					} while (pendingInfo->lastTryIndex != stopIndex);
				}
			}
			return BufferTransaction::Invalid();
		}

/***********************************************************************
LockManager (Deadlock)
***********************************************************************/

		class DeadlockDetection
		{
		public:
			struct Node
			{
				Ptr<LockManager::TransInfo>			transInfo;
				SortedList<vint>					acquired;
				SortedList<Node*>					ins;
				SortedList<Node*>					outs;

				Node*								previous = nullptr;
				vint								next = -1;
				bool								touched = false;
			};

			static void BuildGraph(LockManager* lm, List<Node*>& nodes)
			{
				FOREACH(Ptr<LockManager::PendingInfo>, pendingInfo, lm->pendings.Values())
				{
					FOREACH(BufferTransaction, trans, pendingInfo->transactions)
					{
						auto node = new Node;
						node->transInfo = lm->transactions[trans];
						nodes.Add(node);
					}
				}

				FOREACH(Node*, from, nodes)
				{
					auto pending = from->transInfo->pendingLock;
					auto access = pending.access;
					CHECK_ERROR(pending.IsValid(), L"vl::database::DeadlockDetection::BuildGraph(LockManager*, List<DeadlockDetection::Node*>&)#Internal error: Field pendings is corrupted.");
					FOREACH(Node*, to, nodes)
					{
						for (vint i = 0; i < to->transInfo->acquiredLocks.Count(); i++)
						{
							const auto& acquired = to->transInfo->acquiredLocks[i];
							pending.access = acquired.access;
							if (pending == acquired && !lockCompatibility[(vint)access][(vint)acquired.access])
							{
								if (!to->acquired.Contains(i))
								{
									to->acquired.Add(i);
								}
								from->outs.Add(to);
								to->ins.Add(from);
							}
						}
					}
				}
			}

			static void DeleteGraph(List<Node*>& nodes)
			{
				FOREACH(Node*, node, nodes)
				{
					delete node;
				}
			}

			static bool TestReducableNode(SortedList<Node*>& nodes, Node* node)
			{
				if (node->ins.Count() * node->outs.Count() == 0)
				{
					if (!nodes.Contains(node))
					{
						nodes.Add(node);
					}
					return true;
				}
				return false;
			}

			static void ReduceNode(SortedList<Node*>& nodes, Node* node, vint& index)
			{
				vint position = nodes.IndexOf(node);
				CHECK_ERROR(position >= 0, L"vl::database::DeadlockDetection::ReduceNode(SortedList<DeadlockDetection::Node*>&, DeadlockDetection::Node*, vint&)#Internal error: Deadlock graph corrupted.");
				nodes.RemoveAt(position);
				if (position < index)
				{
					index--;
				}
			}

			static void ReduceGraph(SortedList<Node*>& nodes)
			{
				vint index = 0;
				SortedList<Node*> affected;
				while (index < nodes.Count())
				{
					auto node = nodes[index++];
					if (TestReducableNode(affected, node))
					{
						while (affected.Count() > 0)
						{
							node = affected[affected.Count() - 1];
							ReduceNode(nodes, node, index);
							affected.RemoveAt(affected.Count() - 1);

							FOREACH(Node*, in, node->ins)
							{
								in->outs.Remove(node);
								TestReducableNode(affected, in);
							}
							FOREACH(Node*, out, node->outs)
							{
								out->ins.Remove(node);
								TestReducableNode(affected, out);
							}
						}
						affected.Clear();
					}
				}
			}

			static Node* FindCycle(SortedList<Node*>& nodes)
			{
				if (nodes.Count() == 0)
				{
					return nullptr;
				}
				FOREACH(Node*, node, nodes)
				{
					node->previous = nullptr;
					node->next = -1;
					node->touched = false;
				}

				auto current = nodes[0];
				while (true)
				{
					CHECK_ERROR(current != nullptr, L"vl::database::DeadlockDetection::FindCycle(SortedList<DeadlockDetection::Node*>&, SortedList<DeadlockDetection::Node*>&)#Internal error: Failed to find a cycle, ReduceGraph does not work correctly.");
					current->touched = true;
					if (++current->next < current->outs.Count())
					{
						auto next = current->outs[current->next];
						if (next->next != -1)
						{
							next->previous = current;
							return next;
						}
						else if (!next->touched)
						{
							next->previous = current;
							current = next;
						}
					}
					else
					{
						auto previous = current->previous;
						current->previous = nullptr;
						current = previous;
					}
				}
			}

			static void SaveCycle(SortedList<Node*>& involved, Node* cycle)
			{
				auto current = cycle;
				do
				{
					if (!involved.Contains(current))
					{
						involved.Add(current);
					}
					current = current->previous;
				} while (current != cycle);
			}

			static Node* ChooseVictim(SortedList<Node*>& nodes, Node* cycle)
			{
				FOREACH(Node*, in, cycle->ins)
				{
					in->outs.Remove(cycle);
				}
				FOREACH(Node*, out, cycle->outs)
				{
					out->ins.Remove(cycle);
				}
				nodes.Remove(cycle);
				return cycle;
			}

			static void DetectDeadlock(LockManager* lm, DeadlockInfo& info)
			{
				List<Node*> allNodes;
				SortedList<Node*> nodes, involved;
				BuildGraph(lm, allNodes);
				CopyFrom(nodes, allNodes);

				while (true)
				{
					ReduceGraph(nodes);
					auto cycle = FindCycle(nodes);

					if (cycle)
					{
						SaveCycle(involved, cycle);
						auto rollback = ChooseVictim(nodes, cycle);
						info.rollbacks.Add(rollback->transInfo->trans);
					}
					else
					{
						break;
					}
				}

				FOREACH(Node*, node, involved)
				{
					info.pending.Add(node->transInfo->trans, node->transInfo->pendingLock);
					FOREACH(vint, acquired, node->acquired)
					{
						info.acquired.Add(node->transInfo->trans, node->transInfo->acquiredLocks[acquired]);
					}
				}
				DeleteGraph(allNodes);
			}
		};

		void LockManager::DetectDeadlock(DeadlockInfo& info)
		{
			SPIN_LOCK(lock)
			{
				DeadlockDetection::DetectDeadlock(this, info);
			}
		}

		bool LockManager::Rollback(BufferTransaction trans)
		{
			SPIN_LOCK(lock)
			{
				vint index = transactions.Keys().IndexOf(trans);
				if (index == -1)
				{
					return false;
				}

				auto transInfo = transactions.Values()[index];
				if (!transInfo->pendingLock.IsValid())
				{
					return false;
				}

				bool success = OperateObjectLock<ReleaseLockArgs>(
					trans,
					transInfo->pendingLock,
					&LockManager::ReleaseTableLock,
					&LockManager::ReleasePageLock,
					&LockManager::ReleaseRowLock,
					false,
					false
					);
				if (!success)
				{
					CHECK_ERROR(false, L"vl::database::LockManager::Rollback(BufferTransaction)#Internal error: Failed to rollback a transaction.");
				}
				for (vint i = transInfo->acquiredLocks.Count() - 1; i >= 0; i--)
				{
					success = OperateObjectLock<ReleaseLockArgs>(
						trans,
						transInfo->acquiredLocks[i],
						&LockManager::ReleaseTableLock,
						&LockManager::ReleasePageLock,
						&LockManager::ReleaseRowLock,
						false,
						false
						);
					if (!success)
					{
						CHECK_ERROR(false, L"vl::database::LockManager::Rollback(BufferTransaction)#Internal error: Failed to rollback a transaction.");
					}
				}
				return true;
			}
			return false;
		}
		
#undef LOCK_TYPES
	}
}
