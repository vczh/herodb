#include "Log.h"

/*
 *	Page Structure
 *		Index Page				: [uint64 AddressItems][uint64 NextIndexPage]{[uint64 AddressItem] ...}
 *			The first index page is the index page from the source.
 *			Each address item is the address of transactions of 0, 1, ...
 * 		Log Page				:
 * 			Transaction Header	: [uint64 Transaction]<Item-Header>
 * 				Min Size = 5 * sizeof(uint64)
 * 			Item Header			: [uint64 ItemLength]<Item-Block>
 * 				Min Size = 4 * sizeof(uint64)
 * 			Item Block			: [uint64 CurrentBlockLength][uint64 NextBlockAddress/NextItemAddress][data, adjust to sizeof(uint64)]
 * 				Min Size = 3 * sizeof(uint64)
 */

#define INDEX_INVALID (~(vuint64_t)0)

#define INDEX_INDEXPAGE_ADDRESSITEMS 0
#define INDEX_INDEXPAGE_NEXTINDEXPAGE 1
#define INDEX_INDEXPAGE_ADDRESSITEMBEGIN 2

namespace vl
{
	namespace database
	{
/***********************************************************************
LogManager
***********************************************************************/

		bool LogManager::WriteAddressItem(BufferTransaction transaction, BufferPointer address)
		{
			if (transaction.index >= usedTransactionCount)
			{
				return false;
			}

			vuint64_t index = transaction.index / indexPageItemCount;
			vuint64_t item = transaction.index % indexPageItemCount;
			if (index > indexPages.Count())
			{
				return false;
			}

			if (index < indexPages.Count())
			{
				BufferPage page = indexPages[index];
				auto numbers = (vuint64_t*)bm->LockPage(source, page);
				if (!numbers) return false;
				numbers[item + INDEX_INDEXPAGE_ADDRESSITEMBEGIN] = address.index;
				bm->UnlockPage(source, page, numbers, true);
			}
			else
			{
				BufferPage lastPage = indexPages[indexPages.Count() - 1];
				BufferPage currentPage = bm->AllocatePage(source);
				if (!currentPage.IsValid())
				{
					return false;
				}

				auto numbers = (vuint64_t*)bm->LockPage(source, lastPage);
				if (!numbers) return false;
				numbers[INDEX_INDEXPAGE_NEXTINDEXPAGE] = currentPage.index;
				bm->UnlockPage(source, lastPage, numbers, true);

				numbers = (vuint64_t*)bm->LockPage(source, currentPage);
				memset(numbers, 0, pageSize);
				numbers[INDEX_INDEXPAGE_ADDRESSITEMS] = 0;
				numbers[INDEX_INDEXPAGE_NEXTINDEXPAGE] = INDEX_INVALID;
				numbers[item + INDEX_INDEXPAGE_ADDRESSITEMBEGIN] = address.index;
				bm->UnlockPage(source, currentPage, numbers, true);
				indexPages.Add(currentPage);
			}

			return true;
		}

		bool LogManager::AllocateBlock(vuint64_t minSize, vuint64_t& size, BufferPointer& address)
		{
			if (minSize == 0 || size == 0 || size < minSize) return false;
			minSize = IntUpperBound(minSize, pageSize);
			if (minSize > pageSize) return false;
			size = IntUpperBound(size, pageSize);

			if (nextBlockAddress.IsValid())
			{
				BufferPage page = bm->AllocatePage(source);
				if (!page.IsValid()) return false;
				if (!bm->EncodePointer(nextBlockAddress, page, 0)) return false;
			}

			BufferPage page;
			vuint64_t offset;
			if (!bm->DecodePointer(nextBlockAddress, page, offset)) return false;

			vuint64_t remain = pageSize - offset;
			if (remain < minSize)
			{
				nextBlockAddress = BufferPointer::Invalid();
				return AllocateBlock(minSize, size, address);
			}
			else
			{
				if (size > remain)
				{
					size = remain;
				}
				address = nextBlockAddress;
				if (!bm->EncodePointer(nextBlockAddress, page, pageSize - size)) return false;
			}

			return true;
		}

		LogManager::LogManager(BufferManager* _bm, BufferSource _source, bool _createNew, bool _autoUnload)
			:bm(_bm)
			,source(_source)
			,autoUnload(_autoUnload)
			,pageSize(0)
			,indexPageItemCount(0)
			,usedTransactionCount(0)
			,nextBlockAddress(BufferPointer::Invalid())
		{
			pageSize = bm->GetPageSize();
			indexPageItemCount = (pageSize - INDEX_INDEXPAGE_ADDRESSITEMBEGIN * sizeof(vuint64_t)) / sizeof(vuint64_t);

			if (_createNew)
			{
				BufferPage page = bm->GetIndexPage(source);
				indexPages.Add(page);

				auto numbers = (vuint64_t*)bm->LockPage(source, page);
				memset(numbers, 0, pageSize);
				numbers[INDEX_INDEXPAGE_ADDRESSITEMS] = 0;
				numbers[INDEX_INDEXPAGE_NEXTINDEXPAGE] = INDEX_INVALID;
				bm->UnlockPage(source, page, numbers, true);
			}
			else
			{
				BufferPage page = bm->GetIndexPage(source);
				BufferPage previousPage = BufferPage::Invalid();
				while (page.IsValid())
				{
					previousPage = page;
					indexPages.Add(page);
					auto numbers = (vuint64_t*)bm->LockPage(source, previousPage);
					page.index = numbers[INDEX_INDEXPAGE_NEXTINDEXPAGE];
					usedTransactionCount += numbers[INDEX_INDEXPAGE_ADDRESSITEMS];
					bm->UnlockPage(source, previousPage, numbers, false);
				}
			}
		}

		LogManager::~LogManager()
		{
			if (autoUnload)
			{
				bm->UnloadSource(source);
			}
		}

		vuint64_t LogManager::GetUsedTransactionCount()
		{
			return usedTransactionCount;
		}

		BufferTransaction LogManager::GetTransaction(vuint64_t index)
		{
			if (index < usedTransactionCount)
			{
				BufferTransaction trans{index};
				return trans;
			}
			else
			{
				return BufferTransaction::Invalid();
			}
		}

		BufferTransaction LogManager::OpenTransaction()
		{
			throw 0;
		}

		bool LogManager::CloseTransaction(BufferTransaction transaction)
		{
			throw 0;
		}

		bool LogManager::IsActive(BufferTransaction transaction)
		{
			throw 0;
		}

		Ptr<ILogWriter> LogManager::OpenLogItem(BufferTransaction transaction)
		{
			throw 0;
		}

		Ptr<ILogReader> LogManager::EnumLogItem(BufferTransaction transaction)
		{
			throw 0;
		}

		Ptr<ILogReader> LogManager::EnumInactiveLogItem(BufferTransaction transaction)
		{
			throw 0;
		}
	}
}

#undef INDEX_INVALID
#undef INDEX_INDEXPAGE_ADDRESSITEMS
#undef INDEX_INDEXPAGE_NEXTINDEXPAGE
#undef INDEX_INDEXPAGE_ADDRESSITEMBEGIN
