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
FileBufferSource
***********************************************************************/

		LogManager::LogManager(BufferManager* _bm, BufferSource _source, bool _createNew, bool _autoUnload)
			:bm(_bm)
			,source(_source)
			,autoUnload(_autoUnload)
			,pageSize(0)
			,indexPageItemCount(0)
			,usedTransactionCount(0)
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
