/***********************************************************************
Vczh Library++ 3.0
Developer: Zihan Chen(vczh)
Database::Utility

***********************************************************************/

#ifndef VCZH_DATABASE_UTILITY_INMEMORYBUFFER
#define VCZH_DATABASE_UTILITY_INMEMORYBUFFER

#include "Buffer.h"

namespace vl
{
	namespace database
	{
		class InMemoryBufferSource : public Object, public IBufferSource
		{
			typedef collections::List<Ptr<BufferPageDesc>>	PageList;
			typedef collections::List<vuint64_t>			PageIdList;
		private:
			BufferSource		source;
			volatile vuint64_t*	totalUsedPages;
			vuint64_t			pageSize;
			SpinLock			lock;
			PageList			pages;
			PageIdList			freePages;
			BufferPage			indexPage;

			Ptr<BufferPageDesc>	MapPage(BufferPage page);
		public:
			InMemoryBufferSource(BufferSource _source, volatile vuint64_t* _totalUsedPages, vuint64_t _pageSize);

			void				Unload()override;
			BufferSource		GetBufferSource()override;
			SpinLock&			GetLock()override;
			WString				GetFileName()override;
			bool				UnmapPage(BufferPage page)override;
			BufferPage			GetIndexPage()override;
			BufferPage			AllocatePage()override;
			bool				FreePage(BufferPage page)override;
			void* 				LockPage(BufferPage page)override;
			bool				UnlockPage(BufferPage page, void* address, PersistanceType persistanceType)override;
			void				FillUnmapPageCandidates(collections::List<BufferPageTimeTuple>& pages, vint expectCount)override;
		};

		extern IBufferSource*	CreateMemorySource(BufferSource source, volatile vuint64_t* totalUsedPages, vuint64_t pageSize);
	}
}

#endif
