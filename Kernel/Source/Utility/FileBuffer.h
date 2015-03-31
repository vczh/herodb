/***********************************************************************
Vczh Library++ 3.0
Developer: Zihan Chen(vczh)
Database::Utility

***********************************************************************/

#ifndef VCZH_DATABASE_UTILITY_FILEBUFFER
#define VCZH_DATABASE_UTILITY_FILEBUFFER

#include "Buffer.h"

namespace vl
{
	namespace database
	{
		class FileBufferSource : public Object, public IBufferSource
		{
			typedef collections::Dictionary<vuint64_t, Ptr<BufferPageDesc>>		PageMap;
			typedef collections::List<vuint64_t>								PageList;
		private:
			BufferSource			source;
			volatile vuint64_t*		totalUsedPages;
			vuint64_t				pageSize;
			SpinLock				lock;
			WString					fileName;
			int						fileDescriptor;

			PageMap					mappedPages;
			PageList				initialPages;
			PageList				useMaskPages;
			vint					activeInitialPageIndex;
			vuint64_t				totalPageCount;

			vuint64_t				initialPageItemCount;
			vuint64_t				useMaskPageItemCount;
			BufferPage				indexPage;

			Ptr<BufferPageDesc>		MapPage(BufferPage page);
			void					PushFreePage(BufferPage page);
			BufferPage				PopFreePage();
			bool					GetUseMask(BufferPage page);
			void					SetUseMask(BufferPage page, bool available);
			BufferPage				AppendPage();
		public:

			FileBufferSource(BufferSource _source, volatile vuint64_t* _totalUsedPages, vuint64_t _pageSize, const WString& _fileName, int _fileDescriptor);

			void					InitializeEmptySource();
			void					InitializeExistingSource();

			void					Unload()override;
			BufferSource			GetBufferSource()override;
			SpinLock&				GetLock()override;
			WString					GetFileName()override;
			bool					UnmapPage(BufferPage page)override;
			BufferPage				GetIndexPage()override;
			BufferPage				AllocatePage()override;
			bool					FreePage(BufferPage page)override;
			void*					LockPage(BufferPage page)override;
			bool					UnlockPage(BufferPage page, void* buffer, PersistanceType persistanceType)override;
			void					FillUnmapPageCandidates(collections::List<BufferPageTimeTuple>& pages, vint expectCount)override;
		};

		extern IBufferSource*		CreateFileSource(BufferSource source, volatile vuint64_t* totalUsedPages, vuint64_t pageSize, const WString& fileName, bool createNew);	
	}
}

#endif
