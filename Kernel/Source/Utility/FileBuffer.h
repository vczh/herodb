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
		namespace buffer_internal
		{
			class FileMapping : public Object
			{
				typedef collections::Dictionary<vuint64_t, Ptr<BufferPageDesc>>	PageMap;
			private:
				vuint64_t					pageSize;
				int							fileDescriptor;
				volatile vuint64_t*			totalUsedPages;
				PageMap						mappedPages;
				vuint64_t					totalPageCount = 0;
				
			public:
				FileMapping(vuint64_t _pageSize, int _fileDescriptor, volatile vuint64_t* _totalUsedPages);

				void						InitializeEmptySource();
				void						InitializeExistingSource();

				vuint64_t					GetTotalPageCount();
				Ptr<BufferPageDesc>			MapPage(BufferPage page);
				BufferPage					AppendPage();
				bool						UnmapPage(BufferPage page);
				void						UnmapAllPages();

				vint						GetMappedPageCount();
				BufferPage					GetMappedPage(vint index);
				Ptr<BufferPageDesc>			GetMappedPageDesc(vint index);
				Ptr<BufferPageDesc>			GetMappedPageDesc(BufferPage page);
			};

			class FileUseMasks : public Object
			{
				typedef collections::Dictionary<vuint64_t, Ptr<BufferPageDesc>>	PageMap;
				typedef collections::List<vuint64_t>							PageList;
			private:
				int							fileDescriptor;
				vuint64_t					pageSize;
				PageList					useMaskPages;
				vuint64_t					useMaskPageItemCount;
				FileMapping*				fileMapping = nullptr;
	
			public:
				FileUseMasks(vuint64_t _pageSize, int _fileDescriptor);

				void						InitializeEmptySource(FileMapping* _fileMapping);
				void						InitializeExistingSource(FileMapping* _fileMapping);

				bool						GetUseMask(BufferPage page);
				void						SetUseMask(BufferPage page, bool available);
			};

			class FileFreePages : public Object
			{
				typedef collections::List<vuint64_t>							PageList;
			private:
				vuint64_t					pageSize;
				PageList					freeItemPages;
				vint						activeFreeItemPageIndex;
				vuint64_t					freeItemPageItemCount;
				FileMapping*				fileMapping = nullptr;
				FileUseMasks*				fileUseMasks = nullptr;

			public:
				FileFreePages(vuint64_t _pageSize);

				void						InitializeEmptySource(FileMapping* _fileMapping, FileUseMasks* _fileUseMasks);
				void						InitializeExistingSource(FileMapping* _fileMapping, FileUseMasks* _fileUseMasks);

				void						PushFreePage(BufferPage page);
				BufferPage					PopFreePage();
			};
		}

		class FileBufferSource : public Object, public IBufferSource
		{
			typedef collections::Dictionary<vuint64_t, Ptr<BufferPageDesc>>		PageMap;
			typedef collections::List<vuint64_t>								PageList;
		private:
			BufferSource					source;
			vuint64_t						pageSize;
			SpinLock						lock;
			WString							fileName;
			int								fileDescriptor;
			BufferPage						indexPage;

			buffer_internal::FileMapping	fileMapping;
			buffer_internal::FileUseMasks	fileUseMasks;
			buffer_internal::FileFreePages	fileFreePages;

		public:

			FileBufferSource(BufferSource _source, volatile vuint64_t* _totalUsedPages, vuint64_t _pageSize, const WString& _fileName, int _fileDescriptor);

			void							InitializeEmptySource();
			void							InitializeExistingSource();

			void							Unload()override;
			BufferSource					GetBufferSource()override;
			SpinLock&						GetLock()override;
			WString							GetFileName()override;
			bool							UnmapPage(BufferPage page)override;
			BufferPage						GetIndexPage()override;
			BufferPage						AllocatePage()override;
			bool							FreePage(BufferPage page)override;
			void*							LockPage(BufferPage page)override;
			bool							UnlockPage(BufferPage page, void* buffer, PersistanceType persistanceType)override;
			void							FillUnmapPageCandidates(collections::List<BufferPageTimeTuple>& pages, vint expectCount)override;
		};

		extern IBufferSource*		CreateFileSource(BufferSource source, volatile vuint64_t* totalUsedPages, vuint64_t pageSize, const WString& fileName, bool createNew);	
	}
}

#endif
