#include "FileBuffer.h"
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

/*
 * Page Structure
 *		Initial Page	: [uint64 NextInitialPage][uint64 FreePageItems]{[uint64 FreePage] ...}
 *		Use Mask Page	: [uint64 NextUseMaskPage]{[bit FreePageMask] ...}
 *			FreePageMask 1=used, 0=free
 */

#define INDEX_INVALID (~(vuint64_t)0)
#define INDEX_PAGE_USEMASK 0
#define INDEX_PAGE_FREEITEM 1
#define INDEX_PAGE_INDEX 2

#define INDEX_USEMASK_NEXTUSEMASKPAGE 0
#define INDEX_USEMASK_USEMASKBEGIN 1

#define INDEX_FREEITEM_NEXTINITIALPAGE 0
#define INDEX_FREEITEM_FREEPAGEITEMS 1
#define INDEX_FREEITEM_FREEPAGEITEMBEGIN 2

namespace vl
{
	namespace database
	{
		using namespace collections;
		using namespace buffer_internal;

		namespace buffer_internal
		{

/***********************************************************************
FileMapping
***********************************************************************/

			FileMapping::FileMapping(vuint64_t _pageSize, int _fileDescriptor, volatile vuint64_t* _totalUsedPages)
				:pageSize(_pageSize)
				,fileDescriptor(_fileDescriptor)
				,totalUsedPages(_totalUsedPages)
			{
			}

			void FileMapping::InitializeEmptySource()
			{
				totalPageCount = 3;
			}

			void FileMapping::InitializeExistingSource()
			{
				struct stat fileState;
				fstat(fileDescriptor, &fileState);
				totalPageCount = fileState.st_size / pageSize;
			}

			vuint64_t FileMapping::GetTotalPageCount()
			{
				return totalPageCount;
			}

			Ptr<BufferPageDesc> FileMapping::MapPage(BufferPage page)
			{
				vint index = mappedPages.Keys().IndexOf(page.index);
				if (index == -1)
				{
					vuint64_t offset = page.index * pageSize;
					struct stat fileState;	
					if (fstat(fileDescriptor, &fileState) == -1)
					{
						return nullptr;
					}
					if (fileState.st_size < offset + pageSize)
					{
						if (fileState.st_size != offset)
						{
							return nullptr;
						}
						ftruncate(fileDescriptor, offset + pageSize);
						totalPageCount = offset / pageSize + 1;
					}

					void* address = mmap(nullptr, pageSize, PROT_READ | PROT_WRITE, MAP_SHARED, fileDescriptor, offset);
					if (address == MAP_FAILED)
					{
						return nullptr;
					}
					
					auto pageDesc = MakePtr<BufferPageDesc>();
					pageDesc->address = address;
					pageDesc->offset = offset;
					pageDesc->lastAccessTime = (vuint64_t)time(nullptr);
					mappedPages.Add(page.index, pageDesc);
					INCRC(totalUsedPages);
					return pageDesc;
				}
				else
				{
					auto pageDesc = mappedPages.Values()[index];
					pageDesc->lastAccessTime = (vuint64_t)time(nullptr);
					return pageDesc;
				}
			}

			BufferPage FileMapping::AppendPage()
			{
				BufferPage page{totalPageCount};
				if (MapPage(page))
				{
					return page;
				}
				else
				{
					return BufferPage::Invalid();
				}
			}

			bool FileMapping::UnmapPage(BufferPage page)
			{
				vint index = mappedPages.Keys().IndexOf(page.index);
				if (index == -1)
				{
					return false;
				}

				auto pageDesc = mappedPages.Values()[index];
				if (pageDesc->locked)
				{
					return false;
				}

				if (pageDesc->dirty)
				{
					msync(pageDesc->address, pageSize, MS_SYNC);
					pageDesc->dirty = false;
				}
				munmap(pageDesc->address, pageSize);

				mappedPages.Remove(page.index);
				DECRC(totalUsedPages);
				return true;
			}

			void FileMapping::UnmapAllPages()
			{
				FOREACH(Ptr<BufferPageDesc>, pageDesc, mappedPages.Values())
				{
					DECRC(totalUsedPages);
					munmap(pageDesc->address, pageSize);
				}
			}

			vint FileMapping::GetMappedPageCount()
			{
				return mappedPages.Count();
			}
			
			BufferPage FileMapping::GetMappedPage(vint index)
			{
				return mappedPages.Keys()[index];
			}

			Ptr<BufferPageDesc> FileMapping::GetMappedPageDesc(vint index)
			{
				return mappedPages.Values()[index];
			}

			Ptr<BufferPageDesc> FileMapping::GetMappedPageDesc(BufferPage page)
			{
				vint index = mappedPages.Keys().IndexOf(page.index);
				if (index == -1)
				{
					return nullptr;
				}
				return mappedPages.Values()[index];
			}

/***********************************************************************
FileUseMasks
***********************************************************************/

			FileUseMasks::FileUseMasks(vuint64_t _pageSize, int _fileDescriptor)
				:pageSize(_pageSize)
				,fileDescriptor(_fileDescriptor)
				,useMaskPageItemCount((_pageSize - INDEX_USEMASK_USEMASKBEGIN * sizeof(vuint64_t)) / sizeof(vuint64_t))
			{
			}

			void FileUseMasks::InitializeEmptySource(FileMapping* _fileMapping)
			{
				fileMapping = _fileMapping;

				useMaskPages.Clear();
				BufferPage page{INDEX_PAGE_USEMASK};
				auto pageDesc = fileMapping->MapPage(page);
				CHECK_ERROR(pageDesc != nullptr, L"vl::database::buffer_internal::FileUseMasks::InitializeEmptySource()#Internal error: Failed to map INDEX_PAGE_USEMASK.");
				vuint64_t* numbers = (vuint64_t*)pageDesc->address;
				memset(numbers, 0, pageSize);
				numbers[INDEX_USEMASK_NEXTUSEMASKPAGE] = INDEX_INVALID;
				msync(numbers, pageSize, MS_SYNC);
				useMaskPages.Add(page.index);
			}

			void FileUseMasks::InitializeExistingSource(FileMapping* _fileMapping)
			{
				fileMapping = _fileMapping;

				useMaskPages.Clear();
				BufferPage page{INDEX_PAGE_USEMASK};
				
				while(page.index != INDEX_INVALID)
				{
					useMaskPages.Add(page.index);
					auto pageDesc = fileMapping->MapPage(page);
					vuint64_t* numbers = (vuint64_t*)pageDesc->address;
					page.index = numbers[INDEX_USEMASK_NEXTUSEMASKPAGE];
				}
			}

			bool FileUseMasks::GetUseMask(BufferPage page)
			{
				auto useMaskPageBits = 8 * sizeof(vuint64_t);
				auto useMaskPageIndex = page.index / (useMaskPageBits * useMaskPageItemCount);
				auto useMaskPageBitIndex = page.index % (useMaskPageBits * useMaskPageItemCount);
				auto useMaskPageItem = INDEX_USEMASK_USEMASKBEGIN + useMaskPageBitIndex / useMaskPageBits;
				auto useMaskPageShift = useMaskPageBitIndex % useMaskPageBits;

				BufferPage useMaskPage{useMaskPages[useMaskPageIndex]};
				auto pageDesc = fileMapping->MapPage(useMaskPage);
				CHECK_ERROR(pageDesc != nullptr, L"vl::database::buffer_internal::FileUseMasks::GetUseMask(BufferPage)#Internal error: Failed to map the specified use mask page.");
				vuint64_t* numbers = (vuint64_t*)pageDesc->address;
				auto& item = numbers[useMaskPageItem];
				bool result = ((item >> useMaskPageShift) & ((vuint64_t)1)) == 1;
				msync(numbers, pageSize, MS_SYNC);
				return result;
			}
			
			void FileUseMasks::SetUseMask(BufferPage page, bool available)
			{
				auto useMaskPageBits = 8 * sizeof(vuint64_t);
				auto useMaskPageIndex = page.index / (useMaskPageBits * useMaskPageItemCount);
				auto useMaskPageBitIndex = page.index % (useMaskPageBits * useMaskPageItemCount);
				auto useMaskPageItem = INDEX_USEMASK_USEMASKBEGIN + useMaskPageBitIndex / useMaskPageBits;
				auto useMaskPageShift = useMaskPageBitIndex % useMaskPageBits;
				bool newPage = false;

				BufferPage useMaskPage = BufferPage::Invalid();

				if (useMaskPageIndex == useMaskPages.Count())
				{
					newPage = true;
					BufferPage lastPage{useMaskPages[useMaskPageIndex - 1]};
					useMaskPage = fileMapping->AppendPage();
					CHECK_ERROR(useMaskPage.IsValid(), L"vl::database::buffer_internal::FileUseMasks::SetUseMask(BufferPage, bool)#Internal error: Failed to create a new use mask page.");
					SetUseMask(useMaskPage, true);
					useMaskPages.Add(useMaskPage.index);

					auto pageDesc = fileMapping->MapPage(lastPage);
					CHECK_ERROR(pageDesc != nullptr, L"vl::database::buffer_internal::FileUseMasks::SetUseMask(BufferPage, bool)#Internal error: Failed to map the last use mask page.");
					vuint64_t* numbers = (vuint64_t*)pageDesc->address;
					numbers[INDEX_USEMASK_NEXTUSEMASKPAGE] = useMaskPage.index;
					msync(numbers, pageSize, MS_SYNC);
				}
				else
				{
					useMaskPage.index = useMaskPages[useMaskPageIndex];
				}

				auto pageDesc = fileMapping->MapPage(useMaskPage);
				CHECK_ERROR(pageDesc != nullptr, L"vl::database::buffer_internal::FileUseMasks::SetUseMask(BufferPage, bool)#Internal error: Failed to map the specified use mask page.");
				vuint64_t* numbers = (vuint64_t*)pageDesc->address;
				if (newPage)
				{
					numbers[INDEX_USEMASK_NEXTUSEMASKPAGE] = INDEX_INVALID;
				}

				auto& item = numbers[useMaskPageItem];
				if (available)
				{
					vuint64_t mask = ((vuint64_t)1) << useMaskPageShift;
					item |= mask;
				}
				else
				{
					vuint64_t mask = ~(((vuint64_t)1) << useMaskPageShift);
					item &= mask;
				}
				msync(numbers, pageSize, MS_SYNC);
			}
		}

/***********************************************************************
FileFreePages
***********************************************************************/

			FileFreePages::FileFreePages(vuint64_t _pageSize)
				:pageSize(_pageSize)
				,freeItemPageItemCount((_pageSize - INDEX_FREEITEM_FREEPAGEITEMBEGIN * sizeof(vuint64_t)) / sizeof(vuint64_t))
				,activeFreeItemPageIndex(-1)
			{
			}

			void FileFreePages::InitializeEmptySource(FileMapping* _fileMapping, FileUseMasks* _fileUseMasks)
			{
				fileMapping = _fileMapping;
				fileUseMasks = _fileUseMasks;

				freeItemPages.Clear();
				BufferPage page{INDEX_PAGE_FREEITEM};
				auto pageDesc = fileMapping->MapPage(page);
				CHECK_ERROR(pageDesc != nullptr, L"vl::database::buffer_internal::FileFreePages::InitializeEmptySource()#Internal error: Failed to map INDEX_PAGE_FREEITEM.");

				vuint64_t* numbers = (vuint64_t*)pageDesc->address;
				memset(numbers, 0, pageSize);
				numbers[INDEX_FREEITEM_NEXTINITIALPAGE] = INDEX_INVALID;
				numbers[INDEX_FREEITEM_FREEPAGEITEMS] = 0;
				msync(numbers, pageSize, MS_SYNC);

				freeItemPages.Add(page.index);
				activeFreeItemPageIndex = 0;
			}

			void FileFreePages::InitializeExistingSource(FileMapping* _fileMapping, FileUseMasks* _fileUseMasks)
			{
				fileMapping = _fileMapping;
				fileUseMasks = _fileUseMasks;

				freeItemPages.Clear();
				BufferPage page{INDEX_PAGE_FREEITEM};
				
				while(page.index != INDEX_INVALID)
				{
					freeItemPages.Add(page.index);
					auto pageDesc = fileMapping->MapPage(page);
					vuint64_t* numbers = (vuint64_t*)pageDesc->address;
					page.index = numbers[INDEX_FREEITEM_NEXTINITIALPAGE];

					if (numbers[INDEX_FREEITEM_FREEPAGEITEMS] != 0)
					{
						activeFreeItemPageIndex = freeItemPages.Count() - 1;
					}
				}
				
				if (activeFreeItemPageIndex == -1)
				{
					activeFreeItemPageIndex = 0;
				}
			}

			void FileFreePages::PushFreePage(BufferPage page)
			{
				BufferPage initialPage{freeItemPages[activeFreeItemPageIndex]};
				auto pageDesc = fileMapping->MapPage(initialPage);
				CHECK_ERROR(pageDesc != nullptr, L"vl::database::buffer_internal::FileFreePages::PushFreePage(BufferPage)#Internal error: Failed to map the last active initial page.");
				vuint64_t* numbers = (vuint64_t*)pageDesc->address;
				vuint64_t& count = numbers[INDEX_FREEITEM_FREEPAGEITEMS];
				if (count == freeItemPageItemCount)
				{
					if (activeFreeItemPageIndex == freeItemPages.Count() - 1)
					{
						BufferPage newInitialPage{fileMapping->GetTotalPageCount()};
						numbers[INDEX_FREEITEM_NEXTINITIALPAGE] = newInitialPage.index;
						msync(numbers, pageSize, MS_SYNC);

						auto newPageDesc = fileMapping->MapPage(newInitialPage);
						CHECK_ERROR(newPageDesc != nullptr, L"vl::database::buffer_internal::FileFreePages::PushFreePage(BufferPage)#Internal error: Failed to create a new initial page.");
						numbers = (vuint64_t*)newPageDesc->address;
						memset(numbers, 0, pageSize);
						numbers[INDEX_FREEITEM_NEXTINITIALPAGE] = INDEX_INVALID;
						numbers[INDEX_FREEITEM_FREEPAGEITEMS] = 1;
						numbers[INDEX_FREEITEM_FREEPAGEITEMBEGIN] = page.index;
						msync(numbers, pageSize, MS_SYNC);
						freeItemPages.Add(newInitialPage.index);
						fileUseMasks->SetUseMask(newInitialPage, true);
					}
					else
					{
						BufferPage newInitialPage{freeItemPages[activeFreeItemPageIndex + 1]};
						auto newPageDesc = fileMapping->MapPage(newInitialPage);
						CHECK_ERROR(newPageDesc != nullptr, L"vl::database::buffer_internal::FileFreePages::PushFreePage(BufferPage)#Internal error: Failed to reuse a created initial page.");
						numbers = (vuint64_t*)newPageDesc->address;
						numbers[INDEX_FREEITEM_FREEPAGEITEMS] = 1;
						numbers[INDEX_FREEITEM_FREEPAGEITEMBEGIN] = page.index;
						msync(numbers, pageSize, MS_SYNC);
					}
					activeFreeItemPageIndex++;
				}
				else
				{
					numbers[INDEX_FREEITEM_FREEPAGEITEMBEGIN + count] = page.index;
					count++;
					msync(numbers, pageSize, MS_SYNC);
				}
			}

			BufferPage FileFreePages::PopFreePage()
			{
				BufferPage page = BufferPage::Invalid();
				BufferPage initialPage{freeItemPages[activeFreeItemPageIndex]};
				auto pageDesc = fileMapping->MapPage(initialPage);
				CHECK_ERROR(pageDesc != nullptr, L"vl::database::buffer_internal::FileFreePages::PopFreePage()#Internal error: Failed to map the last active initial page.");
				vuint64_t* numbers = (vuint64_t*)pageDesc->address;
				vuint64_t& count = numbers[INDEX_FREEITEM_FREEPAGEITEMS];

				if (count == 0 && initialPage.index == INDEX_PAGE_FREEITEM)
				{
					return page;
				}
				count--;
				page.index = numbers[INDEX_FREEITEM_FREEPAGEITEMBEGIN + count];
				msync(numbers, pageSize, MS_SYNC);

				if (count == 0)
				{
					activeFreeItemPageIndex--;
				}
				return page;
			}

/***********************************************************************
FileBufferSource
***********************************************************************/

		FileBufferSource::FileBufferSource(BufferSource _source, volatile vuint64_t* _totalUsedPages, vuint64_t _pageSize, const WString& _fileName, int _fileDescriptor)
			:source(_source)
			,pageSize(_pageSize)
			,fileName(_fileName)
			,fileDescriptor(_fileDescriptor)
			,fileMapping(_pageSize, _fileDescriptor, _totalUsedPages)
			,fileUseMasks(_pageSize, _fileDescriptor)
			,fileFreePages(_pageSize)
		{
			indexPage.index = INDEX_PAGE_INDEX;
		}

		void FileBufferSource::InitializeEmptySource()
		{
			fileMapping.InitializeEmptySource();
			fileUseMasks.InitializeEmptySource(&fileMapping);
			fileFreePages.InitializeEmptySource(&fileMapping, &fileUseMasks);

			auto pageDesc = fileMapping.MapPage(BufferPage{INDEX_PAGE_INDEX});
			CHECK_ERROR(pageDesc != nullptr, L"vl::database::FileBufferSource::InitializeEmptySource()#Internal error: Failed to map INDEX_PAGE_INDEX.");

			fileUseMasks.SetUseMask(BufferPage{INDEX_PAGE_FREEITEM}, true);
			fileUseMasks.SetUseMask(BufferPage{INDEX_PAGE_USEMASK}, true);
			fileUseMasks.SetUseMask(BufferPage{INDEX_PAGE_INDEX}, true);
		}

		void FileBufferSource::InitializeExistingSource()
		{
			fileMapping.InitializeExistingSource();
			fileUseMasks.InitializeExistingSource(&fileMapping);
			fileFreePages.InitializeExistingSource(&fileMapping, &fileUseMasks);
		}

		void FileBufferSource::Unload()
		{
			fileMapping.UnmapAllPages();
			close(fileDescriptor);
		}

		BufferSource FileBufferSource::GetBufferSource()
		{
			return source;
		}

		SpinLock& FileBufferSource::GetLock()
		{
			return lock;
		}

		WString FileBufferSource::GetFileName()
		{
			return fileName;
		}

		bool FileBufferSource::UnmapPage(BufferPage page)
		{
			return fileMapping.UnmapPage(page);
		}
		
		BufferPage FileBufferSource::GetIndexPage()
		{
			return indexPage;
		}

		BufferPage FileBufferSource::AllocatePage()
		{
			BufferPage page = fileFreePages.PopFreePage();
			if (!page.IsValid())
			{
				page = fileMapping.AppendPage();
			}
			if (page.IsValid())
			{
				fileUseMasks.SetUseMask(page, true);
			}
			return page;
		}

		bool FileBufferSource::FreePage(BufferPage page)
		{
			switch(page.index)
			{
				case INDEX_PAGE_FREEITEM:
				case INDEX_PAGE_USEMASK:
				case INDEX_PAGE_INDEX:
					return false;
			}

			if (!fileUseMasks.GetUseMask(page)) return false;
			if (fileMapping.GetMappedPageDesc(page))
			{
				if (!UnmapPage(page))
				{
					return false;
				}
			}
			fileFreePages.PushFreePage(page);
			fileUseMasks.SetUseMask(page, false);
			return true;
		}

		void* FileBufferSource::LockPage(BufferPage page)
		{
			if (page.index >= fileMapping.GetTotalPageCount())
			{
				return nullptr;
			}
			if (!fileUseMasks.GetUseMask(page)) return nullptr;
			if (auto pageDesc = fileMapping.MapPage(page))
			{
				if (pageDesc->locked) return nullptr;
				pageDesc->locked = true;
				return pageDesc->address;
			}
			else
			{
				return nullptr;
			}
		}

		bool FileBufferSource::UnlockPage(BufferPage page, void* buffer, PersistanceType persistanceType)
		{
			auto pageDesc = fileMapping.GetMappedPageDesc(page);
			if (!pageDesc) return false;
			if (pageDesc->address != buffer) return false;
			if (!pageDesc->locked) return false;

			switch (persistanceType)
			{
				case PersistanceType::NoChanging:
					break;
				case PersistanceType::Changed:
					pageDesc->dirty = true;
					break;
				case PersistanceType::ChangedAndPersist:
					msync(pageDesc->address, pageSize, MS_SYNC);
					pageDesc->dirty = false;
					break;
			}
			pageDesc->locked = false;
			return true;
		}	

		void FileBufferSource::FillUnmapPageCandidates(collections::List<BufferPageTimeTuple>& pages, vint expectCount)
		{
			vint mappedCount = fileMapping.GetMappedPageCount();
			if (mappedCount == 0) return;

			Array<BufferPageTimeTuple> tuples(mappedCount);
			vint usedCount = 0;
			for (vint i = 0; i < mappedCount; i++)
			{
				auto key = fileMapping.GetMappedPage(i);
				auto value = fileMapping.GetMappedPageDesc(i);
				if (!value->locked)
				{
					BufferPage page{key};
					tuples[usedCount++] = BufferPageTimeTuple(source, page, value->lastAccessTime);
				}
			}

			if (tuples.Count() > 0)
			{
				SortLambda(&tuples[0], usedCount, [](const BufferPageTimeTuple& t1, const BufferPageTimeTuple& t2)
				{
					if (t1.f2 < t2.f2) return -1;
					else if (t1.f2 > t2.f2) return 1;
					else return 0;
				});

				vint copyCount = usedCount < expectCount ? usedCount : expectCount;
				for (vint i = 0; i < copyCount; i++)
				{
					pages.Add(tuples[i]);
				}
			}
		}

		IBufferSource* CreateFileSource(BufferSource source, volatile vuint64_t* totalUsedPages, vuint64_t pageSize, const WString& fileName, bool createNew)
		{
			int fileDescriptor = 0;
			if (createNew)
			{
				auto mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;
				fileDescriptor = open(wtoa(fileName).Buffer(), O_CREAT | O_TRUNC | O_RDWR, mode);
			}
			else
			{
				fileDescriptor = open(wtoa(fileName).Buffer(), O_RDWR);
			}

			if (fileDescriptor == -1)
			{
				return nullptr;
			}
			else
			{
				auto result = new FileBufferSource(source, totalUsedPages, pageSize, fileName, fileDescriptor);
				if (createNew)
				{
					result->InitializeEmptySource();
				}
				else
				{
					result->InitializeExistingSource();
				}
				return result;
			}
		}
	}
}

#undef INDEX_INVALID
#undef INDEX_PAGE_FREEITEM
#undef INDEX_PAGE_USEMASK
#undef INDEX_PAGE_INDEX
#undef INDEX_FREEITEM_NEXTINITIALPAGE
#undef INDEX_FREEITEM_FREEPAGEITEMS
#undef INDEX_FREEITEM_FREEPAGEITEMBEGIN
#undef INDEX_USEMASK_NEXTUSEMASKPAGE
#undef INDEX_USEMASK_USEMASKBEGIN
