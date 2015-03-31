/***********************************************************************
Vczh Library++ 3.0
Developer: Zihan Chen(vczh)
Database::Utility

***********************************************************************/

#ifndef VCZH_DATABASE_UTILITY_LOG
#define VCZH_DATABASE_UTILITY_LOG

#include "Buffer.h"

namespace vl
{
	namespace database
	{
		class ILogAccessor : public virtual Interface
		{
		public:
			virtual BufferTransaction	GetTransaction() = 0;
			virtual stream::IStream&	GetStream() = 0;
		};

		class ILogReader : public virtual ILogAccessor
		{
		public:
			virtual bool				NextItem() = 0;
		};

		class ILogWriter : public virtual ILogAccessor
		{
		public:
			virtual bool				IsOpening() = 0;
			virtual bool				Close() = 0;
		};

		namespace log_internal
		{
			class LogTransDesc
			{
			public:
				BufferPointer					firstItem;
				BufferPointer					lastItem;

				Ptr<ILogWriter>					writer;
			};

			class LogTransactions;
			class LogAddressItem;
			class LogBlocks;

			class LogAddressItem : public Object
			{
				typedef collections::List<BufferPage>												PageList;
			private:
				BufferManager*					bm;
				BufferSource					source;

				vuint64_t						pageSize;
				vuint64_t						indexPageItemCount;
				PageList						indexPages;

			public:
				LogAddressItem(BufferManager* _bm, BufferSource _source);

				vuint64_t						InitializeEmptyItems();
				vuint64_t						InitializeExistingItems();

				BufferPointer					ReadAddressItem(BufferTransaction transaction);
				bool							WriteAddressItem(BufferTransaction transaction, BufferPointer address);
			};

			class LogTransactions : public Object
			{
				typedef collections::Dictionary<BufferTransaction, Ptr<LogTransDesc>>				TransMap;
			private:
				volatile vuint64_t				usedTransactionCount = 0;
				TransMap						activeTransactions;
				LogAddressItem*					logAddressItem = nullptr;

			public:
				LogTransactions();

				void							Initialize(vuint64_t _usedTransactionCount, LogAddressItem* _logAddressItem);

				vuint64_t						GetUsedTransactionCount();
				BufferTransaction				GetTransaction(vuint64_t index);
				Ptr<LogTransDesc>				GetTransDesc(BufferTransaction transaction);

				BufferTransaction				OpenTransaction();
				bool							CloseTransaction(BufferTransaction transaction);
				bool							IsInactive(BufferTransaction transaction);
				bool							IsActive(BufferTransaction transaction);
			};

			class LogBlocks : public Object
			{
			private:
				BufferManager*					bm;
				BufferSource					source;

				vuint64_t						pageSize;
				BufferPointer					nextBlockAddress;
			public:
				LogBlocks(BufferManager* _bm, BufferSource _source);

				bool							AllocateBlock(vuint64_t minSize, vuint64_t& size, BufferPointer& address);
			};

			class LogWriter : public Object, public ILogWriter
			{
			private:
				SpinLock&						lock;
				BufferManager*					bm;
				BufferSource					source;
				LogAddressItem*					logAddressItem;
				LogTransactions*				logTransactions;
				LogBlocks*						logBlocks;

				stream::MemoryStream			stream;
				BufferTransaction				trans;
				bool							opening;

			public:
				LogWriter(SpinLock& _lock, BufferManager* _bm, BufferSource _source, LogAddressItem* _logAddressItem, LogTransactions* _logTransactions, LogBlocks* _logBlocks, BufferTransaction _trans);
				~LogWriter();

				BufferTransaction				GetTransaction()override;
				stream::IStream&				GetStream()override;
				bool							IsOpening()override;
				bool							Close()override;
			};

			class LogReader : public Object, public ILogReader
			{
			private:
				SpinLock&						lock;
				BufferManager*					bm;
				BufferSource					source;
				LogAddressItem*					logAddressItem;
				LogTransactions*				logTransactions;

				BufferTransaction				trans;
				BufferPointer					item;
				Ptr<stream::MemoryStream>		stream;

			public:
				LogReader(SpinLock& _lock, BufferManager* _bm, BufferSource _source, LogAddressItem* _logAddressItem, LogTransactions* _logTransactions, BufferTransaction _trans);
				~LogReader();

				BufferTransaction				GetTransaction()override;
				stream::IStream&				GetStream()override;
				bool							NextItem()override;
			};
		}

		class LogManager : public Object
		{
		private:
			BufferManager*						bm;
			BufferSource						source;
			bool								autoUnload;

			log_internal::LogAddressItem		logAddressItem;
			log_internal::LogBlocks				logBlocks;
			log_internal::LogTransactions		logTransactions;

			SpinLock							lock;

		public:
			LogManager(BufferManager* _bm, BufferSource _source, bool _createNew, bool _autoUnload = true);
			~LogManager();

			vuint64_t							GetUsedTransactionCount();
			BufferTransaction					GetTransaction(vuint64_t index);

			BufferTransaction					OpenTransaction();
			bool								CloseTransaction(BufferTransaction transaction);
			bool								IsActive(BufferTransaction transaction);

			Ptr<ILogWriter>						OpenLogItem(BufferTransaction transaction);
			Ptr<ILogReader>						EnumLogItem(BufferTransaction transaction);
			Ptr<ILogReader>						EnumInactiveLogItem(BufferTransaction transaction);
		};
	}
}

#endif
