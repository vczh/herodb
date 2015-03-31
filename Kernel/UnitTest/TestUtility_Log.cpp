#include "UnitTest.h"
#include "../Source/Utility/Log.h"

using namespace vl;
using namespace vl::database;
using namespace vl::database::log_internal;
using namespace vl::collections;

extern WString GetTempFolder();
#define TEMP_DIR GetTempFolder()+
#define KB *1024
#define MB *1024*1024

TEST_CASE(Utility_Log_TransactionWithNoItem)
{
	BufferManager bm(4 KB, 16);
	auto source = bm.LoadFileSource(TEMP_DIR L"db.bin", true);
	LogManager log(&bm, source, true);

	auto trans = log.OpenTransaction();
	TEST_ASSERT(trans.IsValid());
	TEST_ASSERT(log.IsActive(trans) == true);

	auto reader = log.EnumLogItem(trans);
	TEST_ASSERT(reader);
	TEST_ASSERT(reader->GetTransaction() == trans);
	TEST_ASSERT(!log.EnumInactiveLogItem(trans));
	TEST_ASSERT(reader->NextItem() == false);

	TEST_ASSERT(log.CloseTransaction(trans) == true);
	TEST_ASSERT(log.IsActive(trans) == false);

	reader = log.EnumInactiveLogItem(trans);
	TEST_ASSERT(reader);
	TEST_ASSERT(reader->GetTransaction() == trans);
	TEST_ASSERT(!log.EnumLogItem(trans));
	TEST_ASSERT(reader->NextItem() == false);
}

TEST_CASE(Utility_Log_TransactionWithOneEmptyItem)
{
	BufferManager bm(4 KB, 16);
	auto source = bm.LoadFileSource(TEMP_DIR L"db.bin", true);
	LogManager log(&bm, source, true);

	auto trans = log.OpenTransaction();
	TEST_ASSERT(trans.IsValid());
	TEST_ASSERT(log.IsActive(trans) == true);

	auto writer = log.OpenLogItem(trans);
	TEST_ASSERT(writer);
	TEST_ASSERT(writer->GetTransaction() == trans);
	TEST_ASSERT(writer->IsOpening() == true);
	TEST_ASSERT(writer->Close() == true);
	TEST_ASSERT(writer->IsOpening() == false);
	TEST_ASSERT(writer->Close() == false);

	{
		auto reader = log.EnumLogItem(trans);
		TEST_ASSERT(reader);
		TEST_ASSERT(reader->GetTransaction() == trans);
		TEST_ASSERT(!log.EnumInactiveLogItem(trans));

		TEST_ASSERT(reader->NextItem() == true);
		TEST_ASSERT(reader->GetStream().Size() == 0);
		TEST_ASSERT(reader->NextItem() == false);
	}

	TEST_ASSERT(log.CloseTransaction(trans) == true);
	TEST_ASSERT(log.IsActive(trans) == false);

	{
		auto reader = log.EnumInactiveLogItem(trans);
		TEST_ASSERT(reader);
		TEST_ASSERT(reader->GetTransaction() == trans);
		TEST_ASSERT(!log.EnumLogItem(trans));

		TEST_ASSERT(reader->NextItem() == true);
		TEST_ASSERT(reader->GetStream().Size() == 0);
		TEST_ASSERT(reader->NextItem() == false);
	}
}

TEST_CASE(Utility_Log_TransactionWithOneNonEmptyItem)
{
	char data[] = "Vczh is genius!";
	const int dataLength = sizeof(data) - 1;

	BufferManager bm(4 KB, 16);
	auto source = bm.LoadFileSource(TEMP_DIR L"db.bin", true);
	LogManager log(&bm, source, true);

	auto trans = log.OpenTransaction();
	TEST_ASSERT(trans.IsValid());
	TEST_ASSERT(log.IsActive(trans) == true);

	auto writer = log.OpenLogItem(trans);
	TEST_ASSERT(writer);
	writer->GetStream().Write(data, dataLength);
	TEST_ASSERT(writer->Close() == true);
	TEST_ASSERT(writer->Close() == false);

	{
		auto reader = log.EnumLogItem(trans);
		TEST_ASSERT(reader);
		TEST_ASSERT(reader->GetTransaction() == trans);
		TEST_ASSERT(!log.EnumInactiveLogItem(trans));

		TEST_ASSERT(reader->NextItem() == true);
		TEST_ASSERT(reader->GetStream().Size() == dataLength);
		TEST_ASSERT(reader->GetStream().Read(data, dataLength) == dataLength);
		TEST_ASSERT(strcmp(data, "Vczh is genius!") == 0);
		TEST_ASSERT(reader->NextItem() == false);
	}

	TEST_ASSERT(log.CloseTransaction(trans) == true);
	TEST_ASSERT(log.IsActive(trans) == false);

	{
		auto reader = log.EnumInactiveLogItem(trans);
		TEST_ASSERT(reader);
		TEST_ASSERT(reader->GetTransaction() == trans);
		TEST_ASSERT(!log.EnumLogItem(trans));

		TEST_ASSERT(reader->NextItem() == true);
		TEST_ASSERT(reader->GetStream().Size() == dataLength);
		TEST_ASSERT(reader->GetStream().Read(data, dataLength) == dataLength);
		TEST_ASSERT(strcmp(data, "Vczh is genius!") == 0);
		TEST_ASSERT(reader->NextItem() == false);
	}
}

TEST_CASE(Utility_Log_TransactionWithMultipleItems)
{
	const char* data1 = "Vczh is genius!";
	const char* data2 = "HeroDB is a good database.";
	const char* data3 = "Visual Studio is the best IDE.";
	const char* datas[] = {data1, data2, data3};
	vint dataLengths[] = {(vint)strlen(data1), (vint)strlen(data2), (vint)strlen(data3)};
	vint dataBlocks = sizeof(datas) / sizeof(*datas);

	BufferManager bm(4 KB, 16);
	auto source = bm.LoadFileSource(TEMP_DIR L"db.bin", true);
	LogManager log(&bm, source, true);

	auto trans = log.OpenTransaction();
	TEST_ASSERT(trans.IsValid());
	TEST_ASSERT(log.IsActive(trans) == true);

	for(vint i = 0; i < dataBlocks; i++)
	{
		auto writer = log.OpenLogItem(trans);
		TEST_ASSERT(writer);
		writer->GetStream().Write((void*)datas[i], dataLengths[i]);
		TEST_ASSERT(writer->Close() == true);
		TEST_ASSERT(writer->Close() == false);
	}

	{
		auto reader = log.EnumLogItem(trans);
		TEST_ASSERT(reader);
		TEST_ASSERT(reader->GetTransaction() == trans);
		TEST_ASSERT(!log.EnumInactiveLogItem(trans));
		
		for(vint i = 0; i < dataBlocks; i++)
		{
			char buffer[32] = {0};
			TEST_ASSERT(reader->NextItem() == true);
			TEST_ASSERT(reader->GetStream().Size() == dataLengths[i]);
			TEST_ASSERT(reader->GetStream().Read(buffer, dataLengths[i]) == dataLengths[i]);
			TEST_ASSERT(strcmp(buffer, datas[i]) == 0);
		}
		TEST_ASSERT(reader->NextItem() == false);
	}

	TEST_ASSERT(log.CloseTransaction(trans) == true);
	TEST_ASSERT(log.IsActive(trans) == false);

	{
		auto reader = log.EnumInactiveLogItem(trans);
		TEST_ASSERT(reader);
		TEST_ASSERT(reader->GetTransaction() == trans);
		TEST_ASSERT(!log.EnumLogItem(trans));

		for(vint i = 0; i < dataBlocks; i++)
		{
			char buffer[32] = {0};
			TEST_ASSERT(reader->NextItem() == true);
			TEST_ASSERT(reader->GetStream().Size() == dataLengths[i]);
			TEST_ASSERT(reader->GetStream().Read(buffer, dataLengths[i]) == dataLengths[i]);
			TEST_ASSERT(strcmp(buffer, datas[i]) == 0);
		}
		TEST_ASSERT(reader->NextItem() == false);
	}
}

TEST_CASE(Utility_Log_OpenTransactionsSequencial)
{
	BufferManager bm(4 KB, 16);
	List<BufferTransaction> transes;
	List<WString> names;
	List<WString> items;

	auto source = bm.LoadFileSource(TEMP_DIR L"db.bin", true);
	LogManager log(&bm, source, true);

	TEST_ASSERT(log.GetUsedTransactionCount() == 0);
	TEST_ASSERT(log.GetTransaction(0) == BufferTransaction::Invalid());
	for (vint i = 0; i < 20; i++)
	{
		auto trans = log.OpenTransaction();
		TEST_ASSERT(log.GetUsedTransactionCount() == i + 1);
		TEST_ASSERT(log.GetTransaction(i) == trans);
		transes.Add(trans);
		names.Add(L"Transaction<" + itow(i + 1) + L">");
	}

	for (vint i = 0; i < 20; i++)
	{
		items.Add(L"This is the " + itow(i + 1) + L"-th message.");
	}
	
	for (vint i = 0; i < transes.Count(); i++)
	{
		for (vint j = 0; j < items.Count(); j++)
		{
			auto message = names[i] + L": " + items[j];
			console::Console::WriteLine(L"    Writing \"" + message + L"\".");
			auto writer = log.OpenLogItem(transes[i]);
			writer->GetStream().Write((void*)message.Buffer(), message.Length() * sizeof(wchar_t));
			writer->Close();
		}
	}

	for (vint i = 0; i < transes.Count(); i++)
	{
		auto reader = log.EnumLogItem(transes[i]);
		for (vint j = 0; j < items.Count(); j++)
		{
			auto message = names[i] + L": " + items[j];
			console::Console::WriteLine(L"    Reading \"" + message + L"\".");
			wchar_t buffer[1024] = {0};
			vint size = message.Length() * sizeof(wchar_t);
			TEST_ASSERT(reader->NextItem() == true);
			TEST_ASSERT(reader->GetStream().Size() == size);
			TEST_ASSERT(reader->GetStream().Read(buffer, size) == size);
			TEST_ASSERT(message == buffer);
		}
	}

	for (vint i = 0; i < 20; i++)
	{
		log.CloseTransaction(transes[i]);
	}

	for (vint i = 0; i < transes.Count(); i++)
	{
		auto reader = log.EnumInactiveLogItem(transes[i]);
		for (vint j = 0; j < items.Count(); j++)
		{
			auto message = names[i] + L": " + items[j];
			console::Console::WriteLine(L"    Reading \"" + message + L"\".");
			wchar_t buffer[1024] = {0};
			vint size = message.Length() * sizeof(wchar_t);
			TEST_ASSERT(reader->NextItem() == true);
			TEST_ASSERT(reader->GetStream().Size() == size);
			TEST_ASSERT(reader->GetStream().Read(buffer, size) == size);
			TEST_ASSERT(message == buffer);
		}
	}
}

TEST_CASE(Utility_Log_OpenTransactionsParallel)
{
	BufferManager bm(4 KB, 16);
	List<BufferTransaction> transes;
	List<WString> names;
	List<WString> items;

	{
		TEST_ASSERT(bm.GetCurrentlyCachedPageCount() == 0);
		auto source = bm.LoadFileSource(TEMP_DIR L"db.bin", true);
		LogManager log(&bm, source, true);

		for (vint i = 0; i < 20; i++)
		{
			transes.Add(log.OpenTransaction());
			names.Add(L"Transaction<" + itow(i + 1) + L">");
		}

		for (vint i = 0; i < 20; i++)
		{
			items.Add(L"This is the " + itow(i + 1) + L"-th message.");
		}
		
		for (vint j = 0; j < items.Count(); j++)
		{
			for (vint i = 0; i < transes.Count(); i++)
			{
				auto message = names[i] + L": " + items[j];
				console::Console::WriteLine(L"    Writing \"" + message + L"\".");
				auto writer = log.OpenLogItem(transes[i]);
				writer->GetStream().Write((void*)message.Buffer(), message.Length() * sizeof(wchar_t));
				writer->Close();
			}
		}

		for (vint i = 0; i < transes.Count(); i++)
		{
			auto reader = log.EnumLogItem(transes[i]);
			for (vint j = 0; j < items.Count(); j++)
			{
				auto message = names[i] + L": " + items[j];
				console::Console::WriteLine(L"    Reading \"" + message + L"\".");
				wchar_t buffer[1024] = {0};
				vint size = message.Length() * sizeof(wchar_t);
				TEST_ASSERT(reader->NextItem() == true);
				TEST_ASSERT(reader->GetStream().Size() == size);
				TEST_ASSERT(reader->GetStream().Read(buffer, size) == size);
				TEST_ASSERT(message == buffer);
			}
		}

		for (vint i = 0; i < transes.Count(); i++)
		{
			log.CloseTransaction(transes[i]);
		}
	}

	{
		TEST_ASSERT(bm.GetCurrentlyCachedPageCount() == 0);
		auto source = bm.LoadFileSource(TEMP_DIR L"db.bin", false);
		LogManager log(&bm, source, false);

		for (vint i = 0; i < transes.Count(); i++)
		{
			auto reader = log.EnumInactiveLogItem(transes[i]);
			for (vint j = 0; j < items.Count(); j++)
			{
				auto message = names[i] + L": " + items[j];
				console::Console::WriteLine(L"    Reading \"" + message + L"\".");
				wchar_t buffer[1024] = {0};
				vint size = message.Length() * sizeof(wchar_t);
				TEST_ASSERT(reader->NextItem() == true);
				TEST_ASSERT(reader->GetStream().Size() == size);
				TEST_ASSERT(reader->GetStream().Read(buffer, size) == size);
				TEST_ASSERT(message == buffer);
			}
		}
	}

}

TEST_CASE(Utility_Log_LongItem)
{
	BufferManager bm(4 KB, 16);
	auto source = bm.LoadFileSource(TEMP_DIR L"db.bin", true);
	LogManager log(&bm, source, true);

	vuint64_t message[1024], messageCopy[1024];
	for (vint i = 0; i < sizeof(message)/sizeof(*message); i++)
	{
		message[i] = i;
	}

	auto trans = log.OpenTransaction();
	{
		auto writer = log.OpenLogItem(trans);
		TEST_ASSERT(writer->GetStream().Write(message, sizeof(message)) == sizeof(message));
		TEST_ASSERT(writer->Close());
	}

	auto reader = log.EnumLogItem(trans);
	TEST_ASSERT(reader->NextItem() == true);
	TEST_ASSERT(reader->GetStream().Size() == sizeof(message));
	TEST_ASSERT(reader->GetStream().Read(messageCopy, sizeof(messageCopy)) == sizeof(messageCopy));
	TEST_ASSERT(memcmp(message, messageCopy, sizeof(message)) == 0);
	TEST_ASSERT(reader->NextItem() == false);
}

TEST_CASE(Utility_Log_LogTransactionItem)
{
	{
		BufferManager bm(4 KB, 16);
		auto source = bm.LoadFileSource(TEMP_DIR L"db.bin", true);
		LogAddressItem logAddressItem(&bm, source);
		logAddressItem.InitializeEmptyItems();

		for (vint i = 0; i < 1024; i++)
		{
			BufferTransaction transaction{(vuint64_t)i};
			BufferPointer address{(vuint64_t)i};
			TEST_ASSERT(logAddressItem.WriteAddressItem(transaction, address) == true);
		}

		for (vint i = 0; i < 1024; i++)
		{
			BufferTransaction transaction{(vuint64_t)i};
			BufferPointer address{(vuint64_t)i};
			TEST_ASSERT(logAddressItem.ReadAddressItem(transaction) == address);
		}
	}
	{
		BufferManager bm(4 KB, 16);
		auto source = bm.LoadFileSource(TEMP_DIR L"db.bin", false);
		LogAddressItem logAddressItem(&bm, source);
		logAddressItem.InitializeExistingItems();

		for (vint i = 0; i < 1024; i++)
		{
			BufferTransaction transaction{(vuint64_t)i};
			BufferPointer address{(vuint64_t)i};
			TEST_ASSERT(logAddressItem.ReadAddressItem(transaction) == address);
		}
	}
}
