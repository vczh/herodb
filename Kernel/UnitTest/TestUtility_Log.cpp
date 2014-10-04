#include "UnitTest.h"
#include "../Source/Utility/Log.h"

using namespace vl;
using namespace vl::database;
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
	TEST_ASSERT(reader->GetTransaction().index == trans.index);
	TEST_ASSERT(!log.EnumInactiveLogItem(trans));
	TEST_ASSERT(reader->NextItem() == false);

	TEST_ASSERT(log.CloseTransaction(trans) == true);
	TEST_ASSERT(log.IsActive(trans) == false);

	reader = log.EnumInactiveLogItem(trans);
	TEST_ASSERT(reader);
	TEST_ASSERT(reader->GetTransaction().index == trans.index);
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
	TEST_ASSERT(writer->Close() == true);
	TEST_ASSERT(writer->Close() == false);

	{
		auto reader = log.EnumLogItem(trans);
		TEST_ASSERT(reader);
		TEST_ASSERT(reader->GetTransaction().index == trans.index);
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
		TEST_ASSERT(reader->GetTransaction().index == trans.index);
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
		TEST_ASSERT(reader->GetTransaction().index == trans.index);
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
		TEST_ASSERT(reader->GetTransaction().index == trans.index);
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
}

TEST_CASE(Utility_Log_OpenTransactionsSequencial)
{
}

TEST_CASE(Utility_Log_OpenTransactionsParallel)
{
}

TEST_CASE(Utility_Log_OpenInactiveTransaction)
{
}

TEST_CASE(Utility_Log_LoadExistingLog)
{
}
