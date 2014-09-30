#include "UnitTest.h"
#include "../Source/Utility/Buffer.h"

using namespace vl;
using namespace vl::database;
using namespace vl::collections;

extern WString GetTempFolder();
#define TEMP_DIR GetTempFolder()+
#define KB *1024
#define MB *1024*1024

TEST_CASE(Utility_Log_TransactionWithNoItem)
{
}

TEST_CASE(Utility_Log_TransactionWithOneEmptyItem)
{
}

TEST_CASE(Utility_Log_TransactionWithOneNonEmptyItem)
{
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
