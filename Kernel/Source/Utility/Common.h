/***********************************************************************
Vczh Library++ 3.0
Developer: Zihan Chen(vczh)
Database::Utility

***********************************************************************/

#ifndef VCZH_DATABASE_UTILITY_COMMON
#define VCZH_DATABASE_UTILITY_COMMON

#include "../DatabaseVlppReferences.h"

namespace vl
{
	namespace database
	{
		template<typename T, vint Tag>
		struct IdObject
		{
			typedef T			IndexType;
			T					index;

			IdObject()
				:index(~(T)0)
			{
			}

			IdObject(T _index)
				:index(_index)
			{
			}

			bool IsValid()const
			{
				return index != ~(T)0;
			}

			static IdObject Invalid()
			{
				return IdObject();
			}

			bool operator==(IdObject b)const { return index == b.index; }
			bool operator!=(IdObject b)const { return index != b.index; }
			bool operator< (IdObject b)const { return index <  b.index; }
			bool operator<=(IdObject b)const { return index <= b.index; }
			bool operator> (IdObject b)const { return index >  b.index; }
			bool operator>=(IdObject b)const { return index >= b.index; }
		};

		typedef IdObject<vint32_t,	0>	BufferSource;
		typedef IdObject<vuint64_t,	1>	BufferPage;
		typedef IdObject<vuint64_t,	2>	BufferPointer;
		typedef IdObject<vuint64_t,	3>	BufferTransaction;
		typedef IdObject<vint32_t,	4>	BufferTable;

		template<typename T>
		T IntUpperBound(T size, T divisor)
		{
			return size + (divisor - (size % divisor)) % divisor;
		}
	}
	
	template<typename T, vint Tag>
	struct POD<database::IdObject<T, Tag>>{static const bool Result = true;}; 
}

#endif
