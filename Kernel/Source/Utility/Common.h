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

			static T Compare(IdObject a, IdObject b)
			{
				return a.index - b.index;
			}

			bool operator==(IdObject b)const { return Compare(*this, b) == 0; }
			bool operator!=(IdObject b)const { return Compare(*this, b) != 0; }
			bool operator< (IdObject b)const { return Compare(*this, b) <  0; }
			bool operator<=(IdObject b)const { return Compare(*this, b) <= 0; }
			bool operator> (IdObject b)const { return Compare(*this, b) >  0; }
			bool operator>=(IdObject b)const { return Compare(*this, b) >= 0; }
		};

		typedef IdObject<vint32_t,	0>	BufferSource;
		typedef IdObject<vuint64_t,	1>	BufferPage;
		typedef IdObject<vuint64_t,	2>	BufferPointer;
		typedef IdObject<vuint64_t,	3>	BufferTransaction;
		typedef IdObject<vint32_t,	4>	BufferTable;
		typedef IdObject<vuint64_t,	4>	BufferTask;

		template<typename T>
		T IntUpperBound(T size, T divisor)
		{
			return size + (divisor - (size % divisor)) % divisor;
		}
	}
}

#endif
