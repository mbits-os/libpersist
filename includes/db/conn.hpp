/*
 * Copyright (C) 2013 midnightBITS
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef __DBCONN_H__
#define __DBCONN_H__

#include <memory>
#include <utils.hpp>
#include <list>
#include <vector>

namespace filesystem { class path; }

namespace db
{
	struct Cursor;
	struct Statement;
	struct Connection;
	typedef std::shared_ptr<Connection> ConnectionPtr;
	typedef std::shared_ptr<Statement> StatementPtr;
	typedef std::shared_ptr<Cursor> CursorPtr;

	struct Cursor
	{
		virtual ~Cursor() {}
		virtual bool next() = 0;
		virtual size_t columnCount() = 0;
		virtual int getInt(int column) = 0;
		virtual long getLong(int column) = 0;
		virtual long long getLongLong(int column) = 0;
		virtual tyme::time_t getTimestamp(int column) = 0;
		virtual const char* getText(int column) = 0;
		virtual size_t getBlobSize(int column) = 0;
		virtual const void* getBlob(int column) = 0;
		virtual bool isNull(int column) = 0;
		virtual ConnectionPtr getConnection() const = 0;
		virtual StatementPtr getStatement() const = 0;
	};

	struct time_tag {};

	template <typename Type> struct Selector;
	template <typename Type> struct Struct;

	template <>
	struct Selector<int> { static int get(const CursorPtr& c, int column) { return c->getInt(column); } };

	template <>
	struct Selector<long> { static long get(const CursorPtr& c, int column) { return c->getLong(column); } };

	template <>
	struct Selector<long long> { static long long get(const CursorPtr& c, int column) { return c->getLongLong(column); } };

	template <>
	struct Selector<time_tag> { static tyme::time_t get(const CursorPtr& c, int column) { return c->getTimestamp(column); } };

	template <>
	struct Selector<const char*> { static const char* get(const CursorPtr& c, int column) { return c->getText(column); } };

	template <>
	struct Selector<std::string> { static std::string get(const CursorPtr& c, int column) { return c->isNull(column) ? std::string() : c->getText(column); } };

	struct SelectorBase
	{
		virtual ~SelectorBase() {}
		virtual bool get(const CursorPtr& c, void* context) = 0;
	};
	typedef std::shared_ptr<SelectorBase> SelectorBasePtr;

	template <typename Type, typename Member>
	struct MemberSelector: SelectorBase
	{
		int m_column;
		Member Type::* m_member;
		MemberSelector(int column, Member Type::* member)
			: m_column(column)
			, m_member(member)
		{
		}

		bool get(const CursorPtr& c, void* context)
		{
			Type* ctx = (Type*)context;
			if (!ctx)
				return false;

			ctx->*m_member = Selector<Member>::get(c, m_column);
			return true;
		}
	};

	template <typename Type>
	struct TimeMemberSelector: SelectorBase
	{
		int m_column;
		tyme::time_t Type::* m_member;
		TimeMemberSelector(int column, tyme::time_t Type::* member)
			: m_column(column)
			, m_member(member)
		{
		}

		bool get(const CursorPtr& c, void* context)
		{
			Type* ctx = (Type*)context;
			if (!ctx)
				return false;

			ctx->*m_member = Selector<db::time_tag>::get(c, m_column);
			return true;
		}
	};

	template <typename Type>
	struct CursorStruct
	{
		std::list<SelectorBasePtr> m_selectors;

		template <typename Member>
		void add(int column, Member Type::* dest)
		{
			m_selectors.push_back(std::make_shared< MemberSelector<Type, Member> >(column, dest));
		}

		void addTime(int column, tyme::time_t Type::* dest)
		{
			m_selectors.push_back(std::make_shared< TimeMemberSelector<Type> >(column, dest));
		}

		bool get(const CursorPtr& c, Type& ctx)
		{
			for (auto&& selector : m_selectors)
			{
				if (!selector->get(c, &ctx))
					return false;
			}
			return true;
		};

		bool get(const CursorPtr& c, std::list<Type>& ctx)
		{
			while (c->next())
			{
				Type item;
				if (!get(c, item))
					return false;
				ctx.push_back(item);
			}
			return true;
		};

		bool get(const CursorPtr& c, std::vector<Type>& ctx)
		{
			while (c->next())
			{
				Type item;
				if (!get(c, item))
					return false;
				ctx.push_back(item);
			}
			return true;
		};
	};

	template <typename Type> 
	static inline bool get(const CursorPtr& c, Type& t)
	{
		return Struct<Type>().get(c, t);
	}

	template <typename Type> 
	static inline bool get(const CursorPtr& c, std::list<Type>& l)
	{
		return Struct<Type>().get(c, l);
	}

	template <typename Type> 
	static inline bool get(const CursorPtr& c, std::vector<Type>& l)
	{
		return Struct<Type>().get(c, l);
	}

	struct ErrorReporter
	{
		virtual ~ErrorReporter() {}
		virtual const char* errorMessage() = 0;
		virtual long errorCode() = 0;
	};

	struct Statement : ErrorReporter
	{
		virtual bool bind(int arg, int value) = 0;
		virtual bool bind(int arg, short value) = 0;
		virtual bool bind(int arg, long value) = 0;
		virtual bool bind(int arg, long long value) = 0;
		virtual bool bind(int arg, const char* value) = 0;
		virtual bool bind(int arg, const std::string& value) { return bind(arg, value.c_str()); }
		virtual bool bind(int arg, const void* value, size_t size) = 0;
		virtual bool bindTime(int arg, tyme::time_t value) = 0;
		virtual bool bindNull(int arg) = 0;
		virtual bool execute() = 0;
		virtual CursorPtr query() = 0;
		virtual ConnectionPtr getConnection() const = 0;
	};

	struct Connection : ErrorReporter
	{
		virtual bool isStillAlive() = 0;
		virtual bool beginTransaction() = 0;
		virtual bool rollbackTransaction() = 0;
		virtual bool commitTransaction() = 0;
		virtual bool exec(const char* sql) = 0;
		virtual StatementPtr prepare(const char* sql) = 0;
		virtual StatementPtr prepare(const char* sql, long lowLimit, long hiLimit) = 0;
		virtual bool reconnect() = 0;
		virtual std::string getURI() const = 0;
		static ConnectionPtr open(const filesystem::path& path);
	};

	struct Transaction
	{
		enum State
		{
			UNKNOWN,
			BEGAN,
			COMMITED,
			REVERTED
		};

		State m_state;
		ConnectionPtr m_conn;
		Transaction(ConnectionPtr conn): m_state(UNKNOWN), m_conn(conn) {}
		~Transaction()
		{
			if (m_state == BEGAN)
				m_conn->rollbackTransaction();
		}
		bool begin()
		{
			if (m_state != UNKNOWN)
				return false;
			if (!m_conn->beginTransaction())
				return false;
			m_state = BEGAN;
			return true;
		}
		bool commit()
		{
			if (m_state != BEGAN)
				return false;
			m_state = COMMITED;
			return m_conn->commitTransaction();
		}
		bool rollback()
		{
			if (m_state != BEGAN)
				return false;
			m_state = REVERTED;
			return m_conn->rollbackTransaction();
		}
	};

	struct environment
	{
		bool failed;
		environment();
		~environment();
	};
};

#define CURSOR_RULE(type) \
	template <> \
	struct Struct<type>: CursorStruct<type> \
	{ \
		typedef type Type; \
		Struct(); \
	}; \
	Struct<type>::Struct()
#define CURSOR_ADD(column, name) add(column, &Type::name)
#define CURSOR_TIME(column, name) addTime(column, &Type::name)

#endif //__DBCONN_H__
