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

#include "pch.h"
#include "mysql.hpp"
#include <utils.hpp>
#include <sstream>

extern "C" void flog(const char* file, int line, const char* fmt, ...);
#define MYSQL_LOG(...) ::flog(__FILE__, __LINE__, __VA_ARGS__)

namespace db { namespace mysql {
	bool startup_driver()
	{
		REGISTER_DRIVER("mysql", db::mysql::MySQLDriver);
		return mysql_library_init(0, nullptr, nullptr) == 0;
	}

	void shutdown_driver()
	{
		mysql_library_end();
	}

	struct DriverData
	{
		std::string user;
		std::string password;
		std::string server;
		std::string database;
		bool read(const Driver::Props& props)
		{
			return 
				Driver::getProp(props, "user", user) &&
				Driver::getProp(props, "password", password) &&
				Driver::getProp(props, "server", server) &&
				Driver::getProp(props, "database", database) &&
				!user.empty() && !password.empty() && !server.empty() && !database.empty();
		}
	};

	ConnectionPtr MySQLDriver::open(const filesystem::path& ini_path, const Props& props)
	{
		DriverData data;
		if (!data.read(props))
		{
			MYSQL_LOG("[MySQL] invalid configuration");
			return nullptr;
		}

		//std::cerr << "user: " << user << "\npassword: " << password << "\naddress: " << server << std::endl;

		try {
			auto conn = std::make_shared<MySQLConnection>(ini_path);

			if (!conn->connect(data.user, data.password, data.server, data.database))
			{
				MYSQL_LOG("[MySQL] cannot connect to %s@%s", data.user.c_str(), data.server.c_str());
				return nullptr;
			}

			MYSQL_LOG("[MySQL] connected to %s@%s", data.user.c_str(), data.server.c_str());
			return conn;
		} catch(std::bad_alloc) { return nullptr; }
	}

	MySQLConnection::MySQLConnection(const filesystem::path& path)
		: m_connected(false)
		, m_path(path)
	{
		mysql_init(&m_mysql);
	}

	MySQLConnection::~MySQLConnection()
	{
		if (m_connected)
			mysql_close(&m_mysql);
	}

	bool MySQLConnection::connect(const std::string& user, const std::string& password, const std::string& server, const std::string& database)
	{
		m_fake_uri = "mysql://";
		std::string srvr = server;
		unsigned int port = 0;
		std::string::size_type colon = srvr.rfind(':');
		if (colon != std::string::npos)
		{
			char* ptr;
			port = strtoul(srvr.c_str() + colon + 1, &ptr, 10);
			if (*ptr)
				return m_connected;

			srvr = srvr.substr(0, colon);
		}

		my_bool reconnect = 0;
		mysql_options(&m_mysql, MYSQL_OPT_RECONNECT, &reconnect);

		m_connected = mysql_real_connect(&m_mysql, srvr.c_str(), user.c_str(), password.c_str(), database.c_str(), port, nullptr, 0) != nullptr;

		if (m_connected)
			m_fake_uri = "mysql://" + user + "@" + server + "/" + database;

		return m_connected;
	}

	bool MySQLConnection::reconnect()
	{
		Driver::Props props;
		if (!Driver::readProps(m_path, props))
			return false;

		DriverData data;
		if (!data.read(props))
			return false;

		return connect(data.user, data.password, data.server, data.database);
	}

	bool MySQLConnection::isStillAlive()
	{
		return mysql_ping(&m_mysql) == 0;
	}

	bool MySQLConnection::beginTransaction()
	{
		return mysql_query(&m_mysql, "START TRANSACTION") == 0;
	}

	bool MySQLConnection::rollbackTransaction()
	{
		return mysql_query(&m_mysql, "ROLLBACK") == 0;
	}

	bool MySQLConnection::commitTransaction()
	{
		return mysql_query(&m_mysql, "COMMIT") == 0;
	}

	StatementPtr MySQLConnection::prepare(const char* sql)
	{
		MYSQL_STMT * stmtptr = mysql_stmt_init(&m_mysql);
		if (stmtptr == nullptr)
			return nullptr;

		try {
			auto stmt = std::make_shared<MySQLStatement>(&m_mysql, stmtptr, shared_from_this());

			if (!stmt->prepare(sql))
				return nullptr;

			return stmt;
		} catch(std::bad_alloc) { return nullptr; }
	}

	StatementPtr MySQLConnection::prepare(const char* sql, long lowLimit, long hiLimit)
	{
		std::ostringstream s;
		s << sql << " LIMIT " << lowLimit << ", " << hiLimit;
		return prepare(s.str().c_str());
	}

	bool MySQLConnection::exec(const char* sql)
	{
		return mysql_query(&m_mysql, sql) == 0;
	}

	const char* MySQLConnection::errorMessage()
	{
		return mysql_error(&m_mysql);
	}

	long MySQLConnection::errorCode()
	{
		return mysql_errno(&m_mysql);
	}

	bool MySQLStatement::prepare(const char* stmt)
	{
		if (!stmt) return false;
		int rc = mysql_stmt_prepare(m_stmt, stmt, strlen(stmt));
		//if (rc == 1)
		//	std::cerr << "MySQL: " << mysql_stmt_errno(m_stmt) << ": "
		//		<< mysql_stmt_error(m_stmt) << std::endl;
		if (rc != 0)
			return false;

		if (!allocBind(mysql_stmt_param_count(m_stmt)))
			return false;

		rc = mysql_stmt_bind_param(m_stmt, m_bind);
		//if (rc == 1)
		//	std::cerr << "MySQL: " << mysql_stmt_errno(m_stmt) << ": "
		//		<< mysql_stmt_error(m_stmt) << std::endl;
		return rc == 0;
	}

	bool MySQLStatement::bind(int arg, short value)
	{
		if (!bindImpl(arg, value))
			return false;
		m_bind[arg].buffer_type = MYSQL_TYPE_SHORT;
		return true;
	}

	bool MySQLStatement::bind(int arg, long value)
	{
		if (!bindImpl(arg, value))
			return false;
		m_bind[arg].buffer_type = MYSQL_TYPE_LONG;
		return true;
	}

	bool MySQLStatement::bind(int arg, long long value)
	{
		if (!bindImpl(arg, value))
			return false;
		m_bind[arg].buffer_type = MYSQL_TYPE_LONGLONG;
		return true;
	}

	bool MySQLStatement::bind(int arg, const char* value)
	{
		if (!value)
			return bindNull(arg);

		size_t len = strlen(value);
		if (!bindImpl(arg, value, len))
			return false;

		strncpy(m_buffers[arg], value, len);
		m_bind[arg].buffer_type = MYSQL_TYPE_STRING;
		return true;
	}

	bool MySQLStatement::bind(int arg, const void* value, size_t size)
	{
		if (!value)
			return bindNull(arg);

		if (!bindImpl(arg, value, size))
			return false;

		memcpy(m_buffers[arg], value, size);
		m_bind[arg].buffer_type = MYSQL_TYPE_BLOB;
		return true;
	}

	bool MySQLStatement::bindTime(int arg, tyme::time_t value)
	{
		tyme::tm_t tm = tyme::gmtime(value);
		MYSQL_TIME time = {};
		time.year   = tm.tm_year + 1900;
		time.month  = tm.tm_mon + 1;
		time.day    = tm.tm_mday;
		time.hour   = tm.tm_hour;
		time.minute = tm.tm_min;
		time.second = tm.tm_sec;
		time.time_type = MYSQL_TIMESTAMP_DATETIME;
		if (!bindImpl(arg, time))
			return false;
		m_bind[arg].buffer_type = MYSQL_TYPE_TIMESTAMP;
		return true;
	}

	bool MySQLStatement::bindNull(int arg)
	{
		if ((size_t)arg >= m_count)
		{
			MYSQL_LOG("[MySQL/Bind] Argument out of bounds (size:%d / index:%d)", (int)m_count, arg);
			return false;
		}

		delete [] m_buffers[arg];
		m_buffers[arg] = nullptr;

		m_bind[arg].buffer = nullptr;
		m_bind[arg].buffer_length = 0;
		m_bind[arg].buffer_type = MYSQL_TYPE_NULL;

		return true;
	}

	bool MySQLStatement::bindImpl(int arg, const void* value, size_t len)
	{
		if ((size_t)arg >= m_count)
		{
			MYSQL_LOG("[MySQL/Bind] Argument out of bounds (size:%d / index:%d)", (int)m_count, arg);
			return false;
		}

		delete [] m_buffers[arg];
		m_buffers[arg] = new (std::nothrow) char[len];
		if (!m_buffers[arg])
			return false;

		m_bind[arg].buffer = m_buffers[arg];
		m_bind[arg].buffer_length = len;
		return true;
	}

	bool MySQLStatement::execute()
	{
		if (mysql_stmt_bind_param(m_stmt, m_bind) != 0)
			return false;
		return mysql_stmt_execute(m_stmt) == 0;
	}

	CursorPtr MySQLStatement::query()
	{
		if (mysql_stmt_bind_param(m_stmt, m_bind) != 0)
			return false;

		unsigned long type = CURSOR_TYPE_READ_ONLY;
		if (mysql_stmt_attr_set(m_stmt, STMT_ATTR_CURSOR_TYPE, (void*) &type) != 0)
			return false;

		if (mysql_stmt_execute(m_stmt) != 0)
			return false;

		try {
			auto cursor = std::make_shared<MySQLCursor>(m_mysql, m_stmt, shared_from_this());

			if (!cursor->prepare())
				return nullptr;

			return cursor;
		} catch(std::bad_alloc) { return nullptr; }
	}

	const char* MySQLStatement::errorMessage()
	{
		return mysql_stmt_error(m_stmt);
	}

	long MySQLStatement::errorCode()
	{
		return mysql_stmt_errno(m_stmt);
	}

	bool MySQLCursor::allocBind(size_t count)
	{
		if (!MySQLBinding::allocBind(count))
			return false;

		unsigned long *lengths = new (std::nothrow) unsigned long[m_count];
		my_bool *is_null = new (std::nothrow) my_bool[m_count];
		my_bool *error = new (std::nothrow) my_bool[m_count];
		if (!lengths || !is_null || !error)
		{
			delete [] lengths;
			delete [] is_null;
			delete [] error;
			return false;
		}

		deleteBind();

		m_lengths = lengths;
		m_is_null = is_null;
		m_error = error;

		memset(lengths, 0, sizeof(lengths[0]) * count);
		memset(is_null, 0, sizeof(is_null[0]) * count);
		memset(error, 0, sizeof(error[0]) * count);

		for (size_t i = 0; i < m_count; ++i)
		{
			m_bind[i].length = &m_lengths[i];
			m_bind[i].is_null = &m_is_null[i];
			m_bind[i].error = &m_error[i];
		}

		return true;
	}

	static size_t fieldSize(enum_field_types fld_type)
	{
		switch(fld_type)
		{
		case MYSQL_TYPE_TINY:     return sizeof(char);
		case MYSQL_TYPE_SHORT:    return sizeof(short);
		case MYSQL_TYPE_INT24:    return sizeof(long);
		case MYSQL_TYPE_LONG:     return sizeof(long);
		case MYSQL_TYPE_LONGLONG: return sizeof(long long);
		case MYSQL_TYPE_FLOAT:    return sizeof(float);
		case MYSQL_TYPE_DOUBLE:   return sizeof(double);
		case MYSQL_TYPE_NULL:     return 0;

		case MYSQL_TYPE_YEAR:     return sizeof(short);
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_TIME:
		case MYSQL_TYPE_DATETIME:
			return sizeof(MYSQL_TIME);

		case MYSQL_TYPE_NEWDECIMAL:
		case MYSQL_TYPE_VARCHAR:
		case MYSQL_TYPE_BIT:
		case MYSQL_TYPE_TINY_BLOB:
		case MYSQL_TYPE_MEDIUM_BLOB:
		case MYSQL_TYPE_LONG_BLOB:
		case MYSQL_TYPE_BLOB:
		case MYSQL_TYPE_VAR_STRING:
		case MYSQL_TYPE_STRING:
			return 0; // char[] types
		default:
			break;
		}
		return (size_t)-1;
	}

	bool MySQLCursor::bindResult(const MYSQL_FIELD& field, MYSQL_BIND& bind, char*& buffer)
	{
		size_t size = fieldSize(field.type);
		if (size == (size_t)-1)
			return false;

		bind.buffer_type = field.type;
		bind.buffer_length = 0;
		bind.buffer = buffer;  //?

		if (size == 0)
			return true;

		delete [] buffer;
		buffer = new (std::nothrow) char[size];
		if (!buffer)
			return false;

		bind.buffer = buffer;  //?
		bind.buffer_length = size;
		return true;
	}

	bool MySQLCursor::prepare()
	{
		MYSQL_RES *meta = mysql_stmt_result_metadata(m_stmt);
		if (!meta)
			return false;

		if (!allocBind(mysql_num_fields(meta)))
			return false;

		MYSQL_FIELD* fields = mysql_fetch_fields(meta);

		for (size_t i = 0; i < m_count; ++i)
			if (!bindResult(fields[i], m_bind[i], m_buffers[i]))
				return false;

		if (mysql_stmt_bind_result(m_stmt, m_bind) != 0)
			return false;

		return true;
	}

	bool MySQLCursor::next()
	{
		int rc = mysql_stmt_fetch(m_stmt);
		//if (rc == 1)
		//	std::cerr << "MySQL: " << mysql_stmt_errno(m_stmt) << ": "
		//		<< mysql_stmt_error(m_stmt) << std::endl;
		if (rc == MYSQL_DATA_TRUNCATED)
			rc = 0; // getXxx will take care of truncations...
		return rc == 0;
	}

	size_t MySQLCursor::columnCount()
	{
		return m_count;
	}

	template <typename T>
	T getIntType(MYSQL_STMT* stmt, int column, enum_field_types type)
	{
		T ret = 0;
		MYSQL_BIND bind = {};
		bind.buffer_type = type;
		bind.buffer = &ret;
		if (mysql_stmt_fetch_column(stmt, &bind, column, 0) != 0)
			return 0;
		return ret;
	}

	long MySQLCursor::getLong(int column)
	{
		if ((size_t)column >= m_count)
		{
			MYSQL_LOG("[MySQL/getLong] Argument out of bounds (size:%d / index:%d)", (int)m_count, column);
			return false;
		}

		if (m_is_null[column])
			return 0;

		return getIntType<long>(m_stmt, column, MYSQL_TYPE_LONG);
	}

	long long MySQLCursor::getLongLong(int column)
	{
		if ((size_t)column >= m_count)
		{
			MYSQL_LOG("[MySQL/getLongLong] Argument out of bounds (size:%d / index:%d)", (int)m_count, column);
			return false;
		}

		if (m_is_null[column])
			return 0;

		return getIntType<long long>(m_stmt, column, MYSQL_TYPE_LONGLONG);
	}

	tyme::time_t MySQLCursor::getTimestamp(int column)
	{
		if ((size_t)column >= m_count)
		{
			MYSQL_LOG("[MySQL/getTimestamp] Argument out of bounds (size:%d / index:%d)", (int)m_count, column);
			return false;
		}

		if (m_is_null[column])
			return 0;

		MYSQL_TIME time = {};
		MYSQL_BIND bind = {};
		bind.buffer_type = MYSQL_TYPE_TIMESTAMP;
		bind.buffer = &time;
		if (mysql_stmt_fetch_column(m_stmt, &bind, column, 0) != 0)
			return 0;

		tyme::tm_t tm = {};
		tm.tm_year = time.year - 1900;
		tm.tm_mon  = time.month - 1;
		tm.tm_mday = time.day;
		tm.tm_hour = time.hour;
		tm.tm_min  = time.minute;
		tm.tm_sec  = time.second;
		return tyme::mktime(tm);
	}

	const char* MySQLCursor::getText(int column)
	{
		if ((size_t)column >= m_count)
		{
			MYSQL_LOG("[MySQL/getText] Argument out of bounds (size:%d / index:%d)", (int)m_count, column);
			return nullptr;
		}

		if (m_is_null[column])
			return nullptr;

		if (m_error[column] || !m_buffers[column]) // the field would have been truncated
		{
			char* buffer = new (std::nothrow) char[m_lengths[column] + 1];
			if (!buffer)
				return nullptr;

			delete [] m_buffers[column];
			m_buffers[column] = buffer;

			m_bind[column].buffer_type = MYSQL_TYPE_STRING;
			m_bind[column].buffer = m_buffers[column];
			m_bind[column].buffer_length = m_lengths[column];
		}

		MYSQL_BIND bind = {};
		bind.buffer_type = MYSQL_TYPE_STRING;
		bind.buffer = m_buffers[column];
		bind.buffer_length = m_lengths[column];

		if (mysql_stmt_fetch_column(m_stmt, &bind, column, 0) != 0)
			return nullptr;

		m_buffers[column][m_lengths[column]] = 0;
		return m_buffers[column];
	}

	size_t MySQLCursor::getBlobSize(int column)
	{
		if ((size_t)column >= m_count)
		{
			MYSQL_LOG("[MySQL/getBlobSize] Argument out of bounds (size:%d / index:%d)", (int)m_count, column);
			return 0;
		}

		return m_lengths[column];
	}

	const void* MySQLCursor::getBlob(int column)
	{
		if ((size_t)column >= m_count)
		{
			MYSQL_LOG("[MySQL/getBlob] Argument out of bounds (size:%d / index:%d)", (int)m_count, column);
			return nullptr;
		}

		if (m_is_null[column])
			return nullptr;

		if (m_error[column] || !m_buffers[column]) // the field would have been truncated
		{
			char* buffer = new (std::nothrow) char[m_lengths[column] + 1];
			if (!buffer)
				return nullptr;

			delete[] m_buffers[column];
			m_buffers[column] = buffer;

			m_bind[column].buffer_type = MYSQL_TYPE_BLOB;
			m_bind[column].buffer = m_buffers[column];
			m_bind[column].buffer_length = m_lengths[column];
		}

		MYSQL_BIND bind = {};
		bind.buffer_type = MYSQL_TYPE_BLOB;
		bind.buffer = m_buffers[column];
		bind.buffer_length = m_lengths[column];

		if (mysql_stmt_fetch_column(m_stmt, &bind, column, 0) != 0)
			return nullptr;

		m_buffers[column][m_lengths[column]] = 0;
		return m_buffers[column];
	}

	bool MySQLCursor::isNull(int column)
	{
		if ((size_t)column >= m_count)
		{
			MYSQL_LOG("[MySQL/isNull] Argument out of bounds (size:%d / index:%d)", (int)m_count, column);
			return true;
		}

		return m_is_null[column] != 0;
	}

}}
