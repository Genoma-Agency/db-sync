/*
 * db-sync Copyright (C) 2024 Marco Benuzzi
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <main.h>
#include <soci/soci.h>

namespace dbsync {

extern const std::string SQL_NULL_STRING;
extern const std::string SQL_MD5_CHECK;

/*****************************************************************************/

struct ColumnInfo {
  std::string name;
  std::string type;
  bool nullable;
  bool primaryKey;
  bool operator==(const ColumnInfo&) const = default;
};

std::ostream& operator<<(std::ostream& stream, const ColumnInfo& var);

/*****************************************************************************/

struct TableInfo {
  std::vector<ColumnInfo> columns;
};

std::ostream& operator<<(std::ostream& stream, const TableInfo& var);

using MetadataMap = std::map<std::string, TableInfo>;

/*****************************************************************************/

class Field {
public:
  Field(const soci::row& row, const std::size_t i);
  Field(const Field&) = delete;
  Field(Field&&) = delete;
  Field& operator=(const Field&) = delete;
  Field& operator=(Field&&) = delete;
  std::partial_ordering operator<=>(const Field& other) const;
  const soci::data_type& type() const { return dType; };
  const soci::indicator& indicator() const { return dIndicator; };
  bool isString() const { return dType == soci::dt_string || dType == soci::dt_xml || dType == soci::dt_blob; }
  bool isNull() const { return dIndicator == soci::i_null; }
  const std::string toString() const;
  const std::string& asString() const { return value.string; };
  const double& asDouble() const { return value.number.decimal; };
  const int& asInt() const { return value.number.integer; };
  const long long& asLongLong() const { return value.number.longLong; };
  const unsigned long long& asULongLong() const { return value.number.uLongLong; };
  DbValue asVariant() const;

private:
  soci::data_type dType;
  soci::indicator dIndicator;
  struct {
    std::string string;
    union {
      std::time_t epoch;
      double decimal;
      int integer;
      long long longLong;
      unsigned long long uLongLong;
    } number;
  } value;
};

/*****************************************************************************/

class TableKeys;
class TableKeysIterator;
class TableData;
class TableRow;

class Db {

public:
  Db(const std::string ref);
  ~Db();
  bool
  open(const std::string& host, int port, const std::string& schema, const std::string& user, const std::string& pwd);
  bool loadTables(strings& tables);
  bool loadMetadata(std::set<std::string> tables);
  const std::string& reference() const { return ref; }
  const std::string& lastError() const { return error; }
  const MetadataMap& metadata() const { return map; };
  void logTableInfo() const;
  void transactionBegin();
  void transactionCommit();
  bool loadPk(bool source, const std::string& table, TableKeys& data, std::size_t bulk);
  bool query(const std::string& sql, TableData& data);
  bool query(const std::string& sql, std::function<void(const soci::row&)> consumer);
  bool exec(const std::string& sql);
  bool insertPrepare(const std::string& table);
  bool insertExecute(const std::string& table, const std::unique_ptr<TableRow>& row);
  bool updatePrepare(const std::string& table, const strings& keys, const strings& fields);
  bool updateExecute(const std::string& table, const std::unique_ptr<TableRow>& row);
  bool deletePrepare(const std::string& table, const strings& keys);
  bool deleteExecute(const std::string& table, const TableKeys& keys, long index);
  bool comparePrepare(const std::string& table, const std::size_t bulk);
  bool selectPrepare(const std::string& table, const strings& keys, const std::size_t bulk);
  bool selectExecute(const std::string& table, const TableKeys& keys, TableKeysIterator& iter, TableData& into);
  soci::details::session_backend* backend() { return session->get_backend(); };

private:
  bool apply(const std::string& opDesc, std::function<void(void)> lambda, std::function<void(void)> finally = nullptr);
  void bind(std::optional<soci::statement>& stmt,
            const std::unique_ptr<TableRow>& row,
            const int startIndex,
            const int endIndex);

private:
  const std::string ref;
  std::unique_ptr<soci::session> session;
  std::optional<soci::transaction> tx;
  MetadataMap map;
  log4cxx::LoggerPtr log;
  std::string schema;
  std::optional<soci::statement> stmtRead;
  std::optional<soci::statement> stmtWrite;
  std::size_t readCount;
  int keysCount;
  std::string error;

private:
  static const std::string SQL_TABLES;
  static const std::string SQL_COLUMNS;
};
}

template <> struct fmt::formatter<dbsync::TableInfo> : ostream_formatter {};
template <> struct fmt::formatter<dbsync::ColumnInfo> : ostream_formatter {};

namespace soci {
std::ostream& operator<<(std::ostream& stream, const data_type& var);
std::ostream& operator<<(std::ostream& stream, const indicator& var);
std::ostream& operator<<(std::ostream& stream, const row& var);
}

template <> struct fmt::formatter<soci::data_type> : ostream_formatter {};
template <> struct fmt::formatter<soci::indicator> : ostream_formatter {};
template <> struct fmt::formatter<soci::row> : ostream_formatter {};
