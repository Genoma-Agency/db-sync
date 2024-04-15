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
#include <soci/mysql/soci-mysql.h>
#include <soci/soci.h>

namespace dbsync {

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
  size_t rowCount;
  std::vector<ColumnInfo> columns;
};

std::ostream& operator<<(std::ostream& stream, const TableInfo& var);

using MetadataMap = std::map<std::string, TableInfo>;

/*****************************************************************************/

class Field {
public:
  using value_t = std::variant<std::string, std::time_t, int, long long, unsigned long long, double>;
  Field(const soci::row& row, const std::size_t i);
  const soci::data_type type() const { return dType; };
  const soci::indicator& indicator() const { return dIndicator; };
  bool isNull() const { return dIndicator == soci::i_null; }
  const value_t& variant() const { return value; }
  const std::string& toString() const { return valueString; };
  std::partial_ordering operator<=>(const Field& other) const = default;

private:
  soci::data_type dType;
  soci::indicator dIndicator;
  value_t value;
  std::string valueString;
};

/*****************************************************************************/

class TableData;
class TableRow;

class Db {

public:
  Db(const std::string ref);
  ~Db();
  bool
  open(const std::string& host, int port, const std::string& schema, const std::string& user, const std::string& pwd);
  bool readMetadata();
  const std::string& reference() const { return ref; }
  const std::string& lastError() const { return error; }
  const MetadataMap& metadata() const { return map; };
  void logTableInfo() const;
  bool query(const std::string& sql, TableData& data);
  bool query(const std::string& sql, std::function<void(const soci::row&)> consumer);
  bool exec(const std::string& sql);
  bool insertPrepare(const std::string& table);
  bool insertExecute(const std::string& table, const TableRow& row);
  bool selectPrepare(const std::string& table, const std::vector<std::string>& names);
  bool selectExecute(const std::string& table, const TableRow& row, TableData& into);
  soci::details::session_backend* backend() { return session->get_backend(); };

private:
  bool apply(const std::string& opDesc, std::function<void(void)> lambda);
  void bind(std::optional<soci::statement>& stmt, const TableRow& row);

private:
  const std::string ref;
  std::unique_ptr<soci::session> session;
  MetadataMap map;
  log4cxx::LoggerPtr log;
  std::string schema;
  std::optional<soci::statement> stmtInsert;
  std::optional<soci::statement> stmtSelect;
  soci::row rowSelect;
  std::string error;

private:
  static const std::string SQL_COLUMNS;
};
}

template <> struct fmt::formatter<dbsync::TableInfo> : ostream_formatter {};
template <> struct fmt::formatter<dbsync::ColumnInfo> : ostream_formatter {};

namespace soci {
std::ostream& operator<<(std::ostream& stream, const data_type& var);
std::ostream& operator<<(std::ostream& stream, const indicator& var);
}

template <> struct fmt::formatter<soci::data_type> : ostream_formatter {};
template <> struct fmt::formatter<soci::indicator> : ostream_formatter {};
