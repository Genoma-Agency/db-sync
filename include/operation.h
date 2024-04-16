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

#include <db.h>
#include <main.h>
#include <utils.hxx>

namespace dbsync {

enum Mode { Copy, Sync };

std::ostream& operator<<(std::ostream& stream, const Mode& var);

/*****************************************************************************/

struct OperationConfig {
  Mode mode;
  bool update;
  bool dryRun;
  strings& tables;
  bool disableBinLog;
};

std::ostream& operator<<(std::ostream& stream, const OperationConfig& var);

/*****************************************************************************/

class BaseData {
public:
  std::partial_ordering operator<=>(const BaseData& other) const = default;

protected:
  static log4cxx::LoggerPtr log;
};

/*****************************************************************************/

class Operation : public BaseData {
public:
  Operation(const OperationConfig& config, std::unique_ptr<Db> fromDb, std::unique_ptr<Db> toDb) noexcept;
  ~Operation();
  bool checkMetadata();
  bool preExecute();
  bool execute();
  bool postExecute(bool executeOk);

private:
  bool checkMetadataTables();
  bool checkMetadataColumns(const std::string& table);
  bool execute(const std::string& table);
  std::string buildSqlKeys(const std::string& table) const;
  util::Timer<> timerGlobal;
  util::Timer<> timerTable;

private:
  const OperationConfig& config;
  std::unique_ptr<Db> fromDb;
  std::unique_ptr<Db> toDb;
  std::set<std::string> tables;
};

/*****************************************************************************/

class TableRow : public BaseData {
public:
  TableRow(const soci::row& row, bool updateCheck = false);
  TableRow(const TableRow&) = delete;
  TableRow(TableRow&&) = delete;
  TableRow& operator=(const TableRow&) = delete;
  TableRow& operator=(TableRow&&) = delete;
  std::partial_ordering operator<=>(const TableRow& other) const;
  const bool hasUpdateCheck() const { return updateCheck; };
  const std::unique_ptr<Field>& at(int index) const { return fields.at(index); };
  std::string toString(const strings& names) const;
  size_t size() const { return fields.size(); }
  void rotate(const int moveCount);

private:
  const bool updateCheck;
  std::vector<std::unique_ptr<Field>> fields;
};

/*****************************************************************************/

class TableData : public BaseData {
public:
  TableData(const bool source, const std::string& table, const size_t sizeHint, bool updateCheck = false);
  TableData(const TableData&) = delete;
  TableData(TableData&&) = delete;
  TableData& operator=(const TableData&) = delete;
  TableData& operator=(TableData&&) = delete;
  void clear();
  void loadRow(const soci::row& row);
  const bool hasUpdateCheck() const { return updateCheck; };
  const std::unique_ptr<TableRow>& at(int index) const { return rows.at(index); };
  size_t size() const { return rows.size(); }
  bool empty() const { return rows.empty(); }
  const strings& columnNames() const { return names; };

private:
  const std::string ref;
  const bool updateCheck;
  strings names;
  std::vector<std::unique_ptr<TableRow>> rows;
};

/*****************************************************************************/

class TableDiff : public BaseData {
public:
  TableDiff(TableData& src, TableData& dest) noexcept;
  const std::vector<int>& onlySrcIndexes() const { return onlySrc; };
  const std::vector<int>& commonIndexes() const { return common; };
  const std::vector<int>& updateIndexes() const { return update; };
  const std::vector<int>& onlyDestIndexes() const { return onlyDest; };

private:
  std::vector<int> onlySrc;
  std::vector<int> common;
  std::vector<int> update;
  std::vector<int> onlyDest;
};

/*****************************************************************************/
}

template <> struct fmt::formatter<dbsync::Mode> : ostream_formatter {};
template <> struct fmt::formatter<dbsync::OperationConfig> : ostream_formatter {};
