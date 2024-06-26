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

namespace dbsync {

enum Mode { Copy, Sync };

std::ostream& operator<<(std::ostream& stream, const Mode& var);

/*****************************************************************************/

class stop_request : public std::runtime_error {
public:
  stop_request()
      : std::runtime_error{ "stop requested" } {}
};

/*****************************************************************************/

struct OperationConfig {
  Mode mode;
  bool update;
  bool dryRun;
  strings& tables;
  bool disableBinLog;
  bool noFail;
  std::size_t pkBulk;
  std::size_t compareBulk;
  std::size_t modifyBulk;
};

std::ostream& operator<<(std::ostream& stream, const OperationConfig& var);

/*****************************************************************************/

class Operation {
public:
  Operation(const OperationConfig& config,
            std::shared_ptr<dbsync::DbMeta> fromDb,
            std::shared_ptr<dbsync::DbMeta> toDb) noexcept;
  ~Operation(){};
  const OperationConfig& configuration() const { return config; };
  std::shared_ptr<dbsync::DbMeta> source() const { return fromDb; }
  std::shared_ptr<dbsync::DbMeta> target() const { return toDb; }
  bool checkTables(const strings& src, const strings& dest);
  bool checkMetadata();
  void addRw(const std::size_t inc) { dbRw += inc; }
  bool canRun() const { return run.load(); }
  void checkRun() const;
  void stop();
  std::size_t rwCount() const { return dbRw.load(); }
  int tablesCount() const { return tables.size(); }
  std::string tableToProcess();

private:
  bool checkMetadataColumns(const std::string& table);

private:
  const OperationConfig& config;
  std::shared_ptr<dbsync::DbMeta> fromDb;
  std::shared_ptr<dbsync::DbMeta> toDb;
  std::set<std::string> tables;
  log4cxx::LoggerPtr log;
  std::atomic_size_t dbRw;
  std::atomic_bool run;
  std::mutex mutex;
};

/*****************************************************************************/

class OpJob {
public:
  OpJob(std::shared_ptr<dbsync::Operation> manager) noexcept;
  OpJob(OpJob&&) = default;
  ~OpJob(){};
  bool init();
  void execute();
  bool result() const { return ret; }
  bool isRunning() const { return run; }

private:
  bool execute(const std::string& table);
  bool executeAdd(const std::string& table, TableKeys& srcKeys, std::size_t total);
  bool executeUpdate(const std::string& table, TableKeys& srcKeys, std::size_t total);
  bool executeDelete(const std::string& table, TableKeys& destKeys, std::size_t total);
  std::string buildSqlKeys(const std::string& table) const;
  std::tuple<std::size_t, std::size_t, std::size_t>
  compareKeys(const std::string& table, TableKeys& srcKeys, TableKeys& destKeys);
  bool feedback(const std::size_t count, const std::size_t bulk, const std::size_t total) const;

private:
  std::shared_ptr<dbsync::Operation> manager;
  std::unique_ptr<Db> fromDb;
  std::unique_ptr<Db> toDb;
  log4cxx::LoggerPtr log;
  bool ret;
  bool run;
};

/*****************************************************************************/

class TableRow {
public:
  TableRow(const soci::row& row, const bool& updateCheck);
  TableRow(const TableRow&) = delete;
  TableRow(TableRow&&) = delete;
  TableRow& operator=(const TableRow&) = delete;
  TableRow& operator=(TableRow&&) = delete;
  std::partial_ordering operator<=>(const TableRow& other) const;
  const bool hasUpdateCheck() const { return updateCheck; };
  const std::unique_ptr<Field>& at(int index) const { return fields.at(index); };
  const std::unique_ptr<Field>& checkValue() const {
    assert(updateCheck);
    return fields.back();
  };
  std::string toString() const;
  std::string toString(const strings& names) const;
  DbRecord toRecord() const;
  size_t size() const { return fields.size(); }
  void rotate(const int moveCount);

private:
  const bool updateCheck;
  std::vector<std::unique_ptr<Field>> fields;
  static log4cxx::LoggerPtr log;
};

/*****************************************************************************/

class TableData {
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
  std::string rowString(int index) const { return rows.at(index)->toString(names); };
  size_t size() const { return rows.size(); }
  bool empty() const { return rows.empty(); }
  const strings& columnNames() const { return names; };

private:
  const std::string ref;
  const bool updateCheck;
  strings names;
  std::vector<std::unique_ptr<TableRow>> rows;
  log4cxx::LoggerPtr log;
};

/*****************************************************************************/
}

template <> struct fmt::formatter<dbsync::Mode> : ostream_formatter {};
template <> struct fmt::formatter<dbsync::OperationConfig> : ostream_formatter {};
