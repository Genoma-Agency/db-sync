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

#include <operation.h>

namespace dbsync {

const std::size_t BULK = 1000;
const std::size_t FEEDBACK = 100;
const char* spaces = "                         ";

using TimerMs = util::Timer<std::chrono::milliseconds>;

Operation::Operation(const OperationConfig& c, std::unique_ptr<Db> src, std::unique_ptr<Db> dest) noexcept
    : config{ c },
      fromDb{ std::move(src) },
      toDb{ std::move(dest) },
      log{ log4cxx::Logger::getLogger(LOG_OPERATION) } {}

Operation::~Operation() {
  fromDb.reset(nullptr);
  toDb.reset(nullptr);
}

bool Operation::checkTables(const strings& src, const strings& dest) {
  bool tablesOk = true;
  if(config.tables.empty()) {
    LOG4CXX_DEBUG(log, "tables filter empty - using all tables from source");
    tables.insert(src.begin(), src.end());
  } else {
    LOG4CXX_DEBUG_FMT(log, "tables filter: {}", ba::join(config.tables, ", "));
    for(auto& f : config.tables) {
      if(std::find(src.begin(), src.end(), f) == src.end()) {
        tablesOk = false;
        LOG4CXX_ERROR_FMT(log, "table `{}` not found in source", f);
        std::cerr << "table `" << f << "` not found in source" << std::endl;
      } else {
        tables.insert(f);
      }
    }
  }
  if(!tablesOk)
    return false;
  tablesOk = true;
  for(auto& f : tables) {
    if(std::find(dest.begin(), dest.end(), f) == src.end()) {
      tablesOk = false;
      LOG4CXX_ERROR_FMT(log, "table `{}` not found in destination", f);
      std::cerr << "table `" << f << "` not found in destination" << std::endl;
    }
  }
  if(!tablesOk)
    return false;
  LOG4CXX_INFO_FMT(log, "tables to process: {}", ba::join(tables, ", "));
  return true;
}

bool Operation::checkMetadata() {
  assert(!tables.empty());
  if(!fromDb->loadMetadata(tables))
    return false;
  fromDb->logTableInfo();
  if(!toDb->loadMetadata(tables))
    return false;
  toDb->logTableInfo();
  bool checkColumns = true;
  std::for_each(
      tables.begin(), tables.end(), [&](const std::string& table) { checkColumns &= checkMetadataColumns(table); });
  return checkColumns;
}

bool Operation::checkMetadataColumns(const std::string& table) {
  auto src = fromDb->metadata().at(table);
  auto dest = toDb->metadata().at(table);
  auto sc = src.columns.size();
  auto dc = dest.columns.size();
  if(sc != dc) {
    LOG4CXX_ERROR_FMT(log, "table \"{}\" columns count mismatch [source {}] [destination {}]", table, sc, dc);
    std::cerr << "Table \"" << table << "\" columns count mismatch" << std::endl;
    return false;
  }
  bool columnsOk = true;
  for(int i = 0; i < sc; i++) {
    if(src.columns[i] != dest.columns[i]) {
      LOG4CXX_ERROR_FMT(log,
                        "table \"{}\" column {} mismatch [source {}] [destination {}]",
                        table,
                        i,
                        src.columns[i],
                        dest.columns[i]);
      std::cerr << "Table \"" << table << "\" column " << i << " mismatch " << src.columns[i]
                << " != " << dest.columns[i] << std::endl;
      columnsOk = false;
    }
  }
  return columnsOk;
}

bool Operation::preExecute() {
  if(!toDb->exec("SET autocommit=1"))
    return false;
  if(!toDb->exec("SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0"))
    return false;
  if(!toDb->exec("SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0"))
    return false;
  if(config.disableBinLog)
    if(!toDb->exec("SET SESSION SQL_LOG_BIN=0"))
      return false;
  return true;
}

bool Operation::postExecute(bool executeOk) {
  if(!toDb->exec("SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS"))
    return false;
  if(!toDb->exec("SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS"))
    return false;
  if(!toDb->exec("SET autocommit=0"))
    return false;
  if(config.disableBinLog)
    if(!toDb->exec("SET SESSION SQL_LOG_BIN=1"))
      return false;
  return true;
}

bool Operation::execute() {
  LOG4CXX_DEBUG_FMT(log, "start processing with configuration {}", config);
  bool ok = true;
  TimerMs timer;
  auto iter = tables.begin();
  std::string m{ config.mode == Mode::Copy ? "copy" : "sync" };
  while((config.dryRun || ok) && iter != tables.end()) {
    auto& table = *iter;
    auto src = fromDb->metadata().at(table);
    if(src.rowCount == 0 || src.columns.empty()) {
      LOG4CXX_INFO_FMT(log, "empty table \"{}\"", table);
      std::cout << "empty table \"" << table << "\"" << std::endl;
    } else {
      LOG4CXX_INFO_FMT(log, "{} table `{}`", m, table);
      std::cout << m << " table `" << table << "`" << (config.dryRun ? " dry run" : "") << std::endl;
      ok = execute(table);
    }
    iter++;
  }
  std::cout << "completed in " << timer.elapsed().elapsed().string() << std::endl;
  return ok;
}

bool Operation::execute(const std::string& table) {
  LOG4CXX_DEBUG_FMT(log, "start processing table \"{}\"", table);
  TimerMs timer;
  std::string sqlKeys = buildSqlKeys(table);
  std::cout << fmt::format(
      "loading {} primary keys from source `{}`{}\r", fromDb->metadata().at(table).rowCount, table, spaces)
            << std::flush;
  TableData srcKeys{ true, table, fromDb->metadata().at(table).rowCount, config.update };
  fromDb->query(sqlKeys, srcKeys);
  std::cout << fmt::format(
      "loading {} primary keys from destination `{}`{}\r", toDb->metadata().at(table).rowCount, table, spaces)
            << std::flush;
  TableData destKeys{ false, table, toDb->metadata().at(table).rowCount, config.update };
  toDb->query(sqlKeys, destKeys);
  std::cout << spaces << spaces << spaces << '\r' << std::flush;
  //
  srcKeys.sort();
  destKeys.sort();
  TableDiff diff{ srcKeys, destKeys };
  diff.logResult(table);
  int expected = diff.onlySrcIndexes().size();
  if(config.update)
    expected += diff.updateIndexes().size();
  if(config.mode == Mode::Sync)
    expected += diff.onlyDestIndexes().size();
  timer.reset(expected);
  int count;
  TableData srcRecord{ true, table, BULK };
  auto progress = [table, &timer, this](const char* title, int count, const std::vector<int>& v, bool endl) {
    if(count == 0) {
      std::cout << fmt::format("begin {} `{}` {} records\r", title, table, v.size());
    } else {
      auto times = timer.elapsed(count + 1);
      auto e = times.elapsed().string();
      if(endl) {
        std::cout << fmt::format("{} `{}` {} records [elapsed {}]{}", title, table, v.size(), e, spaces) << std::endl;
      } else {
        auto m = times.missing().isZero() ? "?" : times.missing().string();
        std::cout << fmt::format(
            "{} `{}` {}/{} [elapsed {}] [eta {}]{}\r", title, table, count, v.size(), e, m, spaces);
      }
    }
    std::cout << std::flush;
  };
  std::vector<int>::iterator indexIter;
  // copy records from source to destination
  if(!diff.onlySrcIndexes().empty()) {
    fromDb->selectPrepare(table, srcKeys.columnNames(), BULK);
    toDb->insertPrepare(table);
    count = 0;
    progress("copy", count, diff.onlySrcIndexes(), false);
    indexIter = diff.onlySrcIndexes().begin();
    while(count < diff.onlySrcIndexes().size()) {
      srcRecord.clear();
      if(!fromDb->selectExecute(table, srcKeys, indexIter, diff.onlySrcIndexes().end(), srcRecord)) {
        std::cout << std::endl
                  << "select failed at key " << srcKeys.rowString(*indexIter) << ' ' << fromDb->lastError()
                  << std::endl;
        return false;
      }
      assert(srcRecord.size() > 0);
      for(int i = 0; i < srcRecord.size(); i++) {
        if((count + i) % FEEDBACK == 0)
          progress("copy", count + i, diff.onlySrcIndexes(), false);
        LOG4CXX_TRACE_FMT(log, "insert {}: {}", count + i + 1, srcRecord.rowString(i));
        if(!config.dryRun && !toDb->insertExecute(table, srcRecord.at(i))) {
          std::cout << std::endl
                    << "insert failed for " << srcRecord.rowString(i) << ' ' << toDb->lastError() << std::endl;
          return false;
        }
      }
      count += srcRecord.size();
    }
    progress("copied", count, diff.onlySrcIndexes(), true);
  }
  // update records from source to destination
  if(config.update && !diff.updateIndexes().empty()) {
    fromDb->selectPrepare(table, srcKeys.columnNames(), BULK);
    count = 0;
    progress("update", count, diff.updateIndexes(), false);
    indexIter = diff.updateIndexes().begin();
    while(count < diff.updateIndexes().size()) {
      srcRecord.clear();
      if(!fromDb->selectExecute(table, srcKeys, indexIter, diff.updateIndexes().end(), srcRecord)) {
        std::cout << std::endl
                  << "select failed at key " << srcKeys.rowString(*indexIter) << ' ' << fromDb->lastError()
                  << std::endl;
        return false;
      }
      assert(srcRecord.size() > 0);
      if(count == 0)
        toDb->updatePrepare(table, srcKeys.columnNames(), srcRecord.columnNames());
      for(int i = 0; i < srcRecord.size(); i++) {
        if((count + i) % FEEDBACK == 0)
          progress("copy", count + i, diff.onlySrcIndexes(), false);
        LOG4CXX_TRACE_FMT(log, "update {}: {}", count + i + 1, srcRecord.rowString(i));
        if(!config.dryRun && !toDb->updateExecute(table, srcRecord.at(i))) {
          std::cout << std::endl
                    << "update failed for " << srcRecord.rowString(i) << ' ' << toDb->lastError() << std::endl;
          return false;
        }
      }
      count += srcRecord.size();
    }
    progress("updated", count, diff.updateIndexes(), true);
  }
  // remove records from destination
  if(config.mode == Mode::Sync && !diff.onlyDestIndexes().empty()) {
    toDb->deletePrepare(table, srcKeys.columnNames());
    count = 0;
    for(int i : diff.onlyDestIndexes()) {
      if(count % BULK == 0)
        progress("deleting", count + 1, diff.onlyDestIndexes(), false);
      LOG4CXX_TRACE_FMT(log, "delete {}: {}", count + 1, destKeys.rowString(i));
      if(!config.dryRun && !toDb->deleteExecute(table, destKeys.at(i))) {
        std::cout << std::endl
                  << "delete failed for " << destKeys.rowString(i) << ' ' << toDb->lastError() << std::endl;
        return false;
      }
      count++;
    }
    progress("deleted", count, diff.onlyDestIndexes(), true);
  }
  std::cout << fmt::format("table `{}` processed in {}", table, timer.elapsed().elapsed().string()) << std::endl;
  return true;
}

std::string Operation::buildSqlKeys(const std::string& table) const {
  auto srcTable = fromDb->metadata().at(table);
  auto destTable = toDb->metadata().at(table);
  strings pk;
  strings fields;
  for(ColumnInfo& ci : srcTable.columns) {
    if(ci.primaryKey) {
      pk.push_back(fmt::format("`{}`", ci.name));
    } else if(config.update) {
      fields.push_back(fmt::format("COALESCE(`{}`,\"{}\")", ci.name, SQL_NULL_STRING));
    }
  }
  std::stringstream sqlKeys;
  sqlKeys << "SELECT " << ba::join(pk, ",");
  if(config.update)
    sqlKeys << ",MD5(CONCAT(" << ba::join(fields, ",") << ")) AS " << SQL_MD5_CHECK;
  sqlKeys << " FROM `" << table << '`';
  return sqlKeys.str();
}

/*****************************************************************************/

TableData::TableData(const bool source, const std::string& t, const size_t sizeHint, bool uc)
    : ref{ fmt::format("<{}|{}>", source ? "source" : "destination", t) },
      updateCheck{ uc },
      log{ log4cxx::Logger::getLogger(LOG_DATA) } {
  rows.reserve(sizeHint);
}

void TableData::clear() {
  rows.clear();
  names.clear();
};

void TableData::sort() {
  std::sort(rows.begin(), rows.end(), [](const std::unique_ptr<TableRow>& a, const std::unique_ptr<TableRow>& b) {
    return *a < *b;
  });
};

void TableData::loadRow(const soci::row& row) {
  LOG4CXX_TRACE_FMT(log, "{} loading row {}", ref, rows.size() + 1);
  if(rows.empty()) {
    const int end = updateCheck ? row.size() - 1 : row.size();
    for(std::size_t i = 0; i < end; ++i) {
      auto& props = row.get_properties(i);
      names.push_back(props.get_name());
    }
  }
  rows.emplace_back(std::make_unique<TableRow>(row, updateCheck));
}

/*****************************************************************************/

log4cxx::LoggerPtr TableRow::log{ log4cxx::Logger::getLogger(LOG_DATA) };

TableRow::TableRow(const soci::row& row, const bool& uc)
    : updateCheck{ uc } {
  fields.reserve(row.size());
  for(std::size_t i = 0; i != row.size(); ++i) {
    auto& props = row.get_properties(i);
    fields.emplace_back(std::make_unique<Field>(row, i));
    LOG4CXX_TRACE_FMT(log,
                      "loaded field [{}] [{}] [{}] [{}]",
                      props.get_name(),
                      props.get_data_type(),
                      fields[i]->toString(),
                      fields[i]->indicator());
  }
}

std::partial_ordering TableRow::operator<=>(const TableRow& other) const {
  std::partial_ordering comp = std::partial_ordering::unordered;
  if(size() == other.size()) {
    comp = std::partial_ordering::equivalent;
    const int end = updateCheck ? fields.size() - 1 : fields.size();
    for(int i = 0; i < end && comp == std::partial_ordering::equivalent; i++)
      comp = *at(i) <=> *other.at(i);
  }
  return comp;
}

void TableRow::rotate(const int moveCount) {
  assert(moveCount > 0);
  assert(moveCount < fields.size());
  auto it = std::next(fields.begin(), moveCount);
  std::rotate(fields.begin(), it, fields.end());
}

std::string TableRow::toString(const strings& names) const {
  const int end = updateCheck ? fields.size() - 1 : fields.size();
  assert(names.size() == end);
  std::stringstream s;
  for(int i = 0; i < end; i++)
    s << names[i] << '[' << fields[i]->toString() << "] ";
  if(updateCheck)
    s << '<' << fields[end]->toString() << "> ";
  return s.str();
}

/*****************************************************************************/

TableDiff::TableDiff(TableData& s, TableData& d) noexcept
    : src{ s }, dest{ d }, log{ log4cxx::Logger::getLogger(LOG_DATA) } {
  assert(src.hasUpdateCheck() == dest.hasUpdateCheck());
  onlySrc.reserve(src.size());
  common.reserve(src.size());
  onlyDest.reserve(dest.size());
  int srcIndex = 0;
  int destIndex = 0;
  auto srcEnd = [&srcIndex, this]() -> bool { return srcIndex >= src.size(); };
  auto destEnd = [&destIndex, this]() -> bool { return destIndex >= dest.size(); };
  while(!srcEnd() && !destEnd()) {
    if(*src.at(srcIndex) < *dest.at(destIndex)) {
      onlySrc.push_back(srcIndex++);
    } else if(*src.at(srcIndex) > *dest.at(destIndex)) {
      onlyDest.push_back(destIndex++);
    } else {
      if(src.hasUpdateCheck()) {
        if((*src.at(srcIndex)->checkValue() <=> *dest.at(destIndex)->checkValue()) != std::partial_ordering::equivalent)
          update.push_back(srcIndex);
      }
      common.push_back(srcIndex++);
      destIndex++;
    }
  }
  for(int remaining = srcIndex; remaining < src.size(); onlySrc.push_back(remaining++))
    ;
  for(int remaining = destIndex; remaining < dest.size(); onlyDest.push_back(remaining++))
    ;
  assert(onlySrc.size() + common.size() == src.size());
  assert(onlyDest.size() + common.size() == dest.size());
}

void TableDiff::logResult(const std::string& table) const {
  LOG4CXX_INFO_FMT(log, "table `{}` records: source {} destination {}", table, src.size(), dest.size());
  if(onlySrc.empty())
    LOG4CXX_INFO_FMT(log, "table `{}` only in source empty", table);
  else
    LOG4CXX_INFO_FMT(log,
                     "table `{}` only in source {} first {} last {}",
                     table,
                     onlySrc.size(),
                     src.rowString(onlySrc.front()),
                     src.rowString(onlySrc.back()));
  if(common.empty())
    LOG4CXX_INFO_FMT(log, "table `{}` common empty", table);
  else
    LOG4CXX_INFO_FMT(log,
                     "table `{}` common {} (to update {}) first {} last {}",
                     table,
                     common.size(),
                     update.size(),
                     src.rowString(common.front()),
                     src.rowString(common.back()));
  if(onlyDest.empty())
    LOG4CXX_INFO_FMT(log, "table `{}` only in destination empty", table);
  else
    LOG4CXX_INFO_FMT(log,
                     "table `{}` only in destination {} first {} last {}",
                     table,
                     onlyDest.size(),
                     dest.rowString(onlyDest.front()),
                     dest.rowString(onlyDest.back()));
}
/*****************************************************************************/

std::ostream& operator<<(std::ostream& stream, const Mode& var) {
  switch(var) {
  case Copy:
    return stream << "copy";
  case Sync:
    return stream << "sync";
  default:
    return stream << "Unknown mode " << var;
  }
}

std::ostream& operator<<(std::ostream& stream, const OperationConfig& var) {
  stream << "[mode: " << var.mode << "] [update: " << var.update << "] [dryRun: " << var.dryRun
         << "] [tables: " << ba::join(var.tables, ",") << "] [disableBinLog: " << var.disableBinLog;
  return stream << ']';
}

}
