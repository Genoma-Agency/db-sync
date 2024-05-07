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

#include <future>
#include <keys.h>
#include <operation.h>

namespace dbsync {

Operation::Operation(const OperationConfig& c, std::unique_ptr<Db> src, std::unique_ptr<Db> dest) noexcept
    : config{ c },
      fromDb{ std::move(src) },
      toDb{ std::move(dest) },
      log{ log4cxx::Logger::getLogger(LOG_OPERATION) },
      dbRw{ 0 } {}

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
      LOG4CXX_ERROR_FMT(log, "table `{}` not found in target", f);
      std::cerr << "table `" << f << "` not found in target" << std::endl;
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
    LOG4CXX_ERROR_FMT(log, "table \"{}\" columns count mismatch [source {}] [target {}]", table, sc, dc);
    std::cerr << "Table \"" << table << "\" columns count mismatch" << std::endl;
    return false;
  }
  bool columnsOk = true;
  for(int i = 0; i < sc; i++) {
    if(src.columns[i] != dest.columns[i]) {
      LOG4CXX_ERROR_FMT(
          log, "table \"{}\" column {} mismatch [source {}] [target {}]", table, i, src.columns[i], dest.columns[i]);
      std::cerr << "Table \"" << table << "\" column " << i << " mismatch " << src.columns[i]
                << " != " << dest.columns[i] << std::endl;
      columnsOk = false;
    }
  }
  return columnsOk;
}

bool Operation::preExecute() {
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
    if(src.columns.empty()) {
      LOG4CXX_INFO_FMT(log, "`{}` empty table", table);
      std::cout << fmt::format("`{}` empty table", table) << std::endl;
    } else {
      LOG4CXX_INFO_FMT(log, "`{}` {} table ", table, m);
      std::cout << fmt::format("`{}` {} {}", table, m, config.dryRun ? "dry run" : "") << std::endl;
      TimerMs timerTable;
      ok = execute(table);
      std::cout << fmt::format("`{}` processed in {}", table, timerTable.elapsed().elapsed().string()) << std::endl;
    }
    iter++;
  }
  auto time = timer.elapsed().elapsed().string();
  std::cout << fmt::format(
      "completed in {} db R/W {:L} maximum memory used {}", time, dbRw, util::proc::maxMemoryUsage())
            << std::endl;
  return ok;
}

bool Operation::execute(const std::string& table) {
  LOG4CXX_DEBUG_FMT(log, "`{}` start processing", table);
  // load source primary key
  TableKeys srcKeys;
  auto srcLoad = std::async(std::launch::async, [&] {
    auto loaded = fromDb->loadPk(true, table, srcKeys, config.pkBulk);
    if(loaded)
      srcKeys.sort("source");
    dbRw += srcKeys.size();
    return loaded;
  });
  // load target primary key
  TableKeys destKeys;
  auto destLoad = std::async(std::launch::async, [&] {
    auto loaded = toDb->loadPk(false, table, destKeys, config.pkBulk);
    if(loaded)
      destKeys.sort("target");
    dbRw += destKeys.size();
    return loaded;
  });
  // wait asynch load
  bool loaded;
  loaded = srcLoad.get();
  assert(loaded);
  loaded = destLoad.get();
  assert(loaded);
  // compare primary keys between source and target
  auto diff = compareKeys(table, srcKeys, destKeys);
  // copy records from source to target
  if(!executeAdd(table, srcKeys, std::get<0>(diff)))
    return false;
  // update records from source to target
  if(config.update)
    if(!executeUpdate(table, srcKeys, std::get<1>(diff)))
      return false;
  // remove records from target
  if(config.mode == Mode::Sync)
    if(!executeDelete(table, destKeys, std::get<2>(diff)))
      return false;
  return true;
}

bool Operation::executeAdd(const std::string& table, TableKeys& srcKeys, std::size_t total) {
  if(total == 0)
    return true;
  TimerMs timer{ total };
  std::size_t count = 0;
  std::size_t bulk = std::min(total, config.modifyBulk);
  TableData srcRecord{ true, table, bulk };
  TableKeysIterator indexIter = srcKeys.iter(true);
  toDb->insertPrepare(table);
  progress(table, timer, "copy", count, total);
  while(!indexIter.end()) {
    bulk = std::min(total - count, config.modifyBulk);
    if(count == 0 || bulk < config.modifyBulk)
      fromDb->selectPrepare(table, srcKeys.columnNames(), bulk);
    srcRecord.clear();
    if(!fromDb->selectExecute(table, srcKeys, indexIter, srcRecord)) {
      auto r = srcKeys.rowString(indexIter.value());
      std::cerr << fmt::format("`{}` select failed at key {} {}", table, r, fromDb->lastError()) << std::endl;
      return false;
    }
    assert(srcRecord.size() > 0);
    progress(table, timer, "copy load", count + srcRecord.size(), total);
    toDb->transactionBegin();
    for(int i = 0; i < srcRecord.size(); i++) {
      if(feedback(count + i + 1, srcRecord.size(), total))
        progress(table, timer, "insert", count + i + 1, total);
      LOG4CXX_TRACE_FMT(log, "`{}` insert {}: {}", table, count + i + 1, srcRecord.rowString(i));
      if(!config.dryRun && !toDb->insertExecute(table, srcRecord.at(i))) {
        auto record = srcRecord.rowString(i);
        std::cerr << "insert failed for " << record << ' ' << toDb->lastError() << std::endl;
        LOG4CXX_ERROR_FMT(log, "`{}` insert failed {} {}", table, record, toDb->lastError());
        if(!config.noFail)
          return false;
      }
    }
    toDb->transactionCommit();
    count += srcRecord.size();
    dbRw += srcRecord.size();
  }
  progress(table, timer, "copied", count);
  return true;
}

bool Operation::executeUpdate(const std::string& table, TableKeys& srcKeys, std::size_t total) {
  if(total == 0)
    return true;
  TimerMs timer{ total };
  std::size_t count = 0;
  std::size_t bulk = std::min(total, config.compareBulk);
  TableData srcCompare{ true, table, bulk, true };
  TableData destCompare{ false, table, bulk, true };
  // filter record which need to be updateb (md5 sum fileds compare)
  srcKeys.revertFlags();
  TableKeysIterator fromIter = srcKeys.iter(true);
  TableKeysIterator toIter = srcKeys.iter(true);
  progress(table, timer, "compare fields md5", 0, total);
  while(!fromIter.end()) {
    TableKeysIterator iter{ fromIter };
    bulk = std::min(total - count, config.modifyBulk);
    if(count == 0 || bulk < config.modifyBulk) {
      fromDb->comparePrepare(table, bulk);
      toDb->comparePrepare(table, bulk);
    }
    auto srcLoad = std::async(std::launch::async, [&] {
      srcCompare.clear();
      return fromDb->selectExecute(table, srcKeys, fromIter, srcCompare);
    });
    auto destLoad = std::async(std::launch::async, [&] {
      destCompare.clear();
      return toDb->selectExecute(table, srcKeys, toIter, destCompare);
    });
    bool loaded = srcLoad.get() && destLoad.get();
    if(!loaded) {
      std::cerr << fmt::format(
          "`{}` load md5 sum failed - source [{}] target [{}]", table, fromDb->lastError(), toDb->lastError())
                << std::endl;
      return false;
    }
    assert(srcCompare.size() == destCompare.size());
    dbRw += srcCompare.size() + destCompare.size();
    for(int i = 0; i < srcCompare.size(); i++, count++) {
      TableRow& srcRow = *srcCompare.at(i);
      TableRow& destRow = *destCompare.at(i);
      assert(srcRow <=> destRow == std::partial_ordering::equivalent);
      /*
      std::cout << fmt::format("{} -> {} > KEY {} SRC {} TARGET {}",
                               iter.value(),
                               iter.ref(),
                               srcKeys.rowString(iter.value()),
                               srcRow.toString(),
                               destRow.toString())
                << std::endl;
      */
#ifdef DEBUG
      assert(srcKeys.check(iter.value(), srcRow.toRecord()));
      assert(srcKeys.check(iter.value(), destRow.toRecord()));
#endif
      Field& srcMd5 = *srcRow.checkValue();
      Field& destMd5 = *destRow.checkValue();
      srcKeys.setFlag(iter.value(), srcMd5 <=> destMd5 != std::partial_ordering::equivalent);
      ++iter;
    }
    progress(table, timer, "comparing fields md5", count, total);
  }
  progress(table, timer, "compared fields md5", total);
  // begin updates
  total = srcKeys.size(true);
  if(total == 0) {
    std::cout << fmt::format("`{}` no record to update found", table) << std::endl;
    return true;
  }
  bulk = std::min(total, config.modifyBulk);
  TableData srcRecord{ true, table, bulk };
  std::cout << fmt::format("`{}` {} records to update found", table, total) << std::endl;
  timer.reset(total);
  TableKeysIterator indexIter = srcKeys.iter(true);
  count = 0;
  progress(table, timer, "update", count, total);
  while(!indexIter.end()) {
    bulk = std::min(total - count, config.modifyBulk);
    if(count == 0 || bulk < config.modifyBulk)
      fromDb->selectPrepare(table, srcKeys.columnNames(), bulk);
    srcRecord.clear();
    if(!fromDb->selectExecute(table, srcKeys, indexIter, srcRecord)) {
      auto r = srcKeys.rowString(indexIter.value());
      std::cerr << fmt::format("`{}` select failed at key {} {}", table, r, fromDb->lastError()) << std::endl;
      return false;
    }
    assert(srcRecord.size() > 0);
    dbRw += srcRecord.size();
    progress(table, timer, "update load", count + srcRecord.size(), total);
    if(count == 0)
      toDb->updatePrepare(table, srcKeys.columnNames(), srcRecord.columnNames());
    toDb->transactionBegin();
    for(int i = 0; i < srcRecord.size(); i++) {
      if(feedback(count + i + 1, srcRecord.size(), total))
        progress(table, timer, "update", count + i + 1, total);
      LOG4CXX_TRACE_FMT(log, "update {}: {}", count + i + 1, srcRecord.rowString(i));
      if(!config.dryRun && !toDb->updateExecute(table, srcRecord.at(i))) {
        auto record = srcRecord.rowString(i);
        std::cerr << fmt::format("`{}` update failed for {} {}", table, record, toDb->lastError()) << std::endl;
        LOG4CXX_ERROR_FMT(log, "update failed {} {}", record, toDb->lastError());
        if(!config.noFail)
          return false;
      }
    }
    toDb->transactionCommit();
    count += srcRecord.size();
    dbRw += srcRecord.size();
  }
  progress(table, timer, "updated", count);
  return true;
}

bool Operation::executeDelete(const std::string& table, TableKeys& destKeys, std::size_t total) {
  if(total == 0)
    return true;
  TimerMs timer{ total };
  std::size_t count = 0;
  TableKeysIterator indexIter = destKeys.iter(true);
  toDb->deletePrepare(table, destKeys.columnNames());
  count = 0;
  progress(table, timer, "deleting", count, total);
  toDb->transactionBegin();
  while(!indexIter.end()) {
    if(feedback(++count, total, total))
      progress(table, timer, "deleting", count, total);
    LOG4CXX_TRACE_FMT(log, "`{}` delete {}: {}", table, count, destKeys.rowString(indexIter.value()));
    if(!config.dryRun && !toDb->deleteExecute(table, destKeys, indexIter.value())) {
      auto record = destKeys.rowString(indexIter.value());
      std::cerr << fmt::format("`{}` delete failed for {} {}", table, record, toDb->lastError()) << std::endl;
      LOG4CXX_ERROR_FMT(log, "`{}` delete failed {} {}", table, record, toDb->lastError());
      if(!config.noFail)
        return false;
    }
    ++indexIter;
    dbRw++;
  }
  toDb->transactionCommit();
  progress(table, timer, "deleted", count);
  return true;
}

bool Operation::feedback(const std::size_t count, const std::size_t bulk, const std::size_t total) const {
  if(count == total)
    return true;
  if(count % bulk == 0)
    return true;
  if(count < 1000)
    return count % 100 == 0;
  if(count < 10000)
    return count % 1000 == 0;
  if(count < 100000)
    return count % 10000 == 0;
  return count % 100000 == 0;
}

std::tuple<std::size_t, std::size_t, std::size_t>
Operation::compareKeys(const std::string& table, TableKeys& src, TableKeys& dest) {
  std::size_t srcIndex = 0;
  std::size_t destIndex = 0;
  while(srcIndex < src.size() && destIndex < dest.size()) {
    if(src.less(srcIndex, dest, destIndex)) {
      src.setFlag(srcIndex++);
    } else if(dest.less(destIndex, src, srcIndex)) {
      dest.setFlag(destIndex++);
    } else {
      srcIndex++;
      destIndex++;
    }
  }
  while(srcIndex < src.size())
    src.setFlag(srcIndex++);
  while(destIndex < dest.size())
    dest.setFlag(destIndex++);
  assert(srcIndex == src.size());
  assert(destIndex == dest.size());
  std::size_t onlySrc = src.size(true);
  std::size_t common = src.size() - onlySrc;
  std::size_t onlyDest = dest.size(true);
  assert(common == dest.size() - onlyDest);
  LOG4CXX_INFO_FMT(log, "`{}` records: source {} target {}", table, srcIndex, destIndex);
  if(onlySrc == 0)
    LOG4CXX_INFO_FMT(log, "`{}` only in source empty", table);
  else
    LOG4CXX_INFO_FMT(log, "`{}` only in source {}", table, onlySrc);
  if(common == 0)
    LOG4CXX_INFO_FMT(log, "`{}` common empty", table);
  else
    LOG4CXX_INFO_FMT(log, "`{}` common {}", table, common);
  if(onlyDest == 0)
    LOG4CXX_INFO_FMT(log, "`{}` only in target empty", table);
  else
    LOG4CXX_INFO_FMT(log, "`{}` only in target {}", table, onlyDest);
  std::cout << fmt::format(
      "`{}` primary key compare [only source: {}] [common: {}] [only target: {}]", table, onlySrc, common, onlyDest)
            << std::endl;
  return std::make_tuple(onlySrc, common, onlyDest);
}

/*****************************************************************************/

TableData::TableData(const bool source, const std::string& t, const size_t sizeHint, bool uc)
    : ref{ fmt::format("`{}`|{}", t, source ? "source" : "target") },
      updateCheck{ uc },
      log{ log4cxx::Logger::getLogger(LOG_DATA) } {
  rows.reserve(sizeHint);
}

void TableData::clear() {
  rows.clear();
  names.clear();
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

std::string TableRow::toString() const {
  const int end = updateCheck ? fields.size() - 1 : fields.size();
  return toString(strings(end, ""));
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

DbRecord TableRow::toRecord() const {
  DbRecord record;
  const int end = updateCheck ? fields.size() - 1 : fields.size();
  for(int i = 0; i < end; i++)
    record.emplace_back(std::make_pair(at(i)->type(), at(i)->asVariant()));
  return record;
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
