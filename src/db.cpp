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

#include <db.h>
#include <keys.h>
#include <operation.h>

namespace dbsync {

const std::string SQL_NULL_STRING{ (const char*)u8"∅" };
const std::string SQL_MD5_CHECK{ "`#MD5@CHECK#`" };

const std::string Db::SQL_TABLES{ R"#(
select
	table_name as "NAME"
from
	information_schema.tables c
where
  table_schema = :schema
  and table_type = 'BASE TABLE'
order by 1
;
)#" };

const std::string Db::SQL_COLUMNS{ R"#(
select
	column_name as "NAME",
	data_type as "TYPE",
	is_nullable as "NULLABLE",
	exists(select * from information_schema.key_column_usage k 
		where k.constraint_name = 'primary' 
		and k.table_schema = c.table_schema 
		and k.table_name = c.table_name 
		and k.column_name = c.column_name) as "PK"
from
	information_schema.columns c
where
  table_schema = :schema 
	and table_name = :tabella
;
)#" };

Db::Db(const std::string r)
    : ref{ r }, log{ log4cxx::Logger::getLogger(LOG_DB) } {}

Db::~Db() {
  if(session && session->is_connected()) {
    LOG4CXX_DEBUG_FMT(log, "<{}> closing db", ref);
    session->close();
  }
}

void Db::transactionBegin() { tx.emplace(*session); }

void Db::transactionCommit() {
  assert(tx.has_value());
  tx->commit();
}

bool Db::apply(const std::string& opDesc, std::function<void(void)> lambda, std::function<void(void)> finally) {
  bool ok = false;
  try {
    LOG4CXX_TRACE_FMT(log, "<{}> apply [{}] [RSS: {}]", ref, opDesc, memoryUsage());
    lambda();
    LOG4CXX_TRACE_FMT(log, "<{}> apply done [RSS: {}]", ref, memoryUsage());
    error.clear();
    ok = true;
  } catch(soci::mysql_soci_error const& e) {
    LOG4CXX_ERROR_FMT(log, "<{}> [{}] error [{}]: {}", ref, opDesc, e.err_num_, e.what());
    error = fmt::format("[{}]: {}", e.err_num_, e.what());
  } catch(soci::soci_error const& e) {
    LOG4CXX_ERROR_FMT(log, "<{}> [{}] error: {}", ref, opDesc, e.what());
    error = e.what();
  } catch(std::exception const& e) {
    LOG4CXX_ERROR_FMT(log, "<{}> [{}] fault: {}", ref, opDesc, e.what());
    error = e.what();
  }
  if(finally)
    try {
      finally();
    } catch(std::exception const& e) {
      LOG4CXX_ERROR_FMT(log, "<{}> [{}] finally fault: {}", ref, opDesc, e.what());
    }
  return ok;
}

bool Db::open(const std::string& h, int p, const std::string& s, const std::string& user, const std::string& pwd) {
  assert(!session);
  std::string connection = fmt::format("host={} port={} db={} user={} password={}", h, p, s, user, pwd);
  schema = s;
  return apply(fmt::format("connect {}", connection), [&connection, this] {
    std::cout << "connecting " << connection << std::endl;
    session = std::make_unique<soci::session>(soci::mysql, connection);
  });
}

bool Db::loadTables(strings& tables) {
  return apply("load tables", [&] { *session << SQL_TABLES, soci::use(schema), soci::into(tables); });
}

bool Db::loadMetadata(std::set<std::string> tables) {
  return apply(
      "metadata", [&] {
        std::string table;
        ColumnInfo ci;
        std::string isNullable;
        int pk;
        soci::statement stInfo = (session->prepare << SQL_COLUMNS,
                                  soci::use(schema),
                                  soci::use(table),
                                  soci::into(ci.name),
                                  soci::into(ci.type),
                                  soci::into(isNullable),
                                  soci::into(pk));
        for(auto& t : tables) {
          table = t;
          TableInfo ti;
          // columns
          if(stInfo.execute(true)) {
            do {
              ci.nullable = ba::iequals(isNullable, "yes");
              ci.primaryKey = pk > 0;
              ti.columns.push_back(ci);
            } while(stInfo.fetch());
          }
          //
          std::cout << fmt::format("{} `{}` ", ref, table) << std::endl;
          map.emplace(table, std::move(ti));
        }
      });
}

void Db::logTableInfo() const {
  LOG4CXX_INFO_FMT(log, "<{}> metadata information", ref);
  for(auto& [table, info] : map) {
    LOG4CXX_INFO_FMT(log, "`{}` {}", table, info);
    for(auto& ci : info.columns) {
      LOG4CXX_INFO_FMT(log, "  {}", ci);
    }
  }
}

bool Db::loadPk(bool source, const std::string& table, TableKeys& data, std::size_t bulk) {
  auto tm = metadata().at(table);
  std::string ref = source ? "source" : "target";
  std::string desc;
  strings pk;
  strings fields;
  for(int i = 0; i < tm.columns.size(); i++) {
    if(tm.columns[i].primaryKey) {
      pk.push_back(fmt::format("`{}`", tm.columns[i].name));
    } else if(data.hasUpdateCheck()) {
      fields.push_back(fmt::format("COALESCE(`{}`,'{}')", tm.columns[i].name, SQL_NULL_STRING));
    }
  }
  std::stringstream sqlKeys;
  std::stringstream sqlWhere;
  sqlKeys << "SELECT " << ba::join(pk, ",");
  if(data.hasUpdateCheck())
    sqlKeys << ",MD5(CONCAT(" << ba::join(fields, ",") << ")) AS " << SQL_MD5_CHECK;
  sqlKeys << " FROM `" << table << '`';
  std::string select = sqlKeys.str();
  TimerMs timer;
  bool ok = true;
  std::size_t loaded = bulk;
  desc = ref + " key loading";
  while(ok && loaded == bulk) {
    progress(table, timer, desc.c_str(), data.size());
    std::string sql = fmt::format("{} LIMIT {} OFFSET {}", select, bulk, data.size());
    loaded = 0;
    ok = query(sql, [&](const soci::row& row) {
      data.loadRow(row);
      loaded++;
    });
  };
  desc = ref + " key loaded";
  progress(table, timer, desc.c_str(), data.size(), data.size(), true);
  LOG4CXX_TRACE_FMT(log, "{} load done [RSS: {}]", ref, memoryUsage());
  return ok;
};

bool Db::query(const std::string& sql, TableData& data) {
  return query(sql, [&](const soci::row& row) { data.loadRow(row); });
}

bool Db::query(const std::string& sql, std::function<void(const soci::row&)> consumer) {
  return apply(sql, [&] {
    soci::rowset<soci::row> rs = (session->prepare << sql);
    for(auto it = rs.begin(); it != rs.end(); ++it)
      consumer(*it);
  });
}

bool Db::exec(const std::string& sql) {
  return apply(sql, [&] { *session << sql; });
}

bool Db::insertPrepare(const std::string& table) {
  std::stringstream s;
  s << "INSERT INTO `" << table << "` VALUES(:v0";
  for(int i = 1; i < map.at(table).columns.size(); i++)
    s << ",:v" << i;
  s << ')';
  std::string sql = s.str();
  return apply(sql, [&] { stmtWrite = (session->prepare << sql); });
}

bool Db::insertExecute(const std::string& table, const std::unique_ptr<TableRow>& row) {
  assert(map.at(table).columns.size() == row->size());
  assert(stmtWrite.has_value());
  return apply(
      "exec prepared insert",
      [&] {
        bind(stmtWrite, row, 0, row->size());
        stmtWrite->execute(true);
      },
      std::bind(&soci::statement::bind_clean_up, *stmtWrite));
}

bool Db::updatePrepare(const std::string& table, const strings& keys, const strings& fields) {
  assert(map.at(table).columns.size() == fields.size());
  keysCount = keys.size();
  std::stringstream s;
  s << "UPDATE `" << table << "` SET `" << fields[keysCount] << "`=:v0";
  for(int i = keysCount + 1; i < fields.size(); i++)
    s << ", `" << fields[i] << "`=:v" << i;
  s << " WHERE `" << keys[0] << "`=:k0";
  for(int i = 1; i < keysCount; i++)
    s << " AND `" << keys[i] << "`=:k" << i;
  std::string sql = s.str();
  return apply(sql, [&] { stmtWrite = (session->prepare << sql); });
}

bool Db::updateExecute(const std::string& table, const std::unique_ptr<TableRow>& row) {
  assert(map.at(table).columns.size() == row->size());
  assert(stmtWrite.has_value());
  row->rotate(keysCount);
  return apply(
      "exec prepared update",
      [&] {
        bind(stmtWrite, row, 0, row->size());
        stmtWrite->execute(true);
      },
      std::bind(&soci::statement::bind_clean_up, *stmtWrite));
}

bool Db::deletePrepare(const std::string& table, const strings& keys) {
  keysCount = keys.size();
  assert(keysCount > 0);
  std::stringstream s;
  s << "DELETE FROM `" << table << "` WHERE `" << keys[0] << "`=:v0";
  for(int i = 1; i < keysCount; i++)
    s << " AND `" << keys[i] << "`=:v" << i;
  std::string sql = s.str();
  return apply(sql, [&] { stmtWrite = (session->prepare << sql); });
}

bool Db::deleteExecute(const std::string& table, const TableKeys& keys, long index) {
  assert(stmtWrite.has_value());
  return apply(
      "exec prepared delete",
      [&] {
        LOG4CXX_TRACE_FMT(log, "delete bind [{}] {}", index, keys.rowString(index));
        keys.bind(*stmtWrite, index);
        stmtWrite->execute(true);
      },
      std::bind(&soci::statement::bind_clean_up, *stmtWrite));
}

bool Db::selectPrepare(const std::string& table, const strings& keys, const std::size_t bulk) {
  assert(bulk > 0);
  assert(keys.size() > 0);
  keysCount = keys.size();
  readCount = bulk;
  std::stringstream s;
  s << "SELECT * FROM `" << table << "` WHERE (`" << keys[0] << '`';
  for(int i = 1; i < keysCount; i++)
    s << ",`" << keys[i] << '`';
  s << ") IN (";
  for(int b = 0; b < bulk; b++) {
    if(b > 0)
      s << ',';
    s << "(:k0_" << b;
    for(int i = 1; i < keysCount; i++)
      s << ",:k" << i << '_' << b;
    s << ')';
  }
  s << ')';
  std::string sql = s.str();
  return apply(sql, [&] { stmtRead = (session->prepare << sql); });
}

bool Db::selectExecute(const std::string& table,
                       const TableKeys& keys,
                       std::vector<int>::iterator& from,
                       std::vector<int>::iterator end,
                       TableData& into) {
  static const std::unique_ptr<TableRow> emptyRow;
  assert(stmtRead.has_value());
  return apply(
      "exec prepared select",
      [&] {
        int count = 0;
        while(count < readCount && from != end) {
          LOG4CXX_TRACE_FMT(log, "select bind [{}] {}", *from, keys.rowString(*from));
          keys.bind(*stmtRead, *from);
          from++;
          count++;
        }
        for(; count < readCount; count++)
          bind(stmtRead, emptyRow, 0, keysCount);
        soci::row row;
        stmtRead->exchange_for_rowset(soci::into(row));
        stmtRead->execute(false);
        soci::rowset_iterator<soci::row> it(*stmtRead, row);
        soci::rowset_iterator<soci::row> end;
        for(; it != end; ++it)
          into.loadRow(row);
      },
      std::bind(&soci::statement::bind_clean_up, *stmtRead));
}

void Db::bind(std::optional<soci::statement>& stmt,
              const std::unique_ptr<TableRow>& row,
              const int startIndex,
              const int endIndex) {
  static soci::indicator nullIndicator = soci::i_null;
  static std::string nullString;
  assert(startIndex < endIndex);
  assert(stmt.has_value());
  for(int i = startIndex; i < endIndex; i++) {
    if(!row || row->at(i)->isNull()) {
      stmt->exchange(soci::use(nullString, nullIndicator));
    } else {
      switch(row->at(i)->type()) {
      case soci::dt_string:
      case soci::dt_xml:
      case soci::dt_blob:
      case soci::dt_date:
        stmt->exchange(soci::use(row->at(i)->asString()));
        break;
      case soci::dt_double:
        stmt->exchange(soci::use(row->at(i)->asDouble()));
        break;
      case soci::dt_integer:
        stmt->exchange(soci::use(row->at(i)->asInt()));
        break;
      case soci::dt_long_long:
        stmt->exchange(soci::use(row->at(i)->asLongLong()));
        break;
      case soci::dt_unsigned_long_long:
        stmt->exchange(soci::use(row->at(i)->asULongLong()));
        break;
      }
    }
  }
  stmt->define_and_bind();
}

/*****************************************************************************/

Field::Field(const soci::row& row, const std::size_t i)
    : value{ .number{ .uLongLong = 0ul } } {
  auto props = row.get_properties(i);
  dType = props.get_data_type();
  dIndicator = row.get_indicator(i);
  if(dIndicator == soci::i_null)
    return;
  switch(dType) {
  case soci::dt_string:
  case soci::dt_xml:
  case soci::dt_blob:
    value.string = row.get<std::string>(i);
    break;
  case soci::dt_date: {
    std::tm tm = row.get<std::tm>(i);
    value.number.epoch = std::mktime(&tm);
    value.string = fmt::format("{:%F %T}", tm);
  } break;
  case soci::dt_double:
    value.number.decimal = row.get<double>(i);
    break;
  case soci::dt_integer:
    value.number.integer = row.get<int>(i);
    break;
  case soci::dt_long_long:
    value.number.longLong = row.get<long long>(i);
    break;
  case soci::dt_unsigned_long_long:
    value.number.uLongLong = row.get<unsigned long long>(i);
    break;
  }
}

const std::string Field::toString() const {
  if(dIndicator == soci::i_null)
    return SQL_NULL_STRING;
  switch(dType) {
  case soci::dt_string:
  case soci::dt_xml:
  case soci::dt_blob:
  case soci::dt_date:
    return value.string;
  case soci::dt_double:
    return std::to_string(value.number.decimal);
  case soci::dt_integer:
    return std::to_string(value.number.integer);
  case soci::dt_long_long:
    return std::to_string(value.number.longLong);
  case soci::dt_unsigned_long_long:
    return std::to_string(value.number.uLongLong);
  }
  return "?";
}

std::partial_ordering Field::operator<=>(const Field& other) const {
  std::partial_ordering comp = std::partial_ordering::unordered;
  if(dType == other.dType) {
    if(dIndicator == soci::i_null)
      if(other.dIndicator == soci::i_null)
        comp = std::partial_ordering::equivalent;
      else
        comp = std::partial_ordering::less;
    else if(other.dIndicator == soci::i_null)
      comp = std::partial_ordering::greater;
    else
      switch(dType) {
      case soci::dt_string:
      case soci::dt_xml:
      case soci::dt_blob:
        return value.string <=> other.value.string;
      case soci::dt_date:
        return value.number.epoch <=> other.value.number.epoch;
      case soci::dt_double:
        return value.number.decimal <=> other.value.number.decimal;
      case soci::dt_integer:
        return value.number.integer <=> other.value.number.integer;
      case soci::dt_long_long:
        return value.number.longLong <=> other.value.number.longLong;
      case soci::dt_unsigned_long_long:
        return value.number.uLongLong <=> other.value.number.uLongLong;
      }
  }
  return comp;
}

/*****************************************************************************/

std::ostream& operator<<(std::ostream& stream, const TableInfo& var) {
  return stream << "[columns: " << var.columns.size() << "]";
}

std::ostream& operator<<(std::ostream& stream, const ColumnInfo& var) {
  stream << "`" << var.name << "` type " << var.type;
  if(var.nullable)
    stream << " nullable";
  if(var.primaryKey)
    stream << " primary key";
  return stream;
}
}

/*****************************************************************************/

namespace soci {

std::ostream& operator<<(std::ostream& stream, const data_type& var) {
  switch(var) {
  case soci::dt_string:
    return stream << "string";
  case soci::dt_date:
    return stream << "date";
  case soci::dt_double:
    return stream << "doube";
  case soci::dt_integer:
    return stream << "integer";
  case soci::dt_long_long:
    return stream << "long_long";
  case soci::dt_unsigned_long_long:
    return stream << "unsigned_long_long";
  case soci::dt_blob:
    return stream << "blob";
  case soci::dt_xml:
    return stream << "xml";
  }
  return stream << "unknown data_type " << (int)var;
}

std::ostream& operator<<(std::ostream& stream, const indicator& var) {
  switch(var) {
  case soci::i_ok:
    return stream << "i_ok";
  case soci::i_null:
    return stream << "i_null";
  case soci::i_truncated:
    return stream << "i_truncated";
  }
  return stream << "unknown indicator " << (int)var;
}

std::ostream& operator<<(std::ostream& stream, const row& var) {
  for(std::size_t i = 0; i < var.size(); ++i) {
    auto dType = var.get_properties(i).get_data_type();
    stream << '`' << var.get_properties(i).get_name() << " [";
    switch(dType) {
    case soci::dt_string:
    case soci::dt_xml:
    case soci::dt_blob:
      stream << var.get<std::string>(i);
      break;
    case soci::dt_date: {
      std::tm tm = var.get<std::tm>(i);
      stream << fmt::format("{:%F %T}", tm);
    } break;
    case soci::dt_double:
      stream << var.get<double>(i);
      break;
    case soci::dt_integer:
      stream << var.get<int>(i);
      break;
    case soci::dt_long_long:
      stream << var.get<long long>(i);
      break;
    case soci::dt_unsigned_long_long:
      stream << var.get<unsigned long long>(i);
      break;
    }
    stream << "] ";
  }
  return stream;
}
}
