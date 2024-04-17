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
#include <operation.h>

namespace dbsync {

const std::string SQL_NULL_STRING{ (const char*)u8"∅" };
const std::string SQL_MD5_CHECK{ "`#MD5@CHECK#`" };

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
	and table_name = :tabella;
)#" };

Db::Db(const std::string r)
    : ref{ r }, log{ log4cxx::Logger::getLogger(LOG_DATA) } {}

Db::~Db() {
  if(session && session->is_connected()) {
    LOG4CXX_DEBUG_FMT(log, "<{}> closing db", ref);
    session->close();
  }
}

bool Db::apply(const std::string& opDesc, std::function<void(void)> lambda) {
  try {
    LOG4CXX_DEBUG(log, opDesc);
    lambda();
    error.clear();
    return true;
  } catch(soci::mysql_soci_error const& e) {
    LOG4CXX_ERROR_FMT(log, "<{}> {} error [{}]: {}", ref, opDesc, e.err_num_, e.what());
    error = fmt::format("[{}]: {}", e.err_num_, e.what());
  } catch(soci::soci_error const& e) {
    LOG4CXX_ERROR_FMT(log, "<{}> {} error: {}", ref, opDesc, e.what());
    error = e.what();
  } catch(std::exception const& e) {
    LOG4CXX_ERROR_FMT(log, "<{}> {} fault: {}", ref, opDesc, e.what());
    error = e.what();
  }
  return false;
}

bool Db::open(const std::string& h, int p, const std::string& s, const std::string& user, const std::string& pwd) {
  assert(!session);
  std::string connection = fmt::format("host={} port={} db={} user={} password={}", h, p, s, user, pwd);
  schema = s;
  return apply(fmt::format("connect {}", connection),
               [&connection, this] { session = std::make_unique<soci::session>(soci::mysql, connection); });
}

bool Db::readMetadata() {
  return apply(
      "metadata", [this] {
        strings tables{ 1000 };
        session->get_table_names(), soci::into(tables);
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
        for(int i = 0; i < tables.size(); i++) {
          table = tables[i];
          TableInfo ti;
          // row count
          *session << fmt::format("select count(*) from `{}`", table), soci::into(ti.rowCount);
          // columns
          if(stInfo.execute(true)) {
            do {
              ci.nullable = ba::iequals(isNullable, "yes");
              ci.primaryKey = pk > 0;
              ti.columns.push_back(ci);
            } while(stInfo.fetch());
          }
          //
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

bool Db::query(const std::string& sql, TableData& data) {
  return apply(sql, [&] {
    soci::rowset<soci::row> rs = (session->prepare << sql);
    for(auto it = rs.begin(); it != rs.end(); ++it)
      data.loadRow(*it);
  });
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
  return apply("exec prepared insert", [&] {
    bind(stmtWrite, row, 0, row->size());
    stmtWrite->execute(true);
    stmtWrite->bind_clean_up();
  });
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
  return apply("exec prepared update", [&] {
    bind(stmtWrite, row, 0, row->size());
    stmtWrite->execute(true);
    stmtWrite->bind_clean_up();
  });
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

bool Db::deleteExecute(const std::string& table, const std::unique_ptr<TableRow>& row) {
  assert(stmtWrite.has_value());
  return apply("exec prepared delete", [&] {
    bind(stmtWrite, row, 0, keysCount);
    stmtWrite->execute(true);
    stmtWrite->bind_clean_up();
  });
}

bool Db::selectPrepare(const std::string& table, const strings& keys) {
  keysCount = keys.size();
  assert(keysCount > 0);
  std::stringstream s;
  s << "SELECT * FROM `" << table << "` WHERE `" << keys[0] << "`=:v0";
  for(int i = 1; i < keysCount; i++)
    s << " AND `" << keys[i] << "`=:v" << i;
  std::string sql = s.str();
  return apply(sql, [&] { stmtRead = (session->prepare << sql); });
}

bool Db::selectExecute(const std::string& table, const std::unique_ptr<TableRow>& row, TableData& into) {
  assert(stmtRead.has_value());
  return apply("exec prepared select", [&] {
    bind(stmtRead, row, 0, keysCount);
    stmtRead->exchange_for_rowset(soci::into(rowSelect));
    stmtRead->execute(false);
    soci::rowset_iterator<soci::row> it(*stmtRead, rowSelect);
    soci::rowset_iterator<soci::row> end;
    for(; it != end; ++it)
      into.loadRow(rowSelect);
    stmtRead->bind_clean_up();
  });
}

void Db::bind(std::optional<soci::statement>& stmt,
              const std::unique_ptr<TableRow>& row,
              const int startIndex,
              const int endIndex) {
  static std::string nullString;
  static double nullDouble = 0;
  static int nullInt = 0;
  static long long nullLongLong = 0;
  static unsigned long long nullULongLong = 0;
  static soci::indicator nullIndicator = soci::i_null;
  assert(startIndex < endIndex);
  assert(stmt.has_value());
  for(int i = startIndex; i < endIndex; i++) {
    switch(row->at(i)->type()) {
    case soci::dt_string:
    case soci::dt_xml:
    case soci::dt_blob:
    case soci::dt_date:
      if(row->at(i)->isNull())
        stmt->exchange(soci::use(nullString, nullIndicator));
      else
        stmt->exchange(soci::use(row->at(i)->asString()));
      break;
    case soci::dt_double:
      if(row->at(i)->isNull())
        stmt->exchange(soci::use(nullDouble, nullIndicator));
      else
        stmt->exchange(soci::use(row->at(i)->asDouble()));
      break;
    case soci::dt_integer:
      if(row->at(i)->isNull())
        stmt->exchange(soci::use(nullInt, nullIndicator));
      else
        stmt->exchange(soci::use(row->at(i)->asInt()));
      break;
    case soci::dt_long_long:
      if(row->at(i)->isNull())
        stmt->exchange(soci::use(nullLongLong, nullIndicator));
      else
        stmt->exchange(soci::use(row->at(i)->asLongLong()));
      break;
    case soci::dt_unsigned_long_long:
      if(row->at(i)->isNull())
        stmt->exchange(soci::use(nullULongLong, nullIndicator));
      else
        stmt->exchange(soci::use(row->at(i)->asULongLong()));
      break;
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
  if(dIndicator == soci::i_null) {
    value.string = SQL_NULL_STRING;
    return;
  }
  switch(dType) {
  case soci::dt_string:
  case soci::dt_xml:
  case soci::dt_blob:
    value.string = row.get<std::string>(i);
    break;
  case soci::dt_double:
    value.number.decimal = row.get<double>(i);
    value.string = std::to_string(row.get<double>(i));
    break;
  case soci::dt_integer:
    value.number.integer = row.get<int>(i);
    value.string = std::to_string(row.get<int>(i));
    break;
  case soci::dt_long_long:
    value.number.longLong = row.get<long long>(i);
    value.string = std::to_string(row.get<long long>(i));
    break;
  case soci::dt_unsigned_long_long:
    value.number.uLongLong = row.get<unsigned long long>(i);
    value.string = std::to_string(row.get<unsigned long long>(i));
    break;
  case soci::dt_date:
    std::tm tm = row.get<std::tm>(i);
    value.number.epoch = std::mktime(&tm);
    value.string = fmt::format("{:%F %T}", tm);
    break;
  }
}

/*
    static const partial_ordering less;
    static const partial_ordering equivalent;
    static const partial_ordering greater;
    static const partial_ordering unordered;
*/
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
      case soci::dt_date:
        return value.string <=> other.value.string;
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
  return stream << "[rows: " << var.rowCount << "] [columns: " << var.columns.size() << "]";
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
}
