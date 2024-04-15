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
        std::vector<std::string> tables{ 1000 };
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
    for(auto it = rs.begin(); it != rs.end(); ++it) {
      auto& row = *it;
      data.loadRow(row);
    }
  });
}

bool Db::query(const std::string& sql, std::function<void(const soci::row&)> consumer) {
  return apply(sql, [&] {
    soci::rowset<soci::row> rs = (session->prepare << sql);
    for(auto it = rs.begin(); it != rs.end(); ++it) {
      auto& row = *it;
      consumer(row);
    }
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
  return apply(sql, [&] { stmtInsert = (session->prepare << sql); });
}

bool Db::insertExecute(const std::string& table, const TableRow& row) {
  assert(map.at(table).columns.size() == row.size());
  assert(stmtInsert.has_value());
  return apply("exec prepared insert", [&] {
    bind(stmtInsert, row);
    stmtInsert->execute(true);
    stmtInsert->bind_clean_up();
  });
}

bool Db::selectPrepare(const std::string& table, const std::vector<std::string>& names) {
  assert(!names.empty());
  std::stringstream s;
  s << "SELECT * FROM `" << table << "` WHERE `" << names[0] << "`=:v0";
  for(int i = 1; i < names.size(); i++)
    s << " AND `" << names[i] << "`=:v" << i;
  std::string sql = s.str();
  return apply(sql, [&] { stmtSelect = (session->prepare << sql); });
}

bool Db::selectExecute(const std::string& table, const TableRow& row, TableData& into) {
  assert(stmtSelect.has_value());
  return apply("exec prepared select", [&] {
    bind(stmtSelect, row);
    stmtSelect->exchange_for_rowset(soci::into(rowSelect));
    stmtSelect->execute(true);
    soci::rowset_iterator<soci::row> it(*stmtSelect, rowSelect);
    soci::rowset_iterator<soci::row> end;
    for(; it != end; ++it)
      into.loadRow(*it);
    stmtSelect->bind_clean_up();
  });
}

void Db::bind(std::optional<soci::statement>& stmt, const TableRow& row) {
  static std::string nullString;
  static double nullDouble = 0;
  static int nullInt = 0;
  static long long nullLongLong = 0;
  static unsigned long long nullULongLong = 0;
  static soci::indicator nullIndicator = soci::i_null;
  assert(stmt.has_value());
  for(int i = 0; i < row.size(); i++) {
    auto field = row[i];
    switch(field.type()) {
    case soci::dt_string:
    case soci::dt_xml:
    case soci::dt_blob:
      if(field.isNull())
        stmt->exchange(soci::use(nullString, nullIndicator));
      else
        stmt->exchange(soci::use(std::get<std::string>(field.variant())));
      break;
    case soci::dt_date:
      if(field.isNull())
        stmt->exchange(soci::use(nullString, nullIndicator));
      else
        stmt->exchange(soci::use(field.toString()));
      break;
    case soci::dt_double:
      if(field.isNull())
        stmt->exchange(soci::use(nullDouble, nullIndicator));
      else
        stmt->exchange(soci::use(std::get<double>(field.variant())));
      break;
    case soci::dt_integer:
      if(field.isNull())
        stmt->exchange(soci::use(nullInt, nullIndicator));
      else
        stmt->exchange(soci::use(std::get<int>(field.variant())));
      break;
    case soci::dt_long_long:
      if(field.isNull())
        stmt->exchange(soci::use(nullLongLong, nullIndicator));
      else
        stmt->exchange(soci::use(std::get<long long>(field.variant())));
      break;
    case soci::dt_unsigned_long_long:
      if(field.isNull())
        stmt->exchange(soci::use(nullULongLong, nullIndicator));
      else
        stmt->exchange(soci::use(std::get<unsigned long long>(field.variant())));
      break;
    }
  }
  stmt->define_and_bind();
}

/*****************************************************************************/

Field::Field(const soci::row& row, const std::size_t i) {
  auto props = row.get_properties(i);
  dType = props.get_data_type();
  dIndicator = row.get_indicator(i);
  if(dIndicator == soci::i_null) {
    valueString = std::string((const char*)u8"∅");
    return;
  }
  switch(dType) {
  case soci::dt_string:
  case soci::dt_xml:
  case soci::dt_blob:
    value = row.get<std::string>(i);
    valueString = row.get<std::string>(i);
    break;
  case soci::dt_double:
    value = row.get<double>(i);
    valueString = std::to_string(row.get<double>(i));
    break;
  case soci::dt_integer:
    value = row.get<int>(i);
    valueString = std::to_string(row.get<int>(i));
    break;
  case soci::dt_long_long:
    value = row.get<long long>(i);
    valueString = std::to_string(row.get<long long>(i));
    break;
  case soci::dt_unsigned_long_long:
    value = row.get<unsigned long long>(i);
    valueString = std::to_string(row.get<unsigned long long>(i));
    break;
  case soci::dt_date:
    std::tm tm = row.get<std::tm>(i);
    std::time_t epoch = std::mktime(&tm);
    value = epoch;
    std::chrono::system_clock::time_point tp{ std::chrono::seconds{ epoch } };
    valueString = std::format("{:%F %T}", tp);
    valueString.resize(sizeof "yyyy-mm-dd hh:mm:ss" - 1);
    break;
  }
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
