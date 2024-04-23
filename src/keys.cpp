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

#include <keys.h>

namespace dbsync {

const std::size_t RESERVE = 1000000;

auto log = log4cxx::Logger::getLogger("keys");

TableKeys::TableKeys(bool u)
    : update{ u }, count{ 0 }, sorted(true) {}

void TableKeys::init(const soci::row& row) {
  const int end = update ? row.size() - 1 : row.size();
  for(std::size_t i = 0; i < end; i++)
    names.push_back(row.get_properties(i).get_name());
  for(std::size_t i = 0; i < row.size(); ++i) {
    vect v;
    auto dType = row.get_properties(i).get_data_type();
    switch(dType) {
    case soci::dt_string:
    case soci::dt_xml:
    case soci::dt_blob: {
      vS tmp;
      tmp.reserve(RESERVE);
      v = tmp;
    } break;
    case soci::dt_date: {
      vT tmp;
      tmp.reserve(RESERVE);
      v = tmp;
    } break;
    case soci::dt_double: {
      vD tmp;
      tmp.reserve(RESERVE);
      v = tmp;
    } break;
    case soci::dt_integer: {
      vI tmp;
      tmp.reserve(RESERVE);
      v = tmp;
    } break;
    case soci::dt_long_long: {
      vLL tmp;
      tmp.reserve(RESERVE);
      v = tmp;
    } break;
    case soci::dt_unsigned_long_long: {
      vULL tmp;
      tmp.reserve(RESERVE);
      v = tmp;
    } break;
    }
    keys.emplace_back(std::make_pair(dType, v));
  }
}

void TableKeys::loadRow(const soci::row& row) {
  assert(count < std::numeric_limits<long>::max());
  if(count == 0)
    init(row);
  for(std::size_t i = 0; i < row.size(); ++i) {
    auto dType = row.get_properties(i).get_data_type();
    switch(dType) {
    case soci::dt_string:
    case soci::dt_xml:
    case soci::dt_blob:
      std::get<vS>(keys[i].second).emplace_back(row.get<std::string>(i));
      break;
    case soci::dt_date: {
      std::tm tm = row.get<std::tm>(i);
      std::get<vT>(keys[i].second).emplace_back(std::mktime(&tm));
    } break;
    case soci::dt_double:
      std::get<vD>(keys[i].second).emplace_back(row.get<double>(i));
      break;
    case soci::dt_integer:
      std::get<vI>(keys[i].second).emplace_back(row.get<int>(i));
      break;
    case soci::dt_long_long:
      std::get<vLL>(keys[i].second).emplace_back(row.get<long long>(i));
      break;
    case soci::dt_unsigned_long_long:
      std::get<vULL>(keys[i].second).emplace_back(row.get<unsigned long long>(i));
      break;
    }
  }
  if(count > 0 && sorted)
    sorted = less(count - 1, count);
  count++;
}

void TableKeys::bind(soci::statement& stmt, long i) const {
  assert(i < count);
  long idx = index[i];
  int columns = keys.size() - (update ? 1 : 0);
  for(int i = 0; i < columns; i++) {
    switch(keys[i].first) {
    case soci::dt_string:
    case soci::dt_xml:
    case soci::dt_blob:
      stmt.exchange(soci::use(std::get<vS>(keys[i].second)[idx]));
      break;
    case soci::dt_date:
      stmt.exchange(soci::use(std::get<vT>(keys[i].second)[idx]));
      break;
    case soci::dt_double:
      stmt.exchange(soci::use(std::get<vD>(keys[i].second)[idx]));
      break;
    case soci::dt_integer:
      stmt.exchange(soci::use(std::get<vI>(keys[i].second)[idx]));
      break;
    case soci::dt_long_long:
      stmt.exchange(soci::use(std::get<vLL>(keys[i].second)[idx]));
      break;
    case soci::dt_unsigned_long_long:
      stmt.exchange(soci::use(std::get<vULL>(keys[i].second)[idx]));
      break;
    }
  }
  stmt.define_and_bind();
}

std::string TableKeys::rowString(long i) const {
  long idx = index[i];
  std::stringstream s;
  int columns = keys.size() - (update ? 1 : 0);
  for(int i = 0; i < columns; i++) {
    s << names[i] << '[';
    switch(keys[i].first) {
    case soci::dt_string:
    case soci::dt_xml:
    case soci::dt_blob:
      s << std::get<vS>(keys[i].second)[idx];
      break;
    case soci::dt_date: {
      std::tm* tm = std::localtime(&std::get<vT>(keys[i].second)[idx]);
      s << fmt::format("{:%F %T}", *tm);
    } break;
    case soci::dt_double:
      s << std::get<vD>(keys[i].second)[idx];
      break;
    case soci::dt_integer:
      s << std::get<vI>(keys[i].second)[idx];
      break;
    case soci::dt_long_long:
      s << std::get<vLL>(keys[i].second)[idx];
      break;
    case soci::dt_unsigned_long_long:
      s << std::get<vULL>(keys[i].second)[idx];
      break;
    }
    s << "] ";
  }
  return s.str();
}

void TableKeys::sort() {
  assert(index.empty());
  assert(count <= std::numeric_limits<long>::max());
  index.reserve(count);
  for(long i = 0; i < count; index.emplace_back(i++))
    ;
  if(count > 0 && !sorted)
    std::sort(index.begin(), index.end(), [&](const long& i1, const long& i2) { return less(i1, i2); });
  for(int c = 1; c < count; c++)
    assert(less(index[c - 1], index[c]));
};

bool TableKeys::less(long i1, const TableKeys& other, long i2) const {
  std::partial_ordering c = compare(index[i1], other, other.index[i2]);
  return c == std::partial_ordering::less;
}
bool TableKeys::less(long i1, long i2) const {
  if(i1 == i2)
    return false;
  std::partial_ordering c = compare(i1, *this, i2);
  return c == std::partial_ordering::less;
}

std::partial_ordering TableKeys::compare(long i1, const TableKeys& other, long i2) const {
  std::partial_ordering comp = std::partial_ordering::equivalent;
  int columns = keys.size() - (update ? 1 : 0);
  for(int i = 0; comp == std::partial_ordering::equivalent && i < columns; i++) {
    switch(keys[i].first) {
    case soci::dt_string:
    case soci::dt_xml:
    case soci::dt_blob:
      comp = std::get<vS>(keys[i].second)[i1] <=> std::get<vS>(other.keys[i].second)[i2];
      break;
    case soci::dt_date:
      comp = std::get<vT>(keys[i].second)[i1] <=> std::get<vT>(other.keys[i].second)[i2];
      break;
    case soci::dt_double:
      comp = std::get<vD>(keys[i].second)[i1] <=> std::get<vD>(other.keys[i].second)[i2];
      break;
    case soci::dt_integer:
      comp = std::get<vI>(keys[i].second)[i1] <=> std::get<vI>(other.keys[i].second)[i2];
      break;
    case soci::dt_long_long:
      comp = std::get<vLL>(keys[i].second)[i1] <=> std::get<vLL>(other.keys[i].second)[i2];
      break;
    case soci::dt_unsigned_long_long:
      comp = std::get<vULL>(keys[i].second)[i1] <=> std::get<vULL>(other.keys[i].second)[i2];
      break;
    }
    assert(comp != std::partial_ordering::unordered);
  }
  return comp;
}

void TableKeys::swap(long i1, long i2) {
  if(i1 == i2)
    return;
  for(int i = 0; i < keys.size(); i++) {
    switch(keys[i].first) {
    case soci::dt_string:
    case soci::dt_xml:
    case soci::dt_blob:
      std::swap(std::get<vS>(keys[i].second)[i1], std::get<vS>(keys[i].second)[i2]);
      break;
    case soci::dt_date:
      std::swap(std::get<vT>(keys[i].second)[i1], std::get<vT>(keys[i].second)[i2]);
      break;
    case soci::dt_double:
      std::swap(std::get<vD>(keys[i].second)[i1], std::get<vD>(keys[i].second)[i2]);
      break;
    case soci::dt_integer:
      std::swap(std::get<vI>(keys[i].second)[i1], std::get<vI>(keys[i].second)[i2]);
      break;
    case soci::dt_long_long:
      std::swap(std::get<vLL>(keys[i].second)[i1], std::get<vLL>(keys[i].second)[i2]);
      break;
    case soci::dt_unsigned_long_long:
      std::swap(std::get<vULL>(keys[i].second)[i1], std::get<vULL>(keys[i].second)[i2]);
      break;
    }
  }
}

bool TableKeys::updateEqual(long i1, const TableKeys& other, long i2) const {
  assert(update && other.update);
  int i = keys.size() - 1;
  std::partial_ordering comp = std::get<vS>(keys[i].second)[i1] <=> std::get<vS>(other.keys[i].second)[i2];
  return comp == std::partial_ordering::equivalent;
}
}
