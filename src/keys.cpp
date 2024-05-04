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

#include <execution>
#include <keys.h>

namespace dbsync {

const std::size_t RESERVE = 10000000;

auto log = log4cxx::Logger::getLogger("keys");

TableKeys::TableKeys()
    : count{ 0 }, sorted(true) {}

TableKeysIterator TableKeys::iter(bool flag) const {
  std::size_t index = 0;
  while(flags[index] != flag && index < count)
    index++;
  return TableKeysIterator{ *this, index };
}

void TableKeys::init(const soci::row& row) {
  for(std::size_t i = 0; i < row.size(); i++)
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
  assert(count < std::numeric_limits<std::size_t>::max());
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
  count++;
  if(count > 1 && sorted)
    sorted = less(count - 2, count - 1);
}

void TableKeys::bind(soci::statement& stmt, std::size_t i) const {
  assert(i < count);
  auto idx = index[i];
  for(int i = 0; i < keys.size(); i++) {
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

std::string TableKeys::rowString(std::size_t i) const {
  assert(i < count);
  std::size_t idx = index[i];
  std::stringstream s;
  for(int i = 0; i < keys.size(); i++) {
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

void TableKeys::sort(const char* ref) {
  assert(index.empty());
  index.reserve(count);
  flags.reserve(count);
  TimerMs timer;
  LOG4CXX_DEBUG_FMT(log, "sort {} begin [keys: {}] [RSS: {}]", ref, count, memoryUsage());
  for(std::size_t i = 0; i < count; i++) {
    index.emplace_back(i);
    flags.emplace_back(false);
  }
  LOG4CXX_TRACE_FMT(log, "sort {} index [RSS: {}]", ref, memoryUsage());
  if(count > 0 && !sorted)
    std::sort(std::execution::seq, index.begin(), index.end(), [&](const std::size_t& i1, const std::size_t& i2) {
      return less(i1, i2);
    });
  auto e = timer.elapsed(count);
  LOG4CXX_DEBUG_FMT(log,
                    "sort {} done [{} keys/sec] [elapsed {}] [RSS: {}]",
                    ref,
                    (int)e.speed<std::chrono::seconds>(),
                    e.elapsed().string(),
                    memoryUsage());
#ifdef DEBUG
  for(std::size_t c = 1; c < count; c++)
    assert(less(index[c - 1], index[c]));
  LOG4CXX_TRACE_FMT(log, "sort checked [RSS: {}]", memoryUsage());
#endif
}

bool TableKeys::less(std::size_t i1, const TableKeys& other, std::size_t i2) const {
  assert(i1 < count);
  assert(i2 < other.count);
  std::partial_ordering c = compare(index[i1], other, other.index[i2]);
  return c == std::partial_ordering::less;
}

bool TableKeys::less(std::size_t i1, std::size_t i2) const {
  assert(i1 < count);
  assert(i2 < count);
  if(i1 == i2)
    return false;
  std::partial_ordering c = compare(i1, *this, i2);
  return c == std::partial_ordering::less;
}

bool TableKeys::check(std::size_t idx, DbRecord record) const {
  assert(idx < count);
  assert(keys.size() == record.size());
  std::partial_ordering comp = std::partial_ordering::equivalent;
  for(std::size_t i = 0; comp == std::partial_ordering::equivalent && i < keys.size(); i++) {
    if(keys[i].first != record[i].first) {
      comp = std::partial_ordering::unordered;
      break;
    }
    switch(keys[i].first) {
    case soci::dt_string:
    case soci::dt_xml:
    case soci::dt_blob:
      comp = std::get<vS>(keys[i].second)[index[idx]] <=> std::get<std::string>(record[i].second);
      break;
    case soci::dt_date:
      comp = std::get<vT>(keys[i].second)[index[idx]] <=> std::get<std::time_t>(record[i].second);
      break;
    case soci::dt_double:
      comp = std::get<vD>(keys[i].second)[index[idx]] <=> std::get<double>(record[i].second);
      break;
    case soci::dt_integer:
      comp = std::get<vI>(keys[i].second)[index[idx]] <=> std::get<int>(record[i].second);
      break;
    case soci::dt_long_long:
      comp = std::get<vLL>(keys[i].second)[index[idx]] <=> std::get<long long>(record[i].second);
      break;
    case soci::dt_unsigned_long_long:
      comp = std::get<vULL>(keys[i].second)[index[idx]] <=> std::get<unsigned long long>(record[i].second);
      break;
    }
    assert(comp != std::partial_ordering::unordered);
  }
  return comp == std::partial_ordering::equivalent;
}

std::partial_ordering TableKeys::compare(std::size_t i1, const TableKeys& other, std::size_t i2) const {
  assert(i1 < count);
  assert(i2 < other.count);
  std::partial_ordering comp = std::partial_ordering::equivalent;
  for(std::size_t i = 0; comp == std::partial_ordering::equivalent && i < keys.size(); i++) {
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

void TableKeys::swap(std::size_t i1, std::size_t i2) {
  assert(i1 < count);
  assert(i2 < count);
  if(i1 == i2)
    return;
  for(std::size_t i = 0; i < keys.size(); i++) {
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

/*****************************************************************************/

}
