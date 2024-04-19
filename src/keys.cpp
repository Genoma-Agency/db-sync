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

TableKeys::TableKeys(bool source, const std::string& t, size_t sh, bool u)
    : sizeHint{ sh }, update{ u }, count{ 0 } {}

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
    case soci::dt_blob:
      v = vS(sizeHint);
      break;
    case soci::dt_date:
      v = vT(sizeHint, 0);
      break;
    case soci::dt_double:
      v = vD(sizeHint, 0.0);
      break;
    case soci::dt_integer:
      v = vI(sizeHint, 0);
      break;
    case soci::dt_long_long:
      v = vLL(sizeHint, 0);
      break;
    case soci::dt_unsigned_long_long:
      v = vULL(sizeHint, 0);
      break;
    }
    keys.emplace_back(std::make_pair(dType, v));
  }
}

void TableKeys::loadRow(const soci::row& row) {
  assert(count < sizeHint);
  if(count == 0)
    init(row);
  for(std::size_t i = 0; i < row.size(); ++i) {
    auto dType = row.get_properties(i).get_data_type();
    switch(dType) {
    case soci::dt_string:
    case soci::dt_xml:
    case soci::dt_blob:
      std::get<vS>(keys[i].second)[count] = row.get<std::string>(i);
      break;
    case soci::dt_date: {
      std::tm tm = row.get<std::tm>(i);
      std::get<vT>(keys[i].second)[count] = std::mktime(&tm);
    } break;
    case soci::dt_double:
      std::get<vD>(keys[i].second)[count] = row.get<double>(i);
      break;
    case soci::dt_integer:
      std::get<vI>(keys[i].second)[count] = row.get<int>(i);
      break;
    case soci::dt_long_long:
      std::get<vLL>(keys[i].second)[count] = row.get<long long>(i);
      break;
    case soci::dt_unsigned_long_long:
      std::get<vULL>(keys[i].second)[count] = row.get<unsigned long long>(i);
      break;
    }
  }
  count++;
}

void TableKeys::bind(soci::statement& stmt, long index) const {
  int columns = keys.size() - (update ? 1 : 0);
  for(int i = 0; i < columns; i++) {
    switch(keys[i].first) {
    case soci::dt_string:
    case soci::dt_xml:
    case soci::dt_blob:
      stmt.exchange(soci::use(std::get<vS>(keys[i].second)[index]));
      break;
    case soci::dt_date:
      stmt.exchange(soci::use(std::get<vT>(keys[i].second)[index]));
      break;
    case soci::dt_double:
      stmt.exchange(soci::use(std::get<vD>(keys[i].second)[index]));
      break;
    case soci::dt_integer:
      stmt.exchange(soci::use(std::get<vI>(keys[i].second)[index]));
      break;
    case soci::dt_long_long:
      stmt.exchange(soci::use(std::get<vLL>(keys[i].second)[index]));
      break;
    case soci::dt_unsigned_long_long:
      stmt.exchange(soci::use(std::get<vULL>(keys[i].second)[index]));
      break;
    }
  }
  stmt.define_and_bind();
}

std::string TableKeys::rowString(long index) {
  std::stringstream s;
  int columns = keys.size() - (update ? 1 : 0);
  for(int i = 0; i < columns; i++) {
    s << names[i] << '[';
    switch(keys[i].first) {
    case soci::dt_string:
    case soci::dt_xml:
    case soci::dt_blob:
      s << std::get<vS>(keys[i].second)[index];
      break;
    case soci::dt_date: {
      std::tm* tm = std::localtime(&std::get<vT>(keys[i].second)[index]);
      s << fmt::format("{:%F %T}", *tm);
    } break;
    case soci::dt_double:
      s << std::get<vD>(keys[i].second)[index];
      break;
    case soci::dt_integer:
      s << std::get<vI>(keys[i].second)[index];
      break;
    case soci::dt_long_long:
      s << std::get<vLL>(keys[i].second)[index];
      break;
    case soci::dt_unsigned_long_long:
      s << std::get<vULL>(keys[i].second)[index];
      break;
    }
    s << "] ";
  }
  return s.str();
}

void TableKeys::sort() {
  assert(count <= std::numeric_limits<long>::max());
  if(count > 0)
    quickSort(0, count - 1);
};

void TableKeys::quickSort(long left, long right) {
  int i = left, j = right;
  int pivot = (left + right) / 2;
  while(i <= j) {
    while(less(i, pivot))
      i++;
    while(less(pivot, j))
      j--;
    if(i <= j)
      swap(i++, j--);
  };
  if(left < j)
    quickSort(left, j);
  if(i < right)
    quickSort(i, right);
}

bool TableKeys::less(long i1, const TableKeys& other, long i2) const {
  if(i1 == i2)
    return false;
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
  }
  return comp == std::partial_ordering::less;
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
