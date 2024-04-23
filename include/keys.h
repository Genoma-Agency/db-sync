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

#include <main.h>
#include <soci/soci.h>

namespace dbsync {

class TableKeys {

public:
  TableKeys(bool update);
  void loadRow(const soci::row& row);
  void sort();
  const bool hasUpdateCheck() const { return update; };
  const size_t size() const { return count; }
  bool less(long i1, const TableKeys& other, long i2) const;
  bool updateEqual(long i1, const TableKeys& other, long i2) const;
  const strings& columnNames() const { return names; };
  void bind(soci::statement& stmt, long index) const;
  std::string rowString(long index) const;

private:
  void init(const soci::row& row);
  bool less(long i1, long i2) const;
  std::partial_ordering compare(long i1, const TableKeys& other, long i2) const;
  void swap(long i1, long i2);

private:
  using vI = std::vector<int>;
  using vLL = std::vector<long long>;
  using vULL = std::vector<unsigned long long>;
  using vD = std::vector<double>;
  using vT = std::vector<std::time_t>;
  using vS = std::vector<std::string>;
  using vect = std::variant<vI, vLL, vULL, vD, vT, vS>;
  using key_type = std::pair<soci::data_type, vect>;
  const bool update;
  std::size_t count;
  strings names;
  std::vector<long> index;
  std::vector<key_type> keys;
  bool sorted;
};

}
