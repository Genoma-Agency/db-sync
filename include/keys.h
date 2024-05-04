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

class TableKeysIterator;

class TableKeys {
  friend class TableKeysIterator;

public:
  TableKeys();
  void loadRow(const soci::row& row);
  void sort(const char* ref);
  std::size_t size() const { return count; }
  bool less(std::size_t i1, const TableKeys& other, std::size_t i2) const;
  const strings& columnNames() const { return names; };
  void bind(soci::statement& stmt, std::size_t index) const;
  std::string rowString(std::size_t index) const;
  void setFlag(std::size_t index, bool value = true) { flags.at(index) = value; }
  void revertFlags() { flags.flip(); }
  std::size_t size(bool flag) const { return std::count(flags.begin(), flags.end(), flag); };
  TableKeysIterator iter(bool flag) const;
  bool check(std::size_t index, DbRecord record) const;

private:
  void init(const soci::row& row);
  bool less(std::size_t i1, std::size_t i2) const;
  std::partial_ordering compare(std::size_t i1, const TableKeys& other, std::size_t i2) const;
  void swap(std::size_t i1, std::size_t i2);

private:
  using vI = std::vector<int>;
  using vLL = std::vector<long long>;
  using vULL = std::vector<unsigned long long>;
  using vD = std::vector<double>;
  using vT = std::vector<std::time_t>;
  using vS = std::vector<std::string>;
  using vect = std::variant<vI, vLL, vULL, vD, vT, vS>;
  using key_type = std::pair<soci::data_type, vect>;
  std::size_t count;
  strings names;
  std::vector<std::size_t> index;
  std::vector<key_type> keys;
  std::vector<bool> flags;
  bool sorted;
};

/*****************************************************************************/

class TableKeysIterator {
public:
public:
  TableKeysIterator(const TableKeys& k, std::size_t i)
      : keys{ k }, flag{ k.flags[i] }, index{ i } {};
  TableKeysIterator(TableKeysIterator const& other)
      : keys{ other.keys }, flag{ other.flag }, index{ other.index } {};
  std::size_t value() const { return index; }
  std::size_t ref() const { return keys.index[index]; }
  bool end() const { return index >= keys.count; }
  TableKeysIterator& operator++() {
    do {
      index++;
    } while(keys.flags[index] != flag && index < keys.count);
    return *this;
  }

private:
  const TableKeys& keys;
  const bool flag;
  std::size_t index;
};
}
