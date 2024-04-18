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

#include <boost/algorithm/algorithm.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/optional.hpp>
#include <boost/optional/optional_io.hpp>
#include <cassert>
#include <chrono>
#include <fmt/chrono.h>
#include <fmt/core.h>
#include <fmt/ostream.h>
#include <iomanip>
#include <iostream>
#include <log4cxx/logger.h>
#include <optional>
#include <set>
#include <soci/soci.h>
#include <string>
#include <time.h>
#include <variant>
#include <vector>
#include <version.h>

namespace b = boost;
namespace bf = boost::filesystem;
namespace ba = boost::algorithm;

namespace dbsync {

using strings = std::vector<std::string>;

// log categories
extern const char* LOG_MAIN;
extern const char* LOG_DB;
extern const char* LOG_OPERATION;
extern const char* LOG_DATA;
}
