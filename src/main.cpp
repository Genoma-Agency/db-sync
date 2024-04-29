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

#include <main.h>

#include <boost/program_options.hpp>
#include <db.h>
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/xml/domconfigurator.h>
#include <operation.h>

namespace po = boost::program_options;

b::optional<std::string> logConfig;
b::optional<std::string> operation;
b::optional<std::string> fromHost;
b::optional<int> fromPort;
b::optional<std::string> fromUser;
b::optional<std::string> fromPwd;
b::optional<std::string> fromSchema;
b::optional<std::string> toHost;
b::optional<int> toPort;
b::optional<std::string> toUser;
b::optional<std::string> toPwd;
b::optional<std::string> toSchema;
dbsync::strings tables;
b::optional<int> pkBulk;
b::optional<int> modifyBulk;

const po::options_description OPTIONS = [] {
  po::options_description options{ "Allowed arguments" };
  options.add_options()("help,h", "print this help message");
  options.add_options()("version,v", "print version");
  options.add_options()("copy,c", "copy records from source to target");
  options.add_options()("sync,s", "sync records from source to target");
  options.add_options()("dry-run,d", "execute without modifying the target database");
  options.add_options()("update", "enable update of records from source to target");
  options.add_options()("nofail", "don't stop if error on target records");
  options.add_options()("disablebinlog", "disable binary log (privilege required)");
  options.add_options()("fromHost", po::value<>(&fromHost), "source database host IP or name");
  options.add_options()("fromPort", po::value<>(&fromPort)->default_value(3306), "source database port");
  options.add_options()("fromUser", po::value<>(&fromUser), "source database username");
  options.add_options()("fromPwd", po::value<>(&fromPwd), "source database password");
  options.add_options()("fromSchema", po::value<>(&fromSchema), "source database schema");
  options.add_options()("toHost", po::value<>(&toHost), "target database host IP or name");
  options.add_options()("toPort", po::value<>(&toPort)->default_value(3306), "target database port");
  options.add_options()("toUser", po::value<>(&toUser), "target database username");
  options.add_options()("toPwd", po::value<>(&toPwd), "target database password");
  options.add_options()("toSchema", po::value<>(&toSchema), "target database schema");
  options.add_options()("tables",
                        po::value<>(&tables)->multitoken()->composing()->default_value(dbsync::strings(), ""),
                        "tables to process (if none are provided, use all tables)");
  options.add_options()("logConfig, l",
                        po::value<>(&logConfig)->default_value(std::string{ "./db-sync-log.xml" }),
                        "path of logger xml configuration");
  options.add_options()(
      "pkBulk", po::value<>(&pkBulk)->default_value(10000000), "number of primary keys to read with a single query");
  options.add_options()("modifyBulk",
                        po::value<>(&modifyBulk)->default_value(5000),
                        "number of records to read to insert/update in a single transaction");
  return options;
}();

po::variables_map params;

const int MAX_TABLE = 1000;

int main(int argc, char* argv[]) {
  std::setlocale(LC_ALL, "en_US.UTF-8");
  try {
    auto parsed = po::parse_command_line(argc, argv, OPTIONS);
    po::store(parsed, params);
    po::notify(params);
  } catch(std::exception& e) {
    std::cerr << e.what() << std::endl << std::endl;
    return 1;
  }

  size_t check = params.count("help") + params.count("version") + params.count("copy") + params.count("sync");
  if(check > 1) {
    std::cerr << "only one command argument allowed [help|version|copy|sync]" << std::endl;
    return 2;
  }

  if(pkBulk && *pkBulk < 0) {
    std::cerr << "pkBulk must be a positive integer" << std::endl;
    return 3;
  }

  if(modifyBulk && *modifyBulk < 0) {
    std::cerr << "modifyBulk must be a positive integer" << std::endl;
    return 4;
  }

  if(check == 0 || params.count("help")) {
    std::cout << OPTIONS << std::endl;
    return 0;
  }

  if(params.count("version")) {
    std::cout << dbsync::APP_NAME << ' ' << dbsync::APP_RELEASE << std::endl;
    return 0;
  }

  // configure logger
  bool xml = false;
  if(logConfig) {
    if(!bf::exists(*logConfig)) {
      std::cerr << "logger configuration file not found: " << *logConfig << std::endl;
    } else if(!bf::is_regular_file(*logConfig)) {
      std::cerr << "logger configuration file is not a regular file: " << *logConfig << std::endl;
    } else {
      auto logStatus = log4cxx::xml::DOMConfigurator::configure(*logConfig);
      if(logStatus == log4cxx::spi::ConfigurationStatus::NotConfigured)
        std::cerr << "error initializing logger configuration (please check logger xml configuration file): "
                  << *logConfig << std::endl;
      else
        xml = true;
    }
  }
  if(!xml)
    log4cxx::BasicConfigurator::configure();

  // configure source db
  if(!fromHost || !fromUser || !fromPwd || !fromSchema) {
    std::cerr << "all source arguments must be provided: fromHost, fromUser, fromPwd, fromSchema" << std::endl;
    return 10;
  }
  std::unique_ptr<dbsync::Db> fromDb = std::make_unique<dbsync::Db>("source");
  if(!fromDb->open(*fromHost, *fromPort, *fromSchema, *fromUser, *fromPwd)) {
    std::cerr << "source db connection error, see log file for details" << std::endl;
    return 11;
  }
  dbsync::strings fromTables{ MAX_TABLE };
  if(!fromDb->loadTables(fromTables)) {
    std::cerr << "source db load tables error, see log file for details" << std::endl;
    return 12;
  }

  // configure target db
  if(!toHost || !toUser || !toPwd || !toSchema) {
    std::cerr << "all target arguments must be provided: toHost, toUser, toPwd, toSchema" << std::endl;
    return 20;
  }
  std::unique_ptr<dbsync::Db> toDb = std::make_unique<dbsync::Db>("target");
  if(!toDb->open(*toHost, *toPort, *toSchema, *toUser, *toPwd)) {
    std::cerr << "target db connection error, see log file for details" << std::endl;
    return 21;
  }
  dbsync::strings toTables{ MAX_TABLE };
  if(!toDb->loadTables(fromTables)) {
    std::cerr << "source db load tables error, see log file for details" << std::endl;
    return 22;
  }

  std::cout << "source and target database ready" << std::endl;

  // sort and unique argument tables
  std::sort(tables.begin(), tables.end());
  auto duplicates = std::unique(tables.begin(), tables.end());
  tables.erase(duplicates, tables.end());

  dbsync::OperationConfig config{ .mode = params.count("copy") > 0 ? dbsync::Mode::Copy : dbsync::Mode::Sync,
                                  .update = params.count("update") > 0,
                                  .dryRun = params.count("dry-run") > 0,
                                  .tables = tables,
                                  .disableBinLog = params.count("disablebinlog") > 0,
                                  .noFail = params.count("nofail") > 0,
                                  .pkBulk = static_cast<std::size_t>(*pkBulk),
                                  .modifyBulk = static_cast<std::size_t>(*modifyBulk) };

  std::unique_ptr<dbsync::Operation> op
      = std::make_unique<dbsync::Operation>(config, std::move(fromDb), std::move(toDb));

  if(!op->checkTables(fromTables, toTables)) {
    std::cerr << "tables check failed" << std::endl;
    return 30;
  }

  if(!op->checkMetadata()) {
    std::cerr << "metadata check failed" << std::endl;
    return 31;
  }

  if(!op->preExecute()) {
    std::cerr << "Pre execution failed" << std::endl;
    return 40;
  }

  int ret = 0;

  if(!op->execute()) {
    std::cerr << "Execution failed" << std::endl;
    ret = 100;
  }

  if(!op->postExecute(ret == 0)) {
    std::cerr << "Post execution failed" << std::endl;
  }

  return ret;
}

namespace dbsync {

std::size_t maxMemoryKb = 0;

std::string memoryUsage() {
  std::size_t m = util::proc::memoryUsageKb();
  maxMemoryKb = std::max(m, maxMemoryKb);
  return util::proc::memoryString(m);
}

void progress(const std::string& table, TimerMs& timer, const char* t, int count, std::size_t size, bool endl) {
  static const std::string& ER = util::term::sequence::eraseRight;
  if(count == 0) {
    if(size > 0)
      std::cout << fmt::format("begin {} `{}` {} records\r", t, table, size);
    else
      std::cout << fmt::format("begin {} `{}`\r", t, table);
  } else {
    auto times = timer.elapsed(count + 1);
    auto s = times.speed<std::chrono::minutes>();
    auto e = times.elapsed().string();
    if(endl) {
      if(size > 0)
        std::cout << fmt::format("{} `{}` {} records [{:.1f} rows/min] [elapsed {}]{}", t, table, size, s, e, ER)
                  << std::endl;
      else
        std::cout << fmt::format("{} `{}` [{:.1f} rows/min] [elapsed {}]{}", t, table, s, e, ER) << std::endl;
    } else {
      auto m = times.missing().isZero() ? "?" : times.missing().string();
      if(size > 0)
        std::cout << fmt::format(
            "{} `{}` {}/{} [{:.1f} rows/min] [elapsed {}] [eta {}]{}\r", t, table, count, size, s, e, m, ER);
      else
        std::cout << fmt::format(
            "{} `{}` {} [{:.1f} rows/min] [elapsed {}] [eta {}]{}\r", t, table, count, s, e, m, ER);
    }
  }
  std::cout << std::flush;
};

// log categories
const char* LOG_MAIN = "main";
const char* LOG_DB = "db";
const char* LOG_OPERATION = "exec";
const char* LOG_DATA = "data";
}
