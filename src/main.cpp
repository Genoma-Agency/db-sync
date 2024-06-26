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
#include <signal.h>
#include <unistd.h>

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
b::optional<int> jobs;
b::optional<int> pkBulk;
b::optional<int> compareBulk;
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
  options.add_options()("jobs",
                        po::value<>(&jobs)->default_value(1),
                        "number of parallel execution jobs, use 0 to set as the numbers of cores");
  options.add_options()(
      "pkBulk", po::value<>(&pkBulk)->default_value(10000000), "number of primary keys to read with a single query");
  options.add_options()("compareBulk",
                        po::value<>(&compareBulk)->default_value(10000),
                        "number of records to read to compare md5 content when option 'update' is used");
  options.add_options()("modifyBulk",
                        po::value<>(&modifyBulk)->default_value(5000),
                        "number of records to read to insert/update in a single transaction");
  return options;
}();

po::variables_map params;

const int MAX_TABLE = 1000;

std::shared_ptr<dbsync::Operation> manager;

void sigHandler(int unused) {
  if(manager)
    manager->stop();
}

int main(int argc, char* argv[]) {
  dbsync::TimerMs timer;
  std::setlocale(LC_ALL, "en_US.UTF-8");
  try {
    auto parsed = po::parse_command_line(argc, argv, OPTIONS);
    po::store(parsed, params);
    po::notify(params);
  } catch(std::exception& e) {
    std::cerr << e.what() << std::endl << std::endl;
    return 1;
  }
  // check arguments
  size_t check = params.count("help") + params.count("version") + params.count("copy") + params.count("sync");
  if(check > 1) {
    std::cerr << "only one command argument allowed [help|version|copy|sync]" << std::endl;
    return 2;
  }
  if(jobs && *jobs < 0) {
    std::cerr << "jobs must be a positive integer" << std::endl;
    return 3;
  }
  if(pkBulk && *pkBulk < 1) {
    std::cerr << "pkBulk must be a positive integer" << std::endl;
    return 4;
  }
  if(modifyBulk && *modifyBulk < 1) {
    std::cerr << "modifyBulk must be a positive integer" << std::endl;
    return 5;
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
  std::shared_ptr<dbsync::DbMeta> fromDb = std::make_shared<dbsync::DbMeta>("source");
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
  std::shared_ptr<dbsync::DbMeta> toDb = std::make_shared<dbsync::DbMeta>("target");
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
  // check metadata
  dbsync::OperationConfig config{ .mode = params.count("copy") > 0 ? dbsync::Mode::Copy : dbsync::Mode::Sync,
                                  .update = params.count("update") > 0,
                                  .dryRun = params.count("dry-run") > 0,
                                  .tables = tables,
                                  .disableBinLog = params.count("disablebinlog") > 0,
                                  .noFail = params.count("nofail") > 0,
                                  .pkBulk = static_cast<std::size_t>(*pkBulk),
                                  .compareBulk = static_cast<std::size_t>(*compareBulk),
                                  .modifyBulk = static_cast<std::size_t>(*modifyBulk) };
  manager = std::make_shared<dbsync::Operation>(config, fromDb, toDb);
  if(!manager->checkTables(fromTables, toTables)) {
    std::cerr << "tables check failed" << std::endl;
    return 30;
  }
  if(!manager->checkMetadata()) {
    std::cerr << "metadata check failed" << std::endl;
    return 31;
  }
  // signal handler
  struct sigaction sig;
  sig.sa_handler = sigHandler;
  sigemptyset(&sig.sa_mask);
  sig.sa_flags = 0;
  sig.sa_flags |= SA_RESTART;
  if(sigaction(SIGINT, &sig, 0) && sigaction(SIGTERM, &sig, 0) && sigaction(SIGQUIT, &sig, 0)) {
    std::cerr << "error installing signal handlers" << std::endl;
    return 50;
  }
  // create and initialize workers
  int jobCount = std::min(manager->tablesCount(), *jobs > 0 ? *jobs : (int)std::thread::hardware_concurrency());
  bool ok = true;
  std::vector<dbsync::OpJob> workers;
  for(int i = 0; ok && i < jobCount; i++) {
    std::cout << "creating job " << i + 1 << std::endl;
    dbsync::OpJob& worker = workers.emplace_back(manager);
    ok &= worker.init();
  }
  if(!ok) {
    std::cerr << "worker jobs initilization failed" << std::endl;
    return 40;
  }
  // start jobs
  std::vector<std::thread> threads(jobCount);
  for(int i = 0; i < jobCount; i++)
    threads[i] = std::thread([i, &workers] { workers[i].execute(); });
  // wait thread termination
  bool someRunning = true;
  do {
    if(someRunning)
      std::this_thread::sleep_for(std::chrono::seconds(1));
    someRunning = false;
    for(auto& worker : workers) {
      if(worker.isRunning()) {
        someRunning = true;
      } else {
        ok &= worker.result();
      }
    }
    if(!ok && manager->canRun())
      manager->stop();
  } while(someRunning);
  for(auto& thread : threads)
    thread.join();
  auto time = timer.elapsed().elapsed().string();
  std::cout << fmt::format(
      "completed in {} db R/W {:L} maximum memory used {}", time, manager->rwCount(), util::proc::maxMemoryUsage());
  manager.reset();
  return ok ? 0 : 100;
}

namespace dbsync {

std::size_t maxMemoryKb = 0;

std::string memoryUsage() {
  std::size_t m = util::proc::memoryUsageKb();
  maxMemoryKb = std::max(m, maxMemoryKb);
  return util::proc::memoryString(m);
}

void progress(
    log4cxx::LoggerPtr& log, const std::string& table, TimerMs& timer, const char* t, int count, std::size_t size) {
  if(count == 0) {
    if(size > 0)
      LOG4CXX_INFO_FMT(log, "`{}` begin {} {} records", table, t, size);
    else
      LOG4CXX_INFO_FMT(log, "`{}` begin {} ", table, t);
  } else {
    auto times = timer.elapsed(count + 1);
    auto s = times.speed<std::chrono::seconds>();
    auto e = times.elapsed().string();
    auto m = times.missing().isZero() ? "?" : times.missing().string();
    if(size > 0)
      LOG4CXX_INFO_FMT(log, "`{}` {} {}/{} [{:.1f} rows/sec] [elapsed {}] [eta {}]", table, t, count, size, s, e, m);
    else
      LOG4CXX_INFO_FMT(log, "`{}` {} {} [{:.1f} rows/sec] [elapsed {}]", table, t, count, s, e);
  }
};

// log categories
const char* LOG_MAIN = "main";
const char* LOG_DB = "db";
const char* LOG_OPERATION = "exec";
const char* LOG_DATA = "data";
}
