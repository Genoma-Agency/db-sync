#include <main.h>

#include <boost/optional.hpp>
#include <boost/optional/optional_io.hpp>
#include <boost/program_options.hpp>
#include <iostream>

namespace b = boost;
namespace po = boost::program_options;

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
b::optional<std::vector<std::string>> tables;

const po::options_description OPTIONS = [] {
  po::options_description options{ "Allowed arguments" };
  options.add_options()("help,h", "print this help message");
  options.add_options()("version,v", "print version");
  options.add_options()("copy,c", "copy records from source to target");
  options.add_options()("sync,s", "sync records from source to target");
  options.add_options()("dry-run,d", "execute without modifying the target database");
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
  options.add_options()(
      "tables", po::value<>(&tables)->multitoken()->default_value(std::vector<std::string>(), ""), "tables to process");
  return options;
}();

po::variables_map params;

int main(int argc, char* argv[]) {
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
    std::cerr << "Only one command argument allowed [help|version|copy|sync]" << std::endl;
    return 2;
  }

  if(check == 0 || params.count("help")) {
    std::cout << OPTIONS << std::endl;
    return 0;
  }

  if(params.count("version")) {
    std::cout << dbsync::APP_NAME << ' ' << dbsync::APP_RELEASE << std::endl;
    return 0;
  }

  bool dryRun = params.count("dry-run") > 0;
  bool copy = params.count("copy") > 0;
  bool sync = params.count("sync") > 0;

  std::cout << std::format("Execute copy {} sync {} dry-run {}", copy, sync, dryRun) << std::endl;

  return 0;
}
