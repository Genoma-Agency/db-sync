#include <cassert>
#include <fmt/core.h>
#include <fstream>
#include <unistd.h>
#include <utils.hxx>

namespace util {

namespace proc {

mem_info memoryInfo() {
  std::ifstream file("/proc/self/statm");
  assert(file.is_open());
  std::size_t vss, rss, shared, text, lib, data, dirty;
  file >> vss >> rss >> shared >> text >> lib >> data >> dirty;
  auto ps = getpagesize();
  return mem_info{ .vss = vss * ps,
                   .rss = rss * ps,
                   .shared = shared * ps,
                   .text = text * ps,
                   .lib = lib * ps,
                   .data = data * ps,
                   .dirty = dirty * ps };
}

std::string memoryString(std::size_t kb) {
  if(kb < 1024)
    return fmt::format("{} Kb", kb);
  double tmp = kb;
  tmp /= 1024;
  if(tmp < 1024)
    return fmt::format("{:.2f} Mb", tmp);
  tmp /= 1024;
  return fmt::format("{:.2f} Gb", tmp);
}

std::size_t memoryUsageKb() { return memoryInfo().rss / 1_Kb; }
double memoryUsageMb() { return (double)memoryInfo().rss / 1_Mb; }
double memoryUsageGb() { return (double)memoryInfo().rss / 1_Gb; }
std::string memoryUsage() { return memoryString(memoryUsageKb()); }

std::size_t maxMemoryUsageKb() {
  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);
  return usage.ru_maxrss;
}
double maxMemoryUsageMb() { return (double)maxMemoryUsageKb() / 1_Kb; }
double maxMemoryUsageGb() { return (double)maxMemoryUsageKb() / 1_Mb; }
std::string maxMemoryUsage() { return memoryString(maxMemoryUsageKb()); }
}

namespace term {

namespace sequence {
const std::string eraseLine{ "\033[2K" };
const std::string eraseRight{ "\033[0K" };
const std::string eraseLeft{ "\033[1K" };
}

namespace stream {
std::ostream& eraseLine(std::ostream& stream) { return stream << util::term::sequence::eraseLine; }
std::ostream& eraseRight(std::ostream& stream) { return stream << util::term::sequence::eraseRight; }
std::ostream& eraseLeft(std::ostream& stream) { return stream << util::term::sequence::eraseLeft; }
}

}
}
