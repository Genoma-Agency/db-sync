#pragma once

#include <chrono>
#include <string>

namespace util {

namespace term {
/*****************************************************************************/
/* xterm escape sequences                                                    */
/* https://gist.github.com/fnky/458719343aabd01cfb17a3a4f7296797             */
/*****************************************************************************/

namespace sequence {
extern const std::string esc;
extern const std::string eraseLine;
extern const std::string eraseRight;
extern const std::string eraseLeft;
}

namespace stream {
std::ostream& eraseLine(std::ostream& stream);
std::ostream& eraseRight(std::ostream& stream);
std::ostream& eraseLeft(std::ostream& stream);
}

}

namespace timer {
/*****************************************************************************/
/* timer for processing operation, calculates eta and speed                  */
/*****************************************************************************/

using namespace std::chrono_literals;

using clock = std::chrono::high_resolution_clock;
using time_point = std::chrono::time_point<clock>;

template <typename T>
concept IsDuration
    = std::is_same<T, std::chrono::nanoseconds>::value || std::is_same<T, std::chrono::microseconds>::value
      || std::is_same<T, std::chrono::milliseconds>::value || std::is_same<T, std::chrono::seconds>::value
      || std::is_same<T, std::chrono::minutes>::value || std::is_same<T, std::chrono::hours>::value;

template <typename D1, typename D2>
concept IsDurationGreaterEqual
    = IsDuration<D1> && IsDuration<D2> && std::ratio_greater_equal<typename D1::period, typename D2::period>::value;

/*****************************************************************************/

template <IsDuration D = std::chrono::milliseconds> class Duration {
public:
  using base = std::chrono::nanoseconds;
  Duration(D&& duration) noexcept
      : _duration{ std::move(duration) } {}
  const bool isZero() const { return _duration.count() == 0; };
  const D& duration() const { return _duration; }
  const std::string& string() {
    static const std::pair<std::intmax_t, std::string> conv[6] = {
      {std::chrono::duration_cast<base>(1h).count(),    "h"                 },
      { std::chrono::duration_cast<base>(1min).count(), "m"                 },
      { std::chrono::duration_cast<base>(1s).count(),   "s"                 },
      { std::chrono::duration_cast<base>(1ms).count(),  "ms"                },
      { std::chrono::duration_cast<base>(1us).count(),  (const char*)u8"μs"},
      { 1ns .count(),                                   "ns"                }
    };
    if(!_string.empty())
      return _string;
    std::stringstream str;
    auto limit = std::chrono::duration_cast<std::chrono::nanoseconds>(D{ 1 }).count();
    std::intmax_t integer;
    std::intmax_t fraction = std::chrono::duration_cast<std::chrono::nanoseconds>(_duration).count();
    int last;
    for(int i = 0, count = 0; i < 6 && conv[i].first >= limit && count < 3; last = i++) {
      integer = fraction / conv[i].first;
      fraction = fraction % conv[i].first;
      if(integer > 0 || str.tellp() > 0) {
        str << (str.tellp() > 0 ? " " : "") << integer << conv[i].second;
        count++;
      }
    }
    if(str.tellp() == 0)
      str << "less than 1 " << conv[last].second;
    _string = str.str();
    return _string;
  }

private:
  const D _duration;
  std::string _string;
};
/*****************************************************************************/

template <IsDuration D = std::chrono::milliseconds> class ProcessingTimes {
public:
  ProcessingTimes(std::uint64_t c, D&& elapsed, D&& total) noexcept
      : count{ c },
        _elapsed{ std::move(elapsed) },
        _total{ std::move(total) },
        _missing{ _total.duration().count() > 0 ? _total.duration() - _elapsed.duration() : D{ 0 } } {}
  Duration<D>& elapsed() { return _elapsed; };
  Duration<D>& total() { return _total; };
  Duration<D>& missing() { return _missing; };
  template <IsDuration U = std::chrono::seconds>
    requires IsDurationGreaterEqual<U, D>
  double speed() const {
    if(_elapsed.isZero())
      return 0;
    double speed = count;
    speed *= std::chrono::duration_cast<D>(U{ 1 }).count();
    speed /= _elapsed.duration().count();
    return speed;
  }

private:
  const std::uint64_t count;
  Duration<D> _elapsed;
  Duration<D> _total;
  Duration<D> _missing;
};

/*****************************************************************************/

template <IsDuration D = std::chrono::milliseconds> class Timer {
public:
  using elapsed_t = ProcessingTimes<D>;

  Timer() noexcept { reset(); }

  void reset(std::uint64_t expected = 0) {
    _begin = clock::now();
    _expected = expected;
    _processed = 0;
  }

  void expected(std::uint64_t expected) { _expected = expected; }

  ProcessingTimes<D> elapsed(std::uint64_t processed = 0) {
    _processed += processed;
    D elapsed = std::chrono::duration_cast<D>(clock::now() - _begin);
    std::uint64_t total = 0;
    if(processed > 0 && _expected > 0)
      total = (double)_expected / (double)processed * elapsed.count();
    return ProcessingTimes{ _processed, std::move(elapsed), std::move(D{ total }) };
  }

private:
  time_point _begin;
  std::uint64_t _expected;
  std::uint64_t _processed;
};

}
}
