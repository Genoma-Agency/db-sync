#include <utils.hxx>

namespace util {

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
