#include <boost/cstdint.hpp>

namespace kafka {
namespace utils {
  uint16_t ReadInt16FromBuffer(char *buf);
  uint32_t ReadInt32FromBuffer(char *buf);
  uint64_t ReadInt64FromBuffer(char *buf);
  bool WriteInt16ToBuffer(char *buf, uint16_t val);
  bool WriteInt32ToBuffer(char *buf, uint32_t val);
}  // namespace utils
}  // namespace kafka
