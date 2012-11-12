#ifndef _KAFKA_PRODUCER_NETWORK_BUFFERED_WRITER_
#define _KAFKA_PRODUCER_NETWORK_BUFFERED_WRITER_

#include <string>
#include <boost/asio.hpp>

namespace kafka {
namespace producer {

class NetworkBufferedWriter {
 public:
  NetworkBufferedWriter (char* buf, const int size);
  ~NetworkBufferedWriter ();
  bool WriteInt8 (int8_t val);
  bool WriteInt16 (int16_t val);
  bool WriteInt32 (int32_t val);
  bool WriteShortString (const std::string &str);
  bool WriteLengthAtIndex(int position);
  bool WriteCharN (const char *temp, int size);
  void Reset();
  void GetRawBufferCopy(char* buf, int* size);
  char* GetRawBuffer();
  int GetRawBufferLength();
 private:
  const int buf_size_;
  char *buf_;
  int end_index_;
};

}  // namespace producer
}  // namespace kafka
#endif  // _KAFKA_PRODUCER_NETWORK_BUFFERED_WRITER_
