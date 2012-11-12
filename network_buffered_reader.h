#ifndef _KAFKA_PRODUCER_NETWORK_BUFFERED_READER_
#define _KAFKA_PRODUCER_NETWORK_BUFFERED_READER_ 1

#include <string>
#include <boost/asio.hpp>


using namespace std;

namespace kafka {
namespace producer {

class BrokerChannel;

class NetworkBufferedReader {
 public:
  NetworkBufferedReader (BrokerChannel *, char* buffer,
                         const int size);
  ~NetworkBufferedReader ();
  bool ReadInt8 (int8_t *val);
  bool ReadInt16 (int16_t *val);
  bool ReadInt32 (int32_t *val);
  bool ReadInt64 (int64_t *val);
  bool ReadShortString (string &str);
  bool ReadCharN (char **temp, int size);

 private:
  bool FetchIfNeeded (int min_size);
  int ReadNOrBlock (char *buf, int min_size, int max_size);

  const int buf_size_;
  char *buf_;
  int start_index_;
  int end_index_;
  BrokerChannel* channel_;
};

}  // namespace producer
}  // namespace kafka
#endif  // _KAFKA_PRODUCER_NETWORK_BUFFERED_READER_
