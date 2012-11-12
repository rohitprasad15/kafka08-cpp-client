/* network_buffered_reader.cpp
 * Author: Rohit C Prasad
 * Date: Sep 30th 2012
 */

#include <string>
#include <iostream>

#include "network_buffered_reader.h"
#include "utils.h"
#include "broker_channel.h"


namespace kafka {
namespace producer {

  NetworkBufferedReader::NetworkBufferedReader (BrokerChannel* channel,
                                                char* buffer,
                                                const int size):
      channel_(channel),
      buf_(buffer),
      buf_size_(size),
      start_index_(0),
      end_index_(0)
  {
  }

  NetworkBufferedReader::~NetworkBufferedReader()
  {
  }

  bool NetworkBufferedReader::ReadInt8(int8_t *val)
  {
    FetchIfNeeded(1); // throw exception if network error.
    *val =  *(buf_ + start_index_);
    start_index_ += 1;
  } 

  bool NetworkBufferedReader::ReadInt16(int16_t *val)
  {
    FetchIfNeeded(2); // throw exception if network error.
    *val =  kafka::utils::ReadInt16FromBuffer(buf_ + start_index_);
    start_index_ += 2;
  }

  bool NetworkBufferedReader::ReadInt32(int32_t *val)
  {
    FetchIfNeeded(4);
    *val = kafka::utils::ReadInt32FromBuffer(buf_ + start_index_);
    start_index_ += 4;

    return true;
  }

  bool NetworkBufferedReader::ReadInt64(int64_t *val)
  {
    FetchIfNeeded(8);
    *val = kafka::utils::ReadInt64FromBuffer(buf_ + start_index_);
    start_index_ += 8;

    return true;
  }

  bool NetworkBufferedReader::ReadShortString(string &str)
  {
    // There is an extra copy here. May we can avoid this.
    int16_t length = 0;
    ReadInt16(&length);
    if (length == 0) {
      str.clear(); // may be equal to "".?
      return true;
    }
    char *tmp = NULL;
    ReadCharN(&tmp, length);
    str.assign(tmp, length);
    return true;
  }

  bool NetworkBufferedReader::ReadCharN(char **temp, int size)
  {
    FetchIfNeeded(size);
    *temp = buf_ + start_index_;
    start_index_ += size;
    return true;
  }

  bool NetworkBufferedReader::FetchIfNeeded(int min_size)
  {
    int useful_bytes = end_index_ - start_index_;
    if (useful_bytes >= min_size) {
      return false;
    }

    memcpy(buf_, buf_ + start_index_, useful_bytes);
    int bytes_read = ReadNOrBlock(buf_ + useful_bytes, min_size,
                                  buf_size_ - useful_bytes);

    start_index_ = 0;
    end_index_ = bytes_read + useful_bytes;
    return true;
  }

  int NetworkBufferedReader::ReadNOrBlock(char *buf, int min_size,
                                           int max_size)
  {
    // Block till N bytes are read.
    //
    // Reads a maximum of max_size. If an error is encountered before n bytes
    // are read, then the contents may be lost.
    if (max_size < min_size)
      return 0; // @TODO: correct this.

    int total_read = 0;
    boost::system::error_code error;
    while (min_size > total_read) {
      int len;
      if (!channel_->RecvDataBlocking(max_size, buf, &len)) {
        std::cerr << "error in reading from socket." << endl;
        return 0;
      }
      if (0 == len) {
        std::cerr << "error in reading from socket." << endl;
        return 0;
      }
      total_read += len;
      max_size -= len;
    }
    return total_read;
  }

}  // namespace producer
}  // namespace kafka
