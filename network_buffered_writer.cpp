#include <stdio.h>
#include <string>
#include <iostream>

#include "network_buffered_writer.h"
#include "utils.h"

using std::string;

namespace kafka {
namespace producer {

NetworkBufferedWriter::NetworkBufferedWriter (char* buffer,
                                              const int size)
    : buf_(buffer),
      buf_size_(size)
{
  Reset();
}

NetworkBufferedWriter::~NetworkBufferedWriter()
{
  if (buf_ != NULL) {
    delete [] buf_;
  }
}

void NetworkBufferedWriter::Reset()
{
  end_index_ = 0;
  memset(buf_, 0, buf_size_);
}

void NetworkBufferedWriter::GetRawBufferCopy(char *b, int *s)
{
  memcpy(b, buf_, end_index_);
  *s = end_index_;
}

char* NetworkBufferedWriter::GetRawBuffer()
{
  return buf_;
}

int NetworkBufferedWriter::GetRawBufferLength()
{
  return end_index_;
}

  bool NetworkBufferedWriter::WriteInt8(int8_t val)
  {
    if ((end_index_ + 1) >= buf_size_) {
      return false;
    }
    *(buf_ + end_index_) = val;
    end_index_++;
    return true;
  }

  bool NetworkBufferedWriter::WriteInt16(int16_t val)
  {
    if ((end_index_ + 2) >= buf_size_) {
      return false;
    }
    kafka::utils::WriteInt16ToBuffer(buf_ + end_index_, val);
    end_index_ += 2;
    return true;
  }

  bool NetworkBufferedWriter::WriteInt32(int32_t val)
  {
    if ((end_index_ + 4) >= buf_size_) {
      return false;
    }
    kafka::utils::WriteInt32ToBuffer(buf_ + end_index_, val);
    end_index_ += 4;
    return true;
  }

  bool NetworkBufferedWriter::WriteShortString(const string &str)
  {
    // There is an extra copy here. May we can avoid this.
    WriteInt16(str.size());
    if (str.size() == 0) {
      return true;
    }
    WriteCharN(str.c_str(), str.size());
    return true;
  }

  bool NetworkBufferedWriter::WriteCharN(const char *temp, int size)
  {
    if ((end_index_ + size) >= buf_size_) {
      return false;
    }
    memcpy(buf_ + end_index_, temp, size);
    end_index_ += size;
    return true;
  }

bool NetworkBufferedWriter::WriteLengthAtIndex(int position)
{
  // @TODO: -4 is a hack. Change when we have "WriteInt32AtIndex( )".
  kafka::utils::WriteInt32ToBuffer(buf_ + position, end_index_ - 4);
  return true;
}

}  // namespace producer
}  // namespace kafka
