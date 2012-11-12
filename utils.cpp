#include <arpa/inet.h>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include "utils.h"


namespace kafka {
namespace utils {
  uint16_t ReadInt16FromBuffer(char *buf)
  {
    return ntohs(*((uint16_t *) buf));
  }

  uint32_t ReadInt32FromBuffer(char *buf)
  {
    return ntohl(*((uint32_t *) buf));
  }

  uint64_t ReadInt64FromBuffer(char *buf)
  {
    int32_t test = 1;
    uint64_t return_value;
    if (*((char *)&test) == 1) {
      // Machine ordering is Little Endian.
      char *tmp = ((char *)&return_value) + sizeof(return_value) - 1;
      *tmp = *buf;
      *--tmp = *++buf;
      *--tmp = *++buf;
      *--tmp = *++buf;
      *--tmp = *++buf;
      *--tmp = *++buf;
      *--tmp = *++buf;
      *--tmp = *++buf;
    } else {
      // Machine ordering is Big Endian.
      return_value = *((uint64_t *) buf);
    }
    return return_value;
  }


  bool WriteInt16ToBuffer(char *buf, uint16_t val)
  {
    uint16_t netw_val = htons(val);
    memcpy(buf, &netw_val, sizeof(netw_val));
  }

  bool WriteInt32ToBuffer(char *buf, uint32_t val)
  {
    uint32_t netw_val = htonl(val);
    memcpy(buf, &netw_val, sizeof(netw_val));
  }
}  // namespace utils
}  // namespace kafka
