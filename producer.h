#ifndef _KAFKA_PRODUCER_PRODUCER_H_
#define _KAFKA_PRODUCER_PRODUCER_H_

#include <stdio.h>
#include <vector>
#include "topic_info.h"
#include "network_buffered_writer.h"

namespace kafka {
namespace producer {

enum ErroMapping {
  UnknownCode = -1,
  NoError = 0,
  OffsetOutOfRangeCode = 1,
  InvalidMessageCode = 2,
  UnknownTopicOrPartitionCode = 3,
  InvalidFetchSizeCode = 4,
  LeaderNotAvailableCode = 5,
  NotLeaderForPartitionCode = 6,
  RequestTimedOutCode = 7,
  BrokerNotAvailableCode = 8,
  ReplicaNotAvailableCode = 9,
  MessageSizeTooLargeCode = 10
}; 


class BrokerChannel;
class BrokerPool;

class Producer
{
 public:
  Producer(TopicInfo& topic_info,
           const std::map<std::string, std::string>& config);
  ~Producer();
  bool InitBuffers(const int write_buffer_kb,
                   const int read_buffer_kb);
  // Public API. Currently async is not supported.
  bool Send(const std::string& topic_name, int partition_id,
            const std::vector<std::string>& msgs);
  bool Send(const std::string& topic_name, int partition_id,
            const std::vector<std::string>& msgs, bool async);

 private:
  // Gets the broker channel to send the message.
  bool HandleSend(const std::string& topic_name, int partition_id,
                  const std::vector<std::string>& msgs);
  // Sends the message to the Broker/replica.
  bool DoSend(BrokerChannel* channel, const std::string& topic_name,
              int partition_id, const std::vector<std::string>& msgs);
  // Helper function to calculate CRC32 of each message using boost.
  int32_t GetCrc32(const std::string& my_string);
  // Helper function to write Request header.
  void WriteProduceRequestHeader();
  // Check if server acked back correctly.
  bool CheckResponse(BrokerChannel* channel, const std::string& topic_name,
                     const int32_t partition_id);
  // Contains metadata mapping partitions to Brokers.
  TopicInfo &topic_info_;
  // Helps in forming produce request in Network Byte order.
  NetworkBufferedWriter* writer_;
  // Contains pointers to BrokerChannel.
  BrokerPool broker_pool_;
  char* recv_buf_;
  int recv_buf_size_;
  int max_retries_;
  int recv_timeout_;
  int send_timeout_;
  int16_t num_of_acks_;
};

}
}
#endif  // _PRODUCER_PRODUCER_H_
