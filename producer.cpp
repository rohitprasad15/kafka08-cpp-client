#include <assert.h>
#include <iostream>
#include <string>
#include <stdlib.h>
#include <boost/crc.hpp>

#include "producer.h"
#include "broker_channel.h"
#include "topic_info.h"
#include "network_buffered_reader.h"

#define MAX_PRODUCER_RETRIES  3
#define RETRIES_BEFORE_UPDATE 3
#define NUM_OF_REPLICA_ACKS   2
#define PRODUCER_SEND_TIMEOUT 60
#define PRODUCER_RECV_TIMEOUT 120

using std::vector;
using std::string;
using std::cout;
using std::map;
using std::endl;

namespace kafka {
namespace producer {

  Producer::Producer(TopicInfo& topic_info,
                     const map<string, string>& config)
      : topic_info_(topic_info),
        max_retries_(MAX_PRODUCER_RETRIES),
        send_timeout_(PRODUCER_SEND_TIMEOUT),
        recv_timeout_(PRODUCER_RECV_TIMEOUT),
        num_of_acks_(NUM_OF_REPLICA_ACKS)
  {
    // Initialize BrokerPool. Socket connections are NOT setup in this call!
    broker_pool_.UpdatePool(topic_info_);
    map<string, string>::const_iterator it;
    if ((it = config.find(string("ProducerRetries"))) !=
        config.end()) {
      max_retries_ = atoi((it->second).c_str());
    }

    if ((it = config.find("NumOfAcks")) != config.end()) {
      num_of_acks_ = atoi((it->second).c_str());
    }

    if ((it = config.find("SocketSendTimeout")) != config.end()) {
      send_timeout_ = atoi((it->second).c_str());
    }

    if ((it = config.find("SocketRecvTimeout")) != config.end()) {
      recv_timeout_ = atoi((it->second).c_str());
    }
  }

  Producer::~Producer()
  {
    // Close any open sockets. (No attempt to flush data in sockets.)
    broker_pool_.ClearPool();
  }

  bool Producer::InitBuffers(const int write_buffer_kb,
                             const int read_buffer_kb)
  {
    char* send_buf = new char[write_buffer_kb * 1024];
    if (send_buf == NULL) {
      return false;
    }
    recv_buf_ = new char[read_buffer_kb * 1024];
    if (recv_buf_ == NULL) {
      return false;
    }
    writer_ = new NetworkBufferedWriter(send_buf, write_buffer_kb * 1024);
    if (writer_ == NULL) {
      delete [] recv_buf_;
      delete [] send_buf;
      return false;
    }
    recv_buf_size_ = read_buffer_kb * 1024;
  }

  bool Producer::Send(const std::string& topic_name, int partition_id,
            const std::vector<std::string>& msgs)
  {
    return Send(topic_name, partition_id, msgs, false);
  }

  bool Producer::Send(const std::string& topic_name, int partition_id,
            const std::vector<std::string>& msgs, bool async)
  {
    // Async is not supported currently so we will ignore it.
    return HandleSend(topic_name, partition_id, msgs);
  }

  bool Producer::HandleSend(const string& topic_name, int partition_id,
                            const vector<string> &msgs)
  {
    int32_t broker_id = topic_info_.GetBrokerId(topic_name, partition_id);
    if (broker_id == -1) return false;

    BrokerChannel *channel = broker_pool_.GetBrokerChannel(broker_id);
    int remaining_tries = max_retries_;
    do {
      if (DoSend(channel, topic_name, partition_id, msgs)) {
        if (CheckResponse(channel, topic_name, partition_id)) {
          return true;
        }
      } else {
        std::cerr << "Error in sending data." << std::endl;
        break;
      }
    } while(--remaining_tries);

    // This could ne potential used by multiple threads and hence could
    // block if waiting for a lock.
    if (!topic_info_.UpdateInfo(topic_name)) {
      std::cerr << "Failed to update topic info. " << std::endl;
      return false;
    }
    // Update Broker pool.
    broker_pool_.UpdatePool(topic_info_);  // COPY of topic_info_ is passed.
    remaining_tries = max_retries_;
    broker_id = topic_info_.GetBrokerId(topic_name, partition_id);
    if (broker_id == -1) return false;
    channel = broker_pool_.GetBrokerChannel(broker_id);

    do {
      if (DoSend(channel, topic_name, partition_id, msgs)) {
        if (CheckResponse(channel, topic_name, partition_id)) {
          return true;
        }
      }
    } while(--remaining_tries);

    return false;
  }

  bool Producer::DoSend(BrokerChannel *channel, const string& topic_name,
                        int partition_id, const vector<string>& msgs)
  {
    assert(writer_);
    writer_->Reset();
    WriteProduceRequestHeader();

    uint32_t topics_count = 1;
    writer_->WriteInt32(topics_count);

    for (int i = 0; i < topics_count; ++i) {
      //string topic1("tempdel1");
      //string topic1("cltest2p2r");
      writer_->WriteShortString(topic_name);

      uint32_t partition_count = 1;
      writer_->WriteInt32(partition_count);

      // @TODO: delete this message variable later.
      const char *message = msgs.front().c_str();
      int msgs_total_size = 0;

      for(vector<string>::const_iterator it = msgs.begin();
          it != msgs.end(); ++it) {
        msgs_total_size += (*it).size() + 6 + 4;
      }


      for (int k = 0; k < partition_count; ++k) {
        uint32_t partition_id = 0;
        writer_->WriteInt32(partition_id);
        //uint32_t message_size = strlen(message) + 6;  // 6 is the magic.
        writer_->WriteInt32(msgs_total_size);
        for (vector<string>::const_iterator it2 = msgs.begin();
            it2 != msgs.end(); ++it2) {
          //char data[100] = message;
          int msg_size = (*it2).size() + 6;
          writer_->WriteInt32(msg_size);
          writer_->WriteInt8(1);
          writer_->WriteInt8(0);
          //std::string temp(*it2);
          int32_t crc32 = GetCrc32(*it2);
          // writer_->WriteInt32(0xb61f1169);
          writer_->WriteInt32(crc32);
          // cout << "wrote CRC" << endl;
          writer_->WriteCharN((*it2).c_str(), strlen((*it2).c_str()));
        }
      }
    }
    writer_->WriteLengthAtIndex(0);
    return channel->SendDataBlocking(writer_->GetRawBuffer(),
                                     writer_->GetRawBufferLength());
  }

  void Producer::WriteProduceRequestHeader()
  {
    writer_->WriteInt32(0);  // This will updated later.
    uint16_t produce_request_id = 0;
    writer_->WriteInt16(produce_request_id);

    uint16_t version_id = 0;
    writer_->WriteInt16(version_id);

    uint32_t correlation_id = 0xffffffff;
    writer_->WriteInt32(correlation_id);

    string client_id("");
    // writer_->WriteShortString(client_id);
    writer_->WriteInt16(0);

    // required acks.
    writer_->WriteInt16(num_of_acks_);
    // ack timeout.
    writer_->WriteInt32(0x1f4);

  }

  int32_t Producer::GetCrc32(const string& my_string) {
    boost::crc_32_type result;
    result.process_bytes(my_string.data(), my_string.length());
    return result.checksum();
  }

  bool Producer::CheckResponse(BrokerChannel* channel,
                               const string& topic_name,
                               const int32_t partition_id)
  {
    NetworkBufferedReader instream(channel, recv_buf_, recv_buf_size_);
    try {
      int32_t unused_length;
      instream.ReadInt32(&unused_length);
      int16_t unused_version_id;
      instream.ReadInt16(&unused_version_id);
      int32_t unused_correlation_id;
      instream.ReadInt32(&unused_correlation_id);
      int32_t topic_count;
      instream.ReadInt32(&topic_count);
      for (int i = 0; i < topic_count; ++i) {
        string recv_topic_name;
        instream.ReadShortString(recv_topic_name);
        int32_t partition_count;
        instream.ReadInt32(&partition_count);
        for (int j = 0; j < partition_count; ++j) {
          int32_t recv_partition_id;
          instream.ReadInt32(&recv_partition_id);
          int16_t error_code;
          instream.ReadInt16(&error_code);
          int64_t offset;
          instream.ReadInt64(&offset);
          if (!topic_name.compare(recv_topic_name) &&
              recv_partition_id == partition_id &&
              error_code == NoError) {
            return true;
          }

        }
      }
    }
    catch (std::exception ex) {
      std::cerr << "Exception in reading Producer response from Server."
                << std::endl;
      return false;
    }
    return false;
  }
}  // namespace producer
}  // namespace kafka
