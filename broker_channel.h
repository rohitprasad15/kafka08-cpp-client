#ifndef _KAFKA_PRODUCER_BROKER_CHANNEL_H_
#define _KAFKA_PRODUCER_BROKER_CHANNEL_H_

#include <boost/asio.hpp>
#include <boost/unordered_map.hpp>
#include <map>
#include <stdint.h>  // For exact width int types.
#include <vector>

//#include "topic_info.h"  // Maybe we can remove this, due to forward declaration.

namespace kafka {
namespace producer {

class TopicInfo;  // Forward declaration.

typedef enum BrokerState_
{
  RESET,  // Initial state. Need to do socket connect.
  CONNECTED,  // Normal socket connection.
  SOCKET_ERROR,  // Socket error. Should either Reconnect or update metadata.
  WRITE_FAILED,  // For async, last Write call did not complete.
  READ_FAILED,  // For async, last Read call did not complete.
} BrokerState;

typedef struct
{
  int32_t broker_id;
  char host_name[50];
  int32_t port_num;
} BrokerMetadata;

typedef struct
{
  long operator() (const BrokerMetadata& bm) const
  {
    return (bm.broker_id % 100);
  }
} BrokerMetadataHash;

typedef struct
{
  bool operator() (const BrokerMetadata& m1, const BrokerMetadata& m2) const
  {
    return (m1.broker_id == m2.broker_id);
  }
} BrokerMetadataEq;

  // Represents a broker with a particular broker_id.
  // broker_id can never change for a particular object.
  // host and port can change for 
  class BrokerChannel
  {
   public:
    BrokerChannel(std::string host, int port, int32_t broker_id);
    ~BrokerChannel();
    // Change the Endpoint for a broker. (Can be common in EC2 env).
    void SetEndpoint(const std::string& host, const int port);
    // Get the hostname and port number associated with this Broker.
    void GetEndpoint(std::string* host, int* port);
    // Called from Send( ), Can also be called implicitly, so that Send*()
    // calls do not get performance hit in first call.
    bool Connect();
    // This can be called when this broker is no longer valid for a partition.
    void Disconnect();
    // Send all data or block. Return false on socket errror.
    bool SendDataBlocking(const char *buffer, const int size);
    // Receive a max of 'size' data. Return false on socket error.
    bool RecvDataBlocking(const int max_size, char *buffer, int *recv_size);
    // Check if endpoint in metadata is same as current channel's
    bool IsSameEndpoint(const BrokerMetadata& meta);

   private:
    std::string hostname_;
    int port_;
    int32_t broker_id_;
    BrokerState state_;
    // @TODO: Accept io_service through ctor, so that
    //        there is just 1 io_service in a process.
    boost::asio::io_service io_service_;
    boost::asio::ip::tcp::socket socket_;
  };


class BrokerPool
{
 public:
  BrokerPool();
  ~BrokerPool();
  void UpdatePool(TopicInfo& topic_info);
  BrokerChannel* GetBrokerChannel(const int32_t broker_id);
  void AddBrokerChannel(const int32_t broker_id, BrokerChannel* ch);
  void RemoveBrokerChannel(const int32_t broker_id);
  void ClearPool();

 private:
  boost::unordered_map<int32_t, BrokerChannel*> channel_map_;
};

}
}
#endif
