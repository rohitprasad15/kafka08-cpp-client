#ifndef _KAFKA_PRODUCER_TOPIC_INFO_H_
#define _KAFKA_PRODUCER_TOPIC_INFO_H_

#include <stdint.h>  // For exact width int types.
#include <vector>
#include <boost/asio.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include "broker_channel.h"

#define MAX_PARTITIONS 20
#define MAX_REPLICAS 5

namespace kafka {
namespace producer {

class NetworkBufferedReader;
class NetworkBufferedWriter;
class BrokerChannel;

typedef struct PartitionMetadata_
{
  int32_t partition_id;
  int8_t leader_exists;
  BrokerMetadata leader_replica;
  int16_t num_replicas;
  BrokerMetadata replica[MAX_REPLICAS];
  int16_t num_sync_replicas;
  BrokerMetadata sync_replica[MAX_REPLICAS];
} PartitionMetadata;

typedef struct TopicMetadata_
{
  std::string topic_name;
  int num_partitions;
  PartitionMetadata partitions[MAX_PARTITIONS];
} TopicMetadata;

class TopicInfo
{
 public:
  TopicInfo(std::string& host, int port);
  ~TopicInfo();
  int32_t GetBrokerId(const std::string &topic, int32_t partition);
  std::vector<BrokerMetadata>* GetUniqueBrokers();
  bool UpdateInfo(const std::string& host, int port, const std::string& topic);
  bool UpdateInfo(const std::string& topic);
  void PrintTopicInfo(const std::string& topic);
  void PrintBrokers();

 private:
  bool SendMetadataRequest(BrokerChannel &ch, const std::string& topic);
  bool GetMetadataResponse(const std::string& topic, BrokerChannel& ch,
                           TopicMetadata* topic_metadata);
  void MaybeAddNewBroker(BrokerMetadata& broker);
  bool ReadPartitionInfo(NetworkBufferedReader& instream,
                         PartitionMetadata* partition_info);
  bool ReadBrokerInfo(NetworkBufferedReader& instream, BrokerMetadata* broker_info);

  // @TODO: All Reads should be O(1). Convert to Hashmaps.
  boost::unordered_map<std::string,
      boost::unordered_map<int32_t, int32_t>* > topic_info_;
  boost::unordered_set<BrokerMetadata, BrokerMetadataHash, BrokerMetadataEq>
      unique_brokers_;
  std::string good_host_;
  int good_port_;
};

}
}
#endif
