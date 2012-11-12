#ifndef _PRODUCER_TOPIC_INFO_H_
#define _PRODUCER_TOPIC_INFO_H_

#include <stdint.h>  // For exact width int types.
#include <iostream>

#include "topic_info.h"
#include "broker_channel.h"
#include "network_buffered_reader.h"
#include "network_buffered_writer.h"

#define ReturnIfFalse(x) \
  if (!(x)) return false;

using std::string;

namespace kafka {
namespace producer {

TopicInfo::TopicInfo(string& host, int port)
    : good_host_(host),
      good_port_(port)
{
}

TopicInfo::~TopicInfo()
{
}

int32_t TopicInfo::GetBrokerId(const string &topic, int32_t partition)
{
  // Returns -1 if no broker is present for topic/partition.
  int32_t broker_id = -1;
  boost::unordered_map<std::string,
      boost::unordered_map<int32_t, int32_t>* >::const_iterator it =
          topic_info_.find(topic);
  if (it == topic_info_.end()) {
    return -1;
  }
  boost::unordered_map<int32_t, int32_t>* partition_map = it->second;
  if (partition_map == NULL) {
    return -1;
  }
  boost::unordered_map<int32_t, int32_t>::const_iterator p_it =
      partition_map->find(partition);
  if (p_it == partition_map->end()) {
    return -1;
  }
  return p_it->second;
}

bool TopicInfo::UpdateInfo(const string& topic)
{
  return UpdateInfo(good_host_, good_port_, topic);
  // @TODO: check if the above has failed.
  //        If yes, then traverse topic_info_ to get a good endpoint.
}

bool TopicInfo::UpdateInfo(const string& host, int port, const string& topic)
{
  BrokerChannel ch(host, port, -1);
  ch.Connect();
  if (!SendMetadataRequest(ch, topic)) {
    // Log error.
    std::cerr << "Failed to get Topic metadata." << std::endl;
    return false;
  }

  TopicMetadata topic_metadata;
  if (!GetMetadataResponse(topic, ch, &topic_metadata)) {
    // Log error
    std::cerr << "Failed to get Topic metadata." << std::endl;
    return false;
  }

  // Traverse through the Metadata and update hashmaps.
  // Update for only that topic. (Later Update should be for per partition).

  boost::unordered_map<int32_t, int32_t>* partition_map =
      new boost::unordered_map<int32_t, int32_t>();
  for (int i = 0; i < topic_metadata.num_partitions; ++i) {
    int32_t partition_id = topic_metadata.partitions[i].partition_id;
    int32_t broker_id = topic_metadata.partitions[i].leader_replica.broker_id;
    (*partition_map)[partition_id] = broker_id;
    MaybeAddNewBroker(topic_metadata.partitions[i].leader_replica);
  }

  boost::unordered_map<std::string,
                       boost::unordered_map<int, int>* >::iterator it;
  it = topic_info_.find(topic);
  boost::unordered_map<int, int>* del_old_map = NULL;
  if (it != topic_info_.end()) {
    del_old_map = it->second;
  }
  // @TODO: Put a lock around this. 
  topic_info_[topic] = partition_map;
    // Should we update good_host_ and good_port_ ?
  // giveup lock

  if (del_old_map != NULL) {
    delete del_old_map;
  }
  return true;
}

void TopicInfo::MaybeAddNewBroker(BrokerMetadata& broker)
{
  unique_brokers_.insert(broker);  // Make a new copy for the set.
}

std::vector<BrokerMetadata>* TopicInfo::GetUniqueBrokers()
{
  // the vector should be deleted by the caller.
  std::vector<BrokerMetadata>* brokers = new std::vector<BrokerMetadata> ();
  for (boost::unordered_set<BrokerMetadata>::const_iterator it =
      unique_brokers_.begin(); it != unique_brokers_.end(); ++it) {
    brokers->push_back(*it);
  }
  return brokers;
}

void TopicInfo::PrintTopicInfo(const std::string& topic)
{
  boost::unordered_map<std::string,
      boost::unordered_map<int32_t, int32_t>* >::const_iterator it =
          topic_info_.find(topic);
  if (it == topic_info_.end()) {
    return;
  }
  boost::unordered_map<int32_t, int32_t>* partition_map = it->second;
  for (boost::unordered_map<int32_t, int32_t>::const_iterator pt =
      partition_map->begin(); pt != partition_map->end(); ++pt) {
    cout << "Parition: " << pt->first << ", Broker Id: " << pt->second
         << std::endl;
  }
}

void TopicInfo::PrintBrokers()
{
  for (boost::unordered_set<BrokerMetadata>::const_iterator it =
      unique_brokers_.begin(); it != unique_brokers_.end(); ++it) {
    std::cout << "Id: " << it->broker_id << ", Endpoint: "
              << it->host_name << ":" << it->port_num << std::endl;
  }
}

bool TopicInfo::SendMetadataRequest(BrokerChannel& ch,
                                    const string& topic)
{
  const int buf_size = 2048;
  char* buffer = new char[buf_size];
  if (buffer == NULL) {
    std::cerr << "Failed to allocate buffer for sending Metadata request"
              << std::endl;
    return false;
  }
  NetworkBufferedWriter outstream(buffer, buf_size);
  int32_t size = 0;  // Update later.
  outstream.WriteInt32(size);
  const int16_t metadata_request_id = 3;  // Metadata Request Key.
  outstream.WriteInt16(metadata_request_id);
  const int16_t version_id = 1;
  outstream.WriteInt16(version_id);
  string client_id("");
  //outstream.WriteShortString(client_id);
  outstream.WriteInt16(0);
  const int32_t topics_count = 1;
  outstream.WriteInt32(topics_count);
  outstream.WriteShortString(topic);
  outstream.WriteLengthAtIndex(0);
  return ch.SendDataBlocking(outstream.GetRawBuffer(),
                             outstream.GetRawBufferLength());
}

bool TopicInfo::GetMetadataResponse(const string& topic_name,
                                    BrokerChannel& channel,
                                    TopicMetadata* response_meta)
{
  const int buffer_size = 4096;
  // Keeping large buffer size incase topic has lots of partitions.
  char* buffer = new char[buffer_size];
  if (buffer == NULL) {
    std::cerr << "Failed to allocate memory for metadata response."
              << std::endl;
    return false;
  }
  NetworkBufferedReader instream(&channel, buffer, buffer_size);
  try {
    int32_t unused_response_length;
    instream.ReadInt32(&unused_response_length);
    int16_t unused_version_id;
    instream.ReadInt16(&unused_version_id);
    int32_t topics_count;
    instream.ReadInt32(&topics_count);
    TopicMetadata ignored_meta;
    TopicMetadata *meta_ptr;
    for (int i = 0; i < topics_count; ++i) {
      // Topic Info
      int16_t unused_error_code;
      instream.ReadInt16(&unused_error_code);
      string recv_topic_name;
      instream.ReadShortString(recv_topic_name);
      if (topic_name.compare(recv_topic_name) != 0) {
        meta_ptr = &ignored_meta;
      } else {
        meta_ptr = response_meta;
      }

      instream.ReadInt32(&meta_ptr->num_partitions);
      if (meta_ptr->num_partitions > MAX_PARTITIONS) {
        // @TODO: Log error. Handle more gracefully.
        return false;
     }
      memset(&(meta_ptr->partitions), 0, sizeof(PartitionMetadata) * MAX_PARTITIONS);
      for (int j = 0; j < meta_ptr->num_partitions; ++j) {
        ReturnIfFalse(ReadPartitionInfo(instream, &(meta_ptr->partitions[j])));
      }
    }
  }
  catch (std::exception e)
  {
    std::cerr << "Error in reading metadata response: " << e.what() << std::endl;
    return false;
  }
  return true;
}

bool TopicInfo::ReadPartitionInfo(NetworkBufferedReader& instream,
                                  PartitionMetadata* partition_info)
{
  memset(partition_info, 0, sizeof(PartitionMetadata));
  int16_t unused_error_code;
  instream.ReadInt16(&unused_error_code);
  instream.ReadInt32(&partition_info->partition_id);
  instream.ReadInt8(&partition_info->leader_exists);
  if (partition_info->leader_exists) {
    ReturnIfFalse(ReadBrokerInfo(instream,
                                 &partition_info->leader_replica));
  }
  instream.ReadInt16(&partition_info->num_replicas);
  for (int j = 0; j < partition_info->num_replicas; ++j) {
    ReturnIfFalse(ReadBrokerInfo(instream, 
                                 &partition_info->replica[j]));
  }
  instream.ReadInt16(&partition_info->num_sync_replicas);
  for ( int k = 0; k < partition_info->num_sync_replicas; ++k) {
    ReturnIfFalse(ReadBrokerInfo(instream, 
                                 &partition_info->sync_replica[k]));
  }
  return true;
}

bool TopicInfo::ReadBrokerInfo(NetworkBufferedReader& instream,
                               BrokerMetadata* broker_info)
{
  instream.ReadInt32(&broker_info->broker_id);
  string unused_creator_id;
  instream.ReadShortString(unused_creator_id);
  string host_name;
  instream.ReadShortString(host_name);
  strcpy(broker_info->host_name, host_name.c_str());
  instream.ReadInt32(&broker_info->port_num);
  return true;
}

}
}
#endif
