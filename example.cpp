/* A simple console producer for Kafka 0.8
 * Author: Rohit Prasad
 * Date: 10th October 2012
 */

// Psuedo-code
// 1. Read topic and initial broker list from cmdline.
// 2. Read data from console or from file.
// 3. Write data.
// 4. If write fails - Backoff, print error, update stats, Retry.
// 5. Print stats on exit, and on signal handler.

#include <getopt.h>
#include <iostream>
#include <map>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <boost/program_options.hpp>

#include "topic_info.h"
#include "producer.h"

#define MSG_MULTIPLIER 1
#define ERROR_TOLERANCE_PERCENT 2

using namespace std;
using namespace kafka::producer;
namespace boost_po = boost::program_options;

// Can be used by all threads.
TopicInfo* topic_info = NULL;

void MakeOptionsDescription(boost_po::options_description *desc)
{
  desc->add_options()
      ("help", "Display this help message")
      ("broker_host", boost_po::value<string>(), "Broker IP")
      ("broker_port", boost_po::value<int>(), "Broker port")
      ("topic_name", boost_po::value<string>(), "Topic name")
      ("partition_id", boost_po::value<int32_t>(),
       "Partition Id")
      ("num_messages", boost_po::value<int>()->default_value(10),
       "Number of messages to produce. Timing is measured.")
      ("batch_size", boost_po::value<int>()->default_value(2),
       "Batch size to use")
      ("num_acks", boost_po::value<int>()->default_value(1),
       "Number of servers that should ack")
      ("producer_retry_count", boost_po::value<int>()->default_value(3),
       "Number of failed attempts a producer will make to a single broker "
       "before returning failure.")
      ("socket_send_timeout", boost_po::value<int>()->default_value(2000),
       "Socket timeout for send calls in ms")
      ("socket_recv_timeout", boost_po::value<int>()->default_value(3000),
       "Socket timeout for recv calls in ms")
      ("warmup_msgs", boost_po::value<int>()->default_value(0),
       "Number of messages to send in warm-up and cool-down phase.");
}

bool IsErrorTolerable(int64_t max_messages, int64_t error_count)
{
  if (error_count <= (max_messages * (ERROR_TOLERANCE_PERCENT / 100.0))) {
    return true;
  }
  return false;
}

int main(int argc, char *argv[])
{
  boost_po::options_description desc("Options for configuring client.");
  boost_po::variables_map vm;
  try
  {
    MakeOptionsDescription(&desc);
    boost_po::store(boost_po::parse_command_line(argc, argv, desc), vm);
    if (vm.count("help")) {
      cout << desc << endl;
      return 1;
    }
    boost_po::notify(vm);
  }
  catch(std::exception& e)
  {
    cerr << "Error: " << e.what() << endl;
    return 1;
  }
  catch(...)
  {
    cerr << "Unknown error!" << endl;
    return 1;
  }

  map<string, string> options;
  options["ProducerRetries"] =
      boost::lexical_cast<string>(vm["producer_retry_count"].as<int>());
  options["NumOfAcks"] =
      boost::lexical_cast<string>(vm["num_acks"].as<int>());

  vector<string> dataset;
  string some_bytes = "Need to write something. Need to write something. "
                      "Need to write something. Need to write something. "
                      "Need to write something. Need to write something. "
                      "Need to write something. Need to write something. "
                      "Need to write something. Need to write something. "
                      "Need to write something. Need to write something. "
                      "Need to write something. Need to write something. "
                      "Need to write something. Need to write something. "
                      "Need to write something. Need to write something. "
                      "Need to write something. Need to write something. "
                      "Need to write something. Need to write something. "
                      "Need to write something. Need to write something. ";

  int batch_count = vm["batch_size"].as<int>();
  for (int a = 0; a < batch_count; ++a) {
    dataset.push_back(some_bytes);
  }  

  // Create an object for TopicInfo which will be shared across all threads.
  string hostIP(vm["broker_host"].as<string>());
  int hostport(vm["broker_port"].as<int>());
  topic_info = new TopicInfo(hostIP, hostport);
  string topic_name(vm["topic_name"].as<string>());
  if (!topic_info->UpdateInfo(topic_name)) {
    cout << "Updating topic info failed. Exiting." << endl;
    exit(1);
  }

  cout << "Printing Topic metadata." << endl;
  topic_info->PrintTopicInfo(topic_name); // Remove next 3
  cout << endl << "Printing brokers." << endl;
  topic_info->PrintBrokers();

  Producer producer(*topic_info, options);
  producer.InitBuffers(1024*500, 1024*50);

  int32_t partition_id = vm["partition_id"].as<int32_t>();
  int64_t max_messages = vm["num_messages"].as<int>() * MSG_MULTIPLIER;
  cout << "Sending " << max_messages << " to Topic: " << topic_name
       << ", partition: " << partition_id << endl;

  int i = 0;
  int warm_up_msgs = vm["warmup_msgs"].as<int>();
  int warm_up_errors = 0;

  // Warm up.
  for (i = 0; i < warm_up_msgs; ++i) {
    if (!producer.Send(topic_name, partition_id, dataset)) {
      ++warm_up_errors;
    }
  }

  // Start time.
  struct timeval start_time;
  int ret_code;
  ret_code = gettimeofday(&start_time, NULL);

  int64_t sent;
  int error_count = 0;
  for (sent = 0; sent < max_messages; ++sent) {
    if (!producer.Send(topic_name, partition_id, dataset)) {
      ++error_count;
    }
    if ((sent % 50) == 0) {
      cout << "done with: " << sent << endl;
    }
    if (!IsErrorTolerable(max_messages, error_count)) {
      cout << "Too many errors. Terminating loop." << endl;
      break;
    }
  }

  // Stop time.
  struct timeval end_time;
  ret_code = gettimeofday(&end_time, NULL);

  cout << "Sent " << sent << " with " << error_count << "errors"
       << endl;
  cout << "sec: " << end_time.tv_sec - start_time.tv_sec << " ,usec: "
       << end_time.tv_usec - start_time.tv_usec << endl;

  // Cool down. Keep up the load while other processes finish their tasks.
  for (i = 0; i < warm_up_msgs; ++i) {
    if (!producer.Send(topic_name, partition_id, dataset)) {
      ++warm_up_errors;
    }
  }
  cout << "Number of msgs sent as Warm-up and Cool-down each: "
       << warm_up_msgs << ", Combined errors: " << warm_up_errors << endl;

  // Wait for user to quit.
  string temp;
  cin >> temp;
}
