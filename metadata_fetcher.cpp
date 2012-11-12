#include <iostream>
#include <stdlib.h>

#include "topic_info.h"

using namespace std;
using namespace kafka::producer;

int main(int argc, char *argv[])
{
  if (argc < 2) {
    cout << "Usage:" << endl << " ./" << argv[1]
         << " topic-name [host [port]]" << endl;
   return 1;
  }

  string topic(argv[1]);
  string host("dev-kafka-app1.ext.evbops.com");
  int port = 9092;
  if (argc == 3) {
    host.assign(argv[2]);
  }
  if (argc == 4) {
    port = atoi(argv[3]);
  }

  cout << "Using: " << host << ":" << port << endl;
  TopicInfo topic_info(host, port);
  if (!topic_info.UpdateInfo(topic)) {
    cout << "Error in getting metadata." << endl;
    return 1;
  }

  cout << "Printing topic info for Topic: " << topic << endl;
  topic_info.PrintTopicInfo(topic);
  cout << endl << "Printing all available brokers: " << endl;
  topic_info.PrintBrokers();

  cout << "Broker id for " << topic << ",0 is " << topic_info.GetBrokerId(topic, 0)
       << endl;
  return 0;
}
