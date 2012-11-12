#include <boost/asio.hpp>
#include <stdint.h>  // For exact width int types.
#include <iostream>

#include "broker_channel.h"
#include "topic_info.h"

using std::string;

namespace kafka {
namespace producer {

BrokerChannel::BrokerChannel(const string host, const int port,
                             const int32_t broker_id)
    : socket_(io_service_),
      hostname_(host),
      port_(port),
      state_(RESET)
{
}

BrokerChannel::~BrokerChannel()
{
  // socket_ destructor calls close() automatically.
}

void BrokerChannel::SetEndpoint(const string& host, const int port)
{
  hostname_ = host;
  port_ = port;
}

void BrokerChannel::GetEndpoint(string* host, int* port)
{
  *host = hostname_;
  *port = port_;
}

  bool BrokerChannel::Connect()
  {
    if (state_ == CONNECTED) Disconnect();
    try {
      boost::asio::ip::tcp::endpoint endpoint(
          boost::asio::ip::address::from_string(hostname_), port_);
      socket_.connect(endpoint);
    }
    catch(std::exception& e) {
      std::cerr << e.what() << std::endl;
      state_ = SOCKET_ERROR;
      return false;
    }
    state_ = CONNECTED;
    return true;
  }

  void BrokerChannel::Disconnect()
  {
    socket_.cancel();
    socket_.close();
    state_ = RESET;
  }

  bool BrokerChannel::SendDataBlocking(const char *buffer, int size)
  {
    if (state_ == SOCKET_ERROR) {
      // Return false if there is socket error. Caller must decide
      // whether to reconnect or use a new endpoint for this broker.
      // Currently False is returned only when there is socket error,
      // so caller can easily make out that socket threw an error.
      return false;
    }
    if (state_ == RESET) {
      if (!Connect()) return false;
    }

    int len = 0;
    try {
      // Boost will block till everything is transferred.
      // @TODO: Need to give a timeout for write.
      //        If timeout is encountered, we set socket error, and disconnect.
      len = boost::asio::write(socket_, boost::asio::buffer((void *) buffer, size),
                               boost::asio::transfer_all());
    }
    catch(std::exception& e) {
      std::cerr << "Exception in send: " << e.what() << std::endl;
      state_ = SOCKET_ERROR;
      Disconnect(); // this causes state_ to be RESET.
      return false;
    }
    if (len != size) {
      // This condition shold never be true.
      // We return false, so that data can be sent again.
      // @TODO: Need to handle this more gracefully. ASSERT??
      std::cerr << "FATAL: Boost transfer_all did not work." << std::endl;
      Disconnect();
      return false;
    }
    return true;
  }

bool BrokerChannel::RecvDataBlocking(const int max_size, char *buf,
                                     int *recv_size)
{
  boost::system::error_code error;
  // @TODO: Set timeout.
  *recv_size = socket_.read_some(boost::asio::buffer(buf, max_size), error);
  if (*recv_size == 0) {
    std::cerr << "Error occured in socket read: " << error.message() << std::endl;
    state_ = SOCKET_ERROR;  // Disconnect ??
    return false;
  }
  return true;
} 

bool BrokerChannel::IsSameEndpoint(const BrokerMetadata& meta)
{
  if (!strcmp(hostname_.c_str(), meta.host_name) &&
      (port_ == meta.port_num)) {
    return true;
  }
  return false;
}

/********* BrokerPool *****/

BrokerPool::BrokerPool()
{  
}

BrokerPool::~BrokerPool()
{
  ClearPool();
}

void BrokerPool::UpdatePool(TopicInfo& topic_info)
{
  // This function may be called multiple times with info about various
  // topics. Thus the pool is not cleared in this call.
  std::vector<BrokerMetadata>* brokers = topic_info.GetUniqueBrokers();
  BrokerChannel* temp = NULL;
  for (std::vector<BrokerMetadata>::const_iterator it = brokers->begin();
      it != brokers->end(); ++it) {
    if ((temp = GetBrokerChannel(it->broker_id)) != NULL) {
      // Broker with broker_id is already present.
      if (temp->IsSameEndpoint(*it)) {
        // Endpoint address match, so do nothing.
        return;
      } else {
        // Log that broker's endpoint address has changed.
        std::cerr << "Broker's endpoint address is changed." << std::endl;
        // @TODO: should we remove the Channel?? AddBroker can overwrite it.
        //        Having a shared_ptr will help to close the socket.
      }
    }
    // Insert a new Channel.
    AddBrokerChannel(it->broker_id,
                     new BrokerChannel(string(it->host_name), it->port_num,
                                       it->broker_id));
  }
  // Need to release the vector returned by GetUniqueBrokers().
  delete brokers;
}

void BrokerPool::AddBrokerChannel(int32_t broker_id, BrokerChannel *new_broker)
{
  boost::unordered_map<int32_t, BrokerChannel* >::const_iterator it =
      channel_map_.find(broker_id);
  if (it != channel_map_.end()) {
    it->second->Disconnect();  // @TODO: remove when using shared pointer.
  }
  channel_map_[broker_id] = new_broker;
}

BrokerChannel* BrokerPool::GetBrokerChannel(const int32_t broker_id)
{
  boost::unordered_map<int32_t, BrokerChannel*>::const_iterator it =
      channel_map_.find(broker_id);
  if (it == channel_map_.end()) {
    return NULL;
  }
  return it->second;
}

void BrokerPool::RemoveBrokerChannel(const int32_t broker_id)
{
  boost::unordered_map<int32_t, BrokerChannel*>::const_iterator it =
      channel_map_.find(broker_id);
  if (it != channel_map_.end()) {
    it->second->Disconnect();  // @TODO: remove when using shared pointer.
  }
  channel_map_.erase(it);
}

void BrokerPool::ClearPool()
{
  for (boost::unordered_map<int32_t, BrokerChannel*>::const_iterator it =
      channel_map_.begin(); it != channel_map_.end(); ++it)
  {
    it->second->Disconnect();  // @TODO: remove when using shared pointer.
  }
  channel_map_.clear();
}

}  // namespace producer
}  // namepsace kafka
