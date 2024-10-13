#pragma once
#include <iostream>
#include <boost/asio.hpp>
#include <queue>
#include "MsgNode.h"
#include "LogicSystem.h"

class CServer;

class CSession :public std::enable_shared_from_this<CSession>
{
private:
	void HandleWrite(const boost::system::error_code& error, std::shared_ptr<CSession> shared_self);
	boost::asio::io_context& _io_context;
	CServer* _server;
	boost::asio::ip::tcp::socket _socket;
	std::string _uuid;
	bool _b_close;
	std::mutex _send_lock;
	std::queue<std::shared_ptr<SendNode>> _send_que;
	std::shared_ptr<RecvNode> _recv_msg_node;
	std::shared_ptr<MsgNode> _recv_head_node;
public:
	CSession(boost::asio::io_context& io_context, CServer* server);
	~CSession();
	boost::asio::ip::tcp::socket& GetSocket();
	void Start();
	std::string& GetUuid();
	void Close();
	void Send(const char* msg, short max_length, short msg_id);
	void Send(std::string msg, short msg_id);
};

class LogicNode {
	friend class LogicSystem;
public:
	LogicNode(std::shared_ptr<CSession>, std::shared_ptr<RecvNode>);
private:
	std::shared_ptr<CSession> _session;
	std::shared_ptr<RecvNode> _recvnode;
};

