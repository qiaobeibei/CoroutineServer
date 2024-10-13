#include "LogicSystem.h"

LogicSystem::LogicSystem() :_b_stop(false) {
	RegisterCallBacks();
	_worker_thread = std::thread(&LogicSystem::DealMsg, this);
}

LogicSystem::~LogicSystem() {
	std::cout << "逻辑层成功析构" << std::endl;
	_b_stop = true;
	_consume.notify_one();
	_worker_thread.join();
}

void LogicSystem::PostMsgToQue(std::shared_ptr<LogicNode> msg) {
	std::unique_lock<std::mutex> unique_lk(_mutex); // 锁定互斥量以保护共享数据
	_msg_que.push(msg); // 将消息推入队列

	if (_msg_que.size() == 1) { // 如果这是队列中的第一个消息
		_consume.notify_one(); // 通知消费者线程，结束wait状态
	}
}

void LogicSystem::DealMsg() {
	for (;;) {
		std::unique_lock<std::mutex> unique_lk(_mutex);
		// 循环会耗费资源，这里使用条件变量挂起线程，并释放锁，直至被唤醒
		while (_msg_que.empty() and !_b_stop) {
			_consume.wait(unique_lk);
		}

		// 判断如果为关闭状态，取出逻辑队列所有数据处理并退出循环
		if (_b_stop) {
			while (!_msg_que.empty()) {
				auto msg_node = _msg_que.front();
				std::cout << "recv msg id is " << msg_node->_recvnode->GetMsgID() << std::endl;
				auto call_back_iter = _fun_callback.find(msg_node->_recvnode->GetMsgID());
				if (call_back_iter == _fun_callback.end()) { // 如果未注册对应消息id的回调函数
					_msg_que.pop(); // 释放该消息
					continue; // 处理下一条消息
				}
				// 取出对应消息id的回调函数
				call_back_iter->second(msg_node->_session, msg_node->_recvnode->GetMsgID(),
					std::string(msg_node->_recvnode->_data, msg_node->_recvnode->_total_len));
				_msg_que.pop();
			}
			break;
		}

		// 如果没有停服，且队列不为空
		auto msg_node = _msg_que.front();
		std::cout << "recv msg id is " << msg_node->_recvnode->GetMsgID() << std::endl;

		auto call_back_iter = _fun_callback.find(msg_node->_recvnode->GetMsgID());
		// 若队列中没有该消息对应的回调函数，则释放该消息并处理下一条
		// 注册_fun_callback时，将消息id以及匹配的回调函数绑定在一起
		if (call_back_iter == _fun_callback.end()) {
			_msg_que.pop();
			continue;
		}
		// 执行消息类型对应的回调函数
		call_back_iter->second(msg_node->_session, msg_node->_recvnode->GetMsgID(),
			std::string(msg_node->_recvnode->_data, msg_node->_recvnode->_total_len));
		_msg_que.pop();
	}
}

void LogicSystem::RegisterCallBacks() {
	_fun_callback[MSG_HELLO_WORD] = std::bind(&LogicSystem::HelloWordCallBack,
		this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
}

void LogicSystem::HelloWordCallBack(std::shared_ptr<CSession> session, const short& msg_id, const std::string& msg_data) {
	Json::Reader reader;
	Json::Value root;
	reader.parse(msg_data, root);
	std::cout << "receive msg id is " << root["id"].asInt() << " msg data is "
		<< root["data"].asString() << std::endl;
	root["data"] = "server has receive msg, msg data is " + root["data"].asString();

	std::string return_str = root.toStyledString();
	session->Send(return_str, root["id"].asInt());
}


LogicSystem& LogicSystem::GetInstance() {
	// 返回局部静态变量在C++11及以上是线程安全的
	static LogicSystem instance;
	return instance;
}