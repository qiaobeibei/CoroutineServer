#include "CSession.h"
#include "CServer.h"

CSession::CSession(boost::asio::io_context& io_context, CServer* server) :
	_io_context(io_context), _server(server), _socket(io_context), _b_close(false) {
	// random_generator是函数对象，加()就是函数，再加一个()就是调用该函数
	boost::uuids::uuid a_uuid = boost::uuids::random_generator()();
	_uuid = boost::uuids::to_string(a_uuid);
	_recv_head_node = std::make_shared<MsgNode>(HEAD_TOTAL_LEN);
}

boost::asio::ip::tcp::socket& CSession::GetSocket() {
	return _socket;
}

void CSession::Start() {
	// 防止协程处理过程中，智能指针被意外释放，通过智能指针实现伪闭包
	auto shared_this = shared_from_this();
	// 开启协程接收
	boost::asio::co_spawn(_io_context, [self = shared_from_this(), this]()->boost::asio::awaitable<void> {
		try {
			for (; !_b_close;) {
				_recv_head_node->Clear();
				// 以同步的方式调用异步读，并将读到的字节数（4字节）返回（使用简易粘包处理方式）
				std::size_t n = co_await boost::asio::async_read(_socket, boost::asio::buffer(
					_recv_head_node->_data, HEAD_TOTAL_LEN), use_awaitable);

				if (n == 0) { // 如果没读到数据
					std::cout << "receive peer closed" << std::endl;
					Close();
					_server->ClearSession(_uuid);
					co_return; // 类似return，但co_return是非阻塞的
				}

				// 获取头部消息id
				short msg_id = 0;
				memcpy(&msg_id, _recv_head_node->_data, HEAD_ID_LEN);
				// 转换网络字节序为本地字节序
				msg_id = boost::asio::detail::socket_ops::network_to_host_short(msg_id);
				std::cout << "msg id is " << msg_id << std::endl;
				if (msg_id > MAX_LENGTH) {
					std::cout << "invalid msg id is " << msg_id << std::endl;
					Close();
					_server->ClearSession(_uuid);
					co_return;
				}

				// 获取消息长度
				short msg_len = 0;
				memcpy(&msg_len, _recv_head_node->_data + HEAD_ID_LEN, HEAD_DATA_LEN);
				msg_len = boost::asio::detail::socket_ops::network_to_host_short(msg_len);
				std::cout << "msg len is " << msg_len << std::endl;
				if (msg_len > MAX_LENGTH) {
					std::cout << "invalid msg len is " << msg_len << std::endl;
					Close();
					_server->ClearSession(_uuid);
					co_return;
				}

				// 获取消息内容
				_recv_msg_node = std::make_shared<RecvNode>(msg_len, msg_id); // 构造消息节点
				// 将异步读以同步方式调用，指定读取字节数
				n = co_await boost::asio::async_read(_socket, boost::asio::buffer(_recv_msg_node->_data,
					_recv_msg_node->_total_len),use_awaitable);
				if (n == 0) {
					std::cout << "receive peer closed " << std::endl;
					Close();
					_server->ClearSession(_uuid);
					co_return;
				}

				_recv_msg_node->_data[_recv_msg_node->_total_len] = '\0';
				std::cout << "receive data is " << _recv_msg_node->_data << std::endl;
				// 投递至逻辑线程
				LogicSystem::GetInstance().PostMsgToQue(std::make_shared<LogicNode>(shared_from_this(), _recv_msg_node));
			}
		}
		catch (std::exception& e) {
			std::cerr << "exception is " << e.what() << std::endl;
			Close();
			_server->ClearSession(_uuid);
		}
		}, detached);
}

std::string& CSession::GetUuid() {
	return _uuid;
}

void CSession::Close() {
	_b_close = true;
	_socket.close();
}

CSession::~CSession() {
	try {
		std::cout << "~CSession destruct" << std::endl;
	}
	catch (std::exception& e) {
		std::cerr << "exception is " << e.what() << std::endl;
	}
}

void CSession::Send(const char* msg, short max_length, short msg_id) {
	bool pending = false; // 发送标志，true时有未完成的发送操作，false为空
	std::unique_lock<std::mutex> lock(_send_lock);
	int send_que_size = _send_que.size();
	if (send_que_size > MAX_SENDQUE) {
		std::cout << "session: " << _uuid << " send que fulled, size is " << MAX_SENDQUE << std::endl;
		return;
	}

	// 判断队列是否有未完成的发送操作
	if (_send_que.size() > 0) {
		pending = true;
	}
	_send_que.push(std::make_shared<SendNode>(msg, max_length, msg_id));
	if (pending) { // 如果有未完成的发送，直接返回
		return;
	}

	auto msgnode = _send_que.front();
	// 数据取出来之后就解锁，仅在使用队列时加锁，避免不相干的逻辑占用锁
	lock.unlock();
	boost::asio::async_write(_socket, boost::asio::buffer(msgnode->_data, msgnode->_total_len),
		std::bind(&CSession::HandleWrite, this, std::placeholders::_1, shared_from_this()));
}

void CSession::Send(std::string msg, short msg_id) {
	bool pending = false; // 发送标志，true时有未完成的发送操作，false为空
	std::unique_lock<std::mutex> lock(_send_lock);
	int send_que_size = _send_que.size();
	if (send_que_size > MAX_SENDQUE) {
		std::cout << "session: " << _uuid << " send que fulled, size is " << MAX_SENDQUE << std::endl;
		return;
	}

	// 判断队列是否有未完成的发送操作
	if (_send_que.size() > 0) {
		pending = true;
	}
	_send_que.push(std::make_shared<SendNode>(msg.c_str(), msg.length(), msg_id));
	if (pending) { // 如果有未完成的发送，直接返回
		return;
	}

	auto msgnode = _send_que.front();
	// 数据取出来之后就解锁，仅在使用队列时加锁，避免不相干的逻辑占用锁
	lock.unlock();
	boost::asio::async_write(_socket, boost::asio::buffer(msgnode->_data, msgnode->_total_len),
		std::bind(&CSession::HandleWrite, this, std::placeholders::_1, shared_from_this()));
}

void CSession::HandleWrite(const boost::system::error_code& error, std::shared_ptr<CSession> shared_self) {
	try {
		if (!error) { 
			std::unique_lock<std::mutex> lock(_send_lock); // 加锁保护发送队列
			_send_que.pop(); // 移除上一个已发送的消息（send函数中的异步发）
			if (!_send_que.empty()) { // 若队列不为空，处理下一个消息
				auto& msgnode = _send_que.front();
				lock.unlock(); // 解锁，避免不相干逻辑占用锁
				boost::asio::async_write(_socket, boost::asio::buffer(msgnode->_data, msgnode->_total_len),
					std::bind(&CSession::HandleWrite, this, std::placeholders::_1, shared_self));
			}
		}
		else {
			std::cerr << "handle write failed, error is " << error.what() << std::endl;
			Close();
			_server->ClearSession(_uuid);
		}
	}
	catch (std::exception& e) {
		std::cout << "exception is " << e.what() << std::endl;
		Close();
		_server->ClearSession(_uuid);
	}
}


LogicNode::LogicNode(std::shared_ptr<CSession> session, std::shared_ptr<RecvNode> recvnode) :
	_session(session), _recvnode(recvnode) {

}

