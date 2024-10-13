#include <boost/asio.hpp>
#include <iostream>
#include <json/json.h>
#include <json/value.h>
#include <json/reader.h>

using namespace boost::asio::ip;
using std::cout;
using std::endl;
const int MAX_LENGTH = 1024 * 2; // 发送和接收的长度为1024 * 2字节
const int HEAD_LENGTH = 2;
const int HEAD_TOTAL = 4;

int main()
{
    try {
        boost::asio::io_context ioc; // 创建上下文服务
        // 127.0.0.1是本机的回路地址，也就是服务器和客户端在一个机器上
        tcp::endpoint remote_ep(address::from_string("127.0.0.1"), 10086); // 构造endpoint
        tcp::socket sock(ioc);
        boost::system::error_code error = boost::asio::error::host_not_found; // 错误：主机未找到
        sock.connect(remote_ep, error);
        if (error) {
            cout << "connect failed, code is " << error.value() << " error msg is " << error.message();
            return 0;
        }

        Json::Value root;
        root["id"] = 1001;
        root["data"] = "hello world";
        std::string request = root.toStyledString();
        size_t request_length = request.length();
        char send_data[MAX_LENGTH] = { 0 };
        int msgid = 1001;
        int msgid_host = boost::asio::detail::socket_ops::host_to_network_short(msgid);
        memcpy(send_data, &msgid_host, 2);

        //转为网络字节序
        int request_host_length = boost::asio::detail::socket_ops::host_to_network_short(request_length);
        memcpy(send_data + 2, &request_host_length, 2);
        memcpy(send_data + 4, request.c_str(), request_length);
        boost::asio::write(sock, boost::asio::buffer(send_data, request_length + 4));

        char reply_head[HEAD_TOTAL]; // 首先读取对端发送消息的总长度
        size_t reply_length = boost::asio::read(sock, boost::asio::buffer(reply_head, HEAD_TOTAL));
        msgid = 0;
        memcpy(&msgid, reply_head, HEAD_LENGTH);
        short msglen = 0; // 消息总长度
        memcpy(&msglen, reply_head + 2, HEAD_LENGTH); // 将消息总长度赋值给msglen
        //转为本地字节序
        msglen = boost::asio::detail::socket_ops::network_to_host_short(msglen);
        char msg[MAX_LENGTH] = { 0 }; // 构建消息体（不含消息总长度）
        size_t msg_length = boost::asio::read(sock, boost::asio::buffer(msg, msglen));

        Json::Reader reader;
        reader.parse(std::string(msg, msg_length), root);
        std::cout << "msg id is " << root["id"] << " msg is " << root["data"] << endl;
        getchar();
    }
    catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << endl;
    }
    return 0;
}