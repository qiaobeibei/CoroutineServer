#pragma once
#include <queue>
#include <thread>
#include "CSession.h"
#include <map>
#include <functional>
#include "Const.h"
#include <json/json.h>
#include <json/value.h>
#include <json/reader.h>

class LogicNode;
class CSession;

typedef std::function<void(std::shared_ptr<CSession>, const short& msg_id, const std::string& msg_data)> FunCallBack;

class LogicSystem
{
private:
	LogicSystem();
	~LogicSystem();
	LogicSystem(const LogicSystem&) = delete;
	LogicSystem& operator=(const LogicSystem&) = delete;

	void RegisterCallBacks();
	void HelloWordCallBack(std::shared_ptr<CSession>, const short& msg_id, const std::string& msg_data);
	void DealMsg();

	std::queue<std::shared_ptr<LogicNode>> _msg_que;
	std::mutex _mutex;
	std::condition_variable _consume;
	std::thread _worker_thread;
	bool _b_stop;
	std::map<short, FunCallBack> _fun_callback;
public:
	void PostMsgToQue(std::shared_ptr<LogicNode> msg);
	static LogicSystem& GetInstance();
};

