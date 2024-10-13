#pragma once
#include "boost/asio.hpp"
#include <iostream>
#include <vector>
#include <memory>

class AsioIOServicePool;

class SafeDeletor{
public:
	void operator()(AsioIOServicePool* st);
};

class AsioIOServicePool
{
public:
	friend class SafeDeletor;
	using IOService = boost::asio::io_context;
	using Work = boost::asio::io_context::work;
	using WorkPtr = std::unique_ptr<Work>;

	static std::shared_ptr<AsioIOServicePool> GetInstance() {
		static std::once_flag s_flag; 
		std::call_once(s_flag, [&]() { // call_onceÄÚ²¿ÓÐËø
			_instance = std::shared_ptr<AsioIOServicePool>(new AsioIOServicePool, SafeDeletor());
			});
		return _instance;
	}
	boost::asio::io_context& GetIOService();
	void Stop();
private:
	AsioIOServicePool(std::size_t size = std::thread::hardware_concurrency());
	~AsioIOServicePool();
	AsioIOServicePool(const AsioIOServicePool&) = delete;
	AsioIOServicePool& operator = (const AsioIOServicePool&) = delete;

	std::vector<IOService> _ioServices;
	std::vector<WorkPtr> _works;
	std::vector<std::thread> _threads;
	std::size_t _nextIOService;
	static std::shared_ptr<AsioIOServicePool> _instance;
};

