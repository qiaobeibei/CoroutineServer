#include "AsioIOServicePool.h"
#include <iostream>

void SafeDeletor::operator()(AsioIOServicePool* st) {
	std::cout << "this is safe deleter operator() of AsioIOServicePool" << std::endl;
	delete st;
}

AsioIOServicePool::AsioIOServicePool(std::size_t size) :
	_ioServices(size), _works(size), _nextIOService(0) {
	for (std::size_t i = 0; i < size; i++) {
		_works[i] = std::unique_ptr<Work>(new Work(_ioServices[i]));
	}

	// 遍历多个ioservice,创建多个线程，每个线程内部启动ioservice
	for (std::size_t i = 0; i < _ioServices.size(); i++) {
		_threads.emplace_back([this, i]() {
			_ioServices[i].run();
			});
	}
}

AsioIOServicePool::~AsioIOServicePool() {
	std::cout << "AsioIOServicePool Pool destruct " << std::endl;
}

boost::asio::io_context& AsioIOServicePool::GetIOService() {
	auto& service = _ioServices[_nextIOService++];
	if (_nextIOService == _ioServices.size())
		_nextIOService = 0;

	return service;
}

void AsioIOServicePool::Stop() {
	for (auto& work : _works) {
		work.reset();
	}

	for (auto& t : _threads) {
		t.join();
	}
}

// 初始化
std::shared_ptr<AsioIOServicePool> AsioIOServicePool::_instance = nullptr;

