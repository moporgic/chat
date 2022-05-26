#pragma once
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <map>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <functional>
#include <ctime>
#include <list>
#include <boost/asio.hpp>

namespace chat {

using boost::asio::ip::tcp;
using boost::system::error_code;

class broker_adapter {
public:
	class task {
		friend class broker_adapter;
	public:
		typedef size_t id_t;
		static constexpr id_t id_none = id_t(-1ull);

		enum state_t {
			state_unconfirmed, state_confirmed, state_assigned, state_completed, state_terminated
		};

		id_t id() const { return id_; }
		state_t state() const { return state_; }
		std::string command() const { return command_; }
		int code() const { return code_; }
		std::string output(bool decode = true) const { return decode ? decode_output(output_) : output_; }

		std::string to_string() const;

	private:
		id_t id_ = id_none;
		state_t state_ = state_unconfirmed;
		std::string command_ = {};
		int code_ = -1;
		std::string output_ = {};

		static std::string decode_output(const std::string& encode);
	};

public:

	broker_adapter(boost::asio::io_context& io_context, const std::string& name = {}, const std::string& broker = "broker");

	broker_adapter(const std::string& name = {}, const std::string& broker = "broker");

	virtual ~broker_adapter();

	std::shared_ptr<task> request(const std::string& command, task::state_t state = task::state_confirmed, time_t timeout = 0);

	std::shared_ptr<task> request(const std::string& command, const std::string& options, task::state_t state = task::state_confirmed, time_t timeout = 0);

	std::shared_ptr<task> terminate(std::shared_ptr<task> task, time_t timeout = 0);

	std::shared_ptr<task> wait_until(std::shared_ptr<task> task, task::state_t state = task::state_completed, time_t timeout = 0);

protected:

	virtual bool on_task_completed(std::shared_ptr<task> task) { return true; }

	virtual void on_task_confirmed(std::shared_ptr<task> task, bool accepted) {}

	virtual void on_task_assigned(std::shared_ptr<task> task, const std::string& worker) {}

	virtual void on_idle_worker(const std::string& worker) {}

	virtual void on_busy_worker(const std::string& worker) {}

protected:

	virtual void handle_input(const std::string& input);

	virtual void handle_read_error(error_code ec, size_t n);

	virtual void handle_write_error(const std::string& data, error_code ec, size_t n);

	virtual void handle_connect_error(error_code ec, const tcp::endpoint& remote);

	virtual void handle_handshake_error(const std::string& msg);

protected:

	virtual std::string stringify_request(const std::string& command, const std::string& options) const;

	virtual std::list<std::string> list_subscribed_items() const;

public:

	void connect(const std::string& host, int port, time_t timeout = 10000);

	void disconnect();

	void async_connect(const std::string& host, int port);

	void async_output(const std::string& command, bool to_broker = true);

protected:

	void async_read();

	void async_write(const std::string& data);

protected:

	void notify_all_waits();

private:

	static boost::asio::io_context& io_context();

private:
	boost::asio::io_context& io_context_;
	std::unique_ptr<std::thread> thread_;
	tcp::socket socket_;
	std::string buffer_;

	std::string name_;
	std::string broker_;

private:
	std::list<std::shared_ptr<task>> unconfirmed_;
	std::map<task::id_t, std::shared_ptr<task>> accepted_;

private:
	std::recursive_mutex task_mutex_;
	std::mutex wait_cv_mutex_;
	std::condition_variable wait_cv_;
};

std::ostream& operator <<(std::ostream& out, const broker_adapter::task::state_t& state);

std::ostream& operator <<(std::ostream& out, const broker_adapter::task& task);

} // namespace chat
