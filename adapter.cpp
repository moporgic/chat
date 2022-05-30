#include "adapter.h"
#include <chrono>
#include <iomanip>
#include <regex>
#include <algorithm>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>

class logger {
public:
	void log(const std::string& msg) const {
#ifndef DISABLE_LOGGING
		std::chrono::system_clock::time_point tp = std::chrono::system_clock::now();
		time_t raw_time = std::chrono::system_clock::to_time_t(tp);
		std::tm* timeinfo = std::localtime(&raw_time);
		std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch());
		std::stringstream ss;
		ss << std::put_time(timeinfo, "%Y-%m-%d %H:%M:%S.") << std::setfill('0') << std::setw(3) << (ms.count() % 1000);
		ss << ' ' << msg;
		std::cerr << ss.rdbuf() << std::flush;
#endif
	}

	class ostream_adapter : public std::stringstream {
	public:
		ostream_adapter(logger& logref) : std::stringstream(), logref(logref) {}
		ostream_adapter(ostream_adapter&& out) : std::stringstream(std::move(out)), logref(out.logref) {}
		ostream_adapter(const ostream_adapter&) = delete;
		~ostream_adapter() { logref.log(str()); }
	private:
		logger& logref;
	};

	template<typename type>
	ostream_adapter operator<<(const type& t) {
		ostream_adapter out(*this);
		out << t;
		return out;
	}
} _log;

namespace chat {

using boost::asio::ip::tcp;
using boost::system::error_code;

std::string broker_adapter::task::to_string() const {
	std::stringstream out;
	switch (state_) {
	default:
		out << boost::format("%llu:%d {%s} %d {%s}") % id_ % state_ % command_ % code_ % output_;
		break;
	case broker_adapter::task::state_unconfirmed:
		out << boost::format("? {%s}") % command_;
		break;
	case broker_adapter::task::state_confirmed:
		if (id_ != task::id_none)
			out << boost::format("%llu {%s}") % id_ % command_;
		else
			out << boost::format("X {%s}") % command_;
		break;
	case broker_adapter::task::state_assigned:
		out << boost::format("%llu {%s} at %s") % id_ % command_ % output_;
		break;
	case broker_adapter::task::state_completed:
		out << boost::format("%llu {%s} %d {%s}") % id_ % command_ % code_ % output_;
		break;
	}
	return out.str();
}

std::string broker_adapter::task::decode_output(const std::string& encode) {
	std::string decode = encode;
	boost::replace_all(decode, "\\n", "\n");
	boost::replace_all(decode, "\\t", "\t");
	boost::replace_all(decode, "\\\\", "\\");
	return decode;
}

std::ostream& operator <<(std::ostream& out, const broker_adapter::task::state_t& state) {
	const char* names[] = {"unconfirmed", "confirmed", "assigned", "completed", "terminated", "unknown"};
	return out << names[std::min(int(state), 5)];
}

std::ostream& operator <<(std::ostream& out, const broker_adapter::task& task) { return out << task.to_string(); }

using task = broker_adapter::task;

broker_adapter::broker_adapter(boost::asio::io_context& io_context, const std::string& name, const std::string& broker) :
	io_context_(io_context), socket_(io_context), name_(name), broker_(broker) {}

broker_adapter::broker_adapter(const std::string& name, const std::string& broker) : broker_adapter(io_context(), name, broker) {}

broker_adapter::~broker_adapter() { disconnect(); }

std::shared_ptr<task> broker_adapter::request(const std::string& command, task::state_t state, time_t timeout) {
	return request(command, {}, state, timeout);
}

std::shared_ptr<task> broker_adapter::request(const std::string& command, const std::string& options, task::state_t state, time_t timeout) {
	std::shared_ptr<task> task = std::make_shared<broker_adapter::task>();
	{
		std::scoped_lock lock(task_mutex_);
		task->command_ = command;
		unconfirmed_.push_back(task);
		std::string request = stringify_request(command, options);
		async_output(request);
		_log << request << " has been sent" << std::endl;
	}
	return (state >= task::state_unconfirmed) ? wait_until(task, state, timeout) : task;
}

std::shared_ptr<task> broker_adapter::terminate(std::shared_ptr<task> task, time_t timeout) {
	async_output("terminate " + std::to_string(task->id()));
	_log << "terminate " << task->id() << " has been sent" << std::endl;
	return wait_until(task, task::state_terminated, timeout);
}

std::shared_ptr<task> broker_adapter::wait_until(std::shared_ptr<task> task, task::state_t state, time_t timeout) {
	std::unique_lock lock(wait_cv_mutex_);
	if (timeout) {
		_log << "wait for request '" << *task << "' becomes " << state << " with at most " << timeout << "ms" << std::endl;
		auto until = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout);
		wait_cv_.wait_until(lock, until, [task, state]{ return task->state_ >= state; });
	} else {
		_log << "wait for request '" << *task << "' becomes " << state << std::endl;
		wait_cv_.wait(lock, [task, state]{ return task->state_ >= state; });
	}
	if (task->state_ >= state) {
		_log << "stop waiting, '" << *task << "' has become " << task->state_ << std::endl;
	} else {
		_log << "timed out, '" << *task << "' has not become " << state << std::endl;
	}
	return task;
}

std::regex _regex_message_from("^(\\S+) >> (.+)$");
std::regex _regex_confirm_request("^(accept|reject) request ([0-9]+ )?\\{(.+)\\}$");
std::regex _regex_response("^response ([0-9]+) (.+) \\{(.*)\\}$");
std::regex _regex_notify_assign("^notify assign request ([0-9]+) to (\\S+)$");
std::regex _regex_notify_state("^notify (\\S+) state (idle|busy)$");
std::regex _regex_notify_capacity("^notify capacity ([0-9]+) ?(.*)$");
std::regex _regex_confirm_protocol("^(accept|reject) protocol (.+)$");

void broker_adapter::handle_input(const std::string& input) {
	std::smatch match;
	if (regex_match(input, match, _regex_message_from)) {
		std::string sender = match[1].str();
		std::string message = match[2].str();

		if (sender == broker_) {

			if (regex_match(message, match, _regex_confirm_request)) {
				bool accepted = (match[1].str() == "accept");
				id_t id = accepted ? std::stoull(match[2].str()) : task::id_none;
				std::string command = match[3].str();

				std::scoped_lock lock(task_mutex_);

				auto it = std::find_if(unconfirmed_.begin(), unconfirmed_.end(),
						[=](std::shared_ptr<task> task) { return task->command_ == command; });
				if (it != unconfirmed_.end()) {
					auto task = *it;
					unconfirmed_.erase(it);

					task->id_ = id;
					task->state_ = task::state_confirmed;
					notify_all_waits();

					if (accepted) accepted_.insert({id, task});
					on_task_confirmed(task, accepted);
					_log << boost::format("confirm %sed request %llu {%s}") % match[1] % id % command << std::endl;

				} else {
					_log << boost::format("ignore the confirmation of nonexistent request %llu {%s}") % id % command << std::endl;
				}

			} else if (regex_match(message, match, _regex_response)) {
				id_t id = std::stoull(match[1].str());
				std::string code = match[2].str();
				std::string output = match[3].str();

				std::scoped_lock lock(task_mutex_);

				auto it = accepted_.find(id);
				if (it != accepted_.end()) {
					auto task = it->second;
					accepted_.erase(it);

					try {
						task->code_ = std::stol(code);
						task->output_ = output;
						task->state_ = task::state_completed;
					} catch (std::invalid_argument&) {
						task->code_ = -1;
						task->output_ = code;
						task->state_ = task::state_terminated;
					}
					notify_all_waits();

					bool accept = on_task_completed(task);
					std::string confirm = accept ? "accept" : "reject";
					async_output(confirm + " response " + std::to_string(id));
					_log << boost::format("%s response %llu %s {%s}") % confirm % id % code % output << std::endl;
					if (!accept) {
						task->state_ = task::state_unconfirmed;
						unconfirmed_.push_back(task);
					}

				} else {
					_log << boost::format("ignore the response of nonexistent request %llu") % id << std::endl;
				}

			} else if (regex_match(message, match, _regex_notify_state)) {
				std::string worker = match[1].str();
				std::string state = match[2].str();
				if (state == "idle")
					on_idle_worker(worker);
				else if (state == "busy")
					on_busy_worker(worker);
				_log << boost::format("confirm worker %s is %s") % worker % state << std::endl;

			} else if (regex_match(message, match, _regex_notify_assign)) {
				id_t id = std::stoull(match[1].str());
				std::string worker = match[2].str();

				std::scoped_lock lock(task_mutex_);

				auto it = accepted_.find(id);
				if (it != accepted_.end()) {
					auto task = it->second;

					task->output_ = worker;
					task->state_ = task::state_assigned;
					notify_all_waits();

					on_task_assigned(task, worker);
					_log << boost::format("confirm request %llu assigned to worker %s") % id % worker << std::endl;

				} else {
					_log << boost::format("ignore the confirmation of nonexistent request %llu assigned to worker %s") % id % worker << std::endl;
				}

			} else if (regex_match(message, match, _regex_notify_capacity)) {
				size_t capacity = std::stoull(match[1].str());
				std::string details = match[2].str();
				on_capacity_changed(capacity, details);
				_log << boost::format("confirm capacity %llu with '%s'") % capacity % details << std::endl;

			} else if (regex_match(message, match, _regex_confirm_protocol)) {
				bool accepted = (match[1].str() == "accept");
				if (accepted) {
					notify_all_waits();
					_log << boost::format("handshake with %s successfully") % broker_ << std::endl;

					for (const std::string& item : list_subscribed_items()) {
						async_output("subscribe " + item);
					}
				} else {
					handle_handshake_error(message);
				}

			} else /* message does not match any watched formats */ {
				_log << boost::format("ignore message '%s' from %s") % message % sender << std::endl;
			}

		} else /* if (sender != broker_) */ {
			_log << boost::format("ignore message '%s' from %s") % message % sender << std::endl;
		}

	} else if (input.size() && input[0] == '%') { // reply from system
		std::string message = input.substr(2);
		if (message.find("name") == 0) {
			std::string name = message.substr(message.find(' ') + 1);
			name_ = name;
			_log << boost::format("confirm client name %s from chat system") % name << std::endl;
			_log << boost::format("send handshake to %s...") % broker_ << std::endl;
			async_output("use protocol 0");

		} else if (message.find("failed") == 0) {
			handle_handshake_error(message);
		}

	} else if (input.size() && input[0] == '#') { // notification from system
		std::string message = input.substr(2);
		_log << boost::format("ignore notification '%s'") % message << std::endl;
	}
}

void broker_adapter::handle_read_error(error_code ec, size_t n) {
	if (ec == boost::asio::error::eof || ec == boost::asio::error::operation_aborted) {
		_log << "socket input stream closed: " << ec << std::endl;
	} else {
		_log << "unexpected socket read error: " << ec << std::endl;
		disconnect();
	}
}
void broker_adapter::handle_write_error(const std::string& data, error_code ec, size_t n) {
	_log << "unexpected socket write error: " << ec << std::endl;
	disconnect();
}
void broker_adapter::handle_connect_error(error_code ec, const tcp::endpoint& remote) {
	_log << "unexpected socket connect error: " << ec << std::endl;
	disconnect();
}
void broker_adapter::handle_handshake_error(const std::string& msg) {
	_log << "unexpected chat handshake error: " << msg << std::endl;
	disconnect();
}

std::string broker_adapter::stringify_request(const std::string& command, const std::string& options) const {
	std::string request;
	request.reserve(command.size() + options.size() + 20);
	request += "request {";
	request += command;
	request += '}';
	if (options.size()) {
		request += " with ";
		request += options;
	}
	return request;
}

std::list<std::string> broker_adapter::list_subscribed_items() const {
	return {"idle", "assign", "capacity"};
}

bool broker_adapter::connect(const std::string& host, int port, time_t timeout) {
	if (thread_) return true;

	async_connect(host, port);
	std::unique_lock lock(wait_cv_mutex_);
	if (timeout) {
		_log << "wait for the connection with at most " << timeout << "ms" << std::endl;
		auto until = std::chrono::system_clock::now() + std::chrono::milliseconds(timeout);
		if (wait_cv_.wait_until(lock, until, []{ return true; }))
			return true;
		_log << "connection timed out" << std::endl;
		handle_handshake_error("timeout");
		return false;
	} else {
		_log << "wait for the connection" << std::endl;
		wait_cv_.wait(lock, []{ return true; });
		return true;
	}
}

void broker_adapter::disconnect() {
	if (!thread_) return;
	_log << "disconnecting..." << std::endl;

	boost::asio::post(io_context_, [this]() { socket_.close(); });
	try {
		buffer_.clear();
		thread_->join();
		thread_.reset();
	} catch (std::exception&) {}
	_log << "disconnected" << std::endl;
}

void broker_adapter::async_connect(const std::string& host, int port) {
	if (thread_) return;
	_log << boost::format("connecting to %s %d...") % host % port << std::endl;

	tcp::resolver resolver(io_context_);
	auto endpoints = resolver.resolve(host, std::to_string(port));

	boost::asio::async_connect(socket_, endpoints,
		[this](error_code ec, tcp::endpoint remote) {
			if (!ec) {
				_log << "connected to " << remote.address().to_string() << ':' << remote.port() << std::endl;
				boost::asio::socket_base::keep_alive option(true);
				socket_.set_option(option);
				_log << "send chat room handshake messages" << std::endl;
				async_output("protocol 0", false);
				async_output(name_.size() ? "name " + name_ : "name", false);
				// start reading from socket input
				async_read();
			} else {
				handle_connect_error(ec, remote);
			}
		});

	thread_ = std::make_unique<std::thread>([this]() { io_context_.run(); });
}

void broker_adapter::async_output(const std::string& command, bool to_broker) {
	std::string output;
	output.reserve(command.length() + broker_.length() + 10);
	if (to_broker) {
		output += broker_;
		output += " << ";
	}
	output += command;
	output += '\n';
	async_write(output);
	_log << "output '" << command << (to_broker ? "' to broker" : "'") << std::endl;
}

void broker_adapter::async_read() {
	boost::asio::async_read_until(socket_, boost::asio::dynamic_buffer(buffer_), "\n",
		[this](error_code ec, size_t n) {
			if (!ec) {
				std::string input(buffer_.substr(0, n - 1));
				handle_input(input);
				buffer_.erase(0, n);
				async_read();
			} else {
				handle_read_error(ec, n);
			}
		});
}

void broker_adapter::async_write(const std::string& data) {
	auto buffer = std::make_shared<std::string>(data);
	boost::asio::async_write(socket_, boost::asio::buffer(*buffer),
		[this, buffer](error_code ec, size_t n) {
			if (!ec) {
			} else {
				handle_write_error(*buffer, ec, n);
			}
		});
}

void broker_adapter::notify_all_waits() {
	{ std::scoped_lock lock(wait_cv_mutex_); }
	wait_cv_.notify_all();
}

boost::asio::io_context& broker_adapter::io_context() {
	static boost::asio::io_context io_context;
	return io_context; // reference of a lazy-initialized io_context
}

} // namespace chat

int main(int argc, char *argv[]) {
	try {
		if (argc != 3) {
			std::cerr << "usage: " << argv[0] << " <host> <port>" << std::endl;
			return 1;
		}
		chat::broker_adapter c;
		c.connect(argv[1], std::stoul(argv[2]), 1);

		std::string line;
		while (std::getline(std::cin, line) && line.size()) {
			c.async_output(line);
		}

	} catch (std::exception& e) {
		std::cerr << "exception: " << e.what() << "\n";
	}

	return 0;
}
