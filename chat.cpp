#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <unordered_map>
#include <cctype>
#include <atomic>
#include <mutex>
#include <memory>
#include <functional>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <regex>
#include <deque>
#include <boost/asio.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>

class logger {
public:
	void log(const std::string& msg) const {
		std::chrono::system_clock::time_point tp = std::chrono::system_clock::now();
		time_t raw_time = std::chrono::system_clock::to_time_t(tp);
		std::tm* timeinfo = std::localtime(&raw_time);
		std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch());
		std::stringstream ss;
		ss << std::put_time(timeinfo, "%Y-%m-%d %H:%M:%S.") << std::setfill('0') << std::setw(3) << (ms.count() % 1000);
		ss << ' ' << msg;
		std::cerr << ss.rdbuf() << std::flush;
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
} logger;

namespace chat {

using boost::asio::ip::tcp;
using boost::system::error_code;

class client: public std::enable_shared_from_this<client> {
public:
	class handler {
	public:
		virtual void handle_read(std::shared_ptr<client>, const std::string&) = 0;
		virtual void handle_write(std::shared_ptr<client>, const std::string&) = 0;
		virtual void handle_read_error(std::shared_ptr<client>, const std::string&, error_code) = 0;
		virtual void handle_write_error(std::shared_ptr<client>, const std::string&, error_code) = 0;
	};

	client(tcp::socket socket, std::string name, handler* handler) : socket_(std::move(socket)), name_(name), handler_(handler) {}

public:
	class ostream_adapter : public std::stringstream {
	public:
		ostream_adapter(std::shared_ptr<client> self) : std::stringstream(), self(self) {}
		ostream_adapter(ostream_adapter&& out) : std::stringstream(std::move(out)), self(out.self) {}
		ostream_adapter(const ostream_adapter&) = delete;
		~ostream_adapter() { self->async_write(str()); }
	private:
		std::shared_ptr<client> self;
	};

	ostream_adapter output() { return ostream_adapter(shared_from_this()); }
	ostream_adapter reply()  { auto out = output(); out << "% "; return out; }
	ostream_adapter notify() { auto out = output(); out << "# "; return out; }

public:
	tcp::socket& socket() { return socket_; }
	const tcp::socket& socket() const { return socket_; }

	const std::string& name() const { return name_; }
	void name(const std::string& name) { name_ = name; }

public:
	void async_read() {
		if (!read_continue_.exchange(true)) async_read_impl();
	}

	void async_write(const std::string& data) {
		std::scoped_lock lock(mutex_);
		write_queue_.emplace_back(data);
		if (write_queue_.size() == 1) async_write_impl();
	}

private:
	void async_read_impl() {
		auto self(shared_from_this());
		boost::asio::async_read_until(socket_, boost::asio::dynamic_buffer(read_buffer_), "\n",
			[this, self](error_code ec, size_t n) {
				if (!ec) {
					std::string input(read_buffer_.substr(0, n - 1));
					handler_->handle_read(self, input);
					read_buffer_.erase(0, n);
					if (read_continue_.load()) async_read_impl();
				} else {
					handler_->handle_read_error(self, read_buffer_, ec);
				}
			});
	}

	void async_write_impl() {
		auto self(shared_from_this());
		boost::asio::async_write(socket_, boost::asio::buffer(write_queue_.front()),
			[this, self](error_code ec, size_t n) {
				if (!ec) {
					std::scoped_lock lock(mutex_);
					handler_->handle_write(self, write_queue_.front());
					write_queue_.pop_front();
					if (write_queue_.size()) async_write_impl();
				} else {
					handler_->handle_write_error(self, write_queue_.front(), ec);
				}
			});
	}

private:
	tcp::socket socket_;
	std::string name_;
	handler* handler_;
	std::string read_buffer_;
	std::deque<std::string> write_queue_;
	std::atomic<bool> read_continue_ = false;
	std::mutex mutex_;
};

class server : public client::handler {
public:
	server(boost::asio::io_context& io_context, unsigned short port) :
			acceptor_(io_context, tcp::endpoint(tcp::v4(), port)) {
		tcp::endpoint endpoint = acceptor_.local_endpoint();
		logger << "server initialized: " << endpoint.address() << ':' << endpoint.port() << std::endl;
	}

	void async_accept() {
		acceptor_.async_accept(
			[this](error_code ec, tcp::socket socket) {
				if (!ec) {
					boost::asio::socket_base::keep_alive option(true);
					socket.set_option(option);
					std::scoped_lock lock(mutex_);
					std::string name;
					while (find_client(name = "u" + std::to_string(++ticket_)) != nullptr);
					std::shared_ptr<client> user = std::make_shared<client>(std::move(socket), name, this);
					handle_client_login(user);
					user->async_read();
				} else {
					logger << boost::format("exception at async_accept: %s") % ec << std::endl;
				}
				async_accept();
			});
	}

protected:
	virtual void handle_read(std::shared_ptr<client> self, const std::string& input) {
		logger << self->name() << " >> " << input << std::endl;

		if (auto it = input.find('<'); it != std::string::npos) { // WHO << MESSAGE
			auto wt = input.find_first_not_of(' '), mt = input.find_first_not_of('<', it);
			std::string who = input.substr(wt, std::min(input.find_last_not_of(' ', it - 1) + 1 - wt, it - wt));
			std::string msg = input.substr(mt != std::string::npos ? mt + (input[mt] == ' ') : input.size());

			std::shared_ptr<client> remote = find_client(who);
			if (remote) {
				remote->output() << boost::format("%s >> %s") % self->name() % msg << std::endl;

			} else if (who.find_first_of("*?") != std::string::npos) {
				boost::replace_all(who, ".", "\\.");
				boost::replace_all(who, "*", ".*");
				boost::replace_all(who, "?", ".");
				std::regex broadcast(who);
				std::vector<std::shared_ptr<client>> remotes;
				std::string whos;
				for (std::shared_ptr<client> remote : list_clients()) {
					if (std::regex_match(remote->name(), broadcast)) {
						remotes.push_back(remote);
						whos += remote->name() + ' ';
					}
				}
				if (whos.size()) {
					whos.pop_back();
					self->notify() << boost::format("broadcast: %s") % whos << std::endl;
					for (std::shared_ptr<client> remote : remotes) {
						remote->output() << boost::format("%s >> %s") % self->name() % msg << std::endl;
					}
				} else {
					self->reply() << boost::format("failed chat: invalid broadcast") << std::endl;
				}
			} else {
				self->reply() << boost::format("failed chat: invalid client") << std::endl;
			}
			return;
		}

		std::stringstream parser(input);
		std::string cmd;
		parser >> cmd;

		if (cmd == "name") {
			std::string name;
			parser >> name;
			std::string old = self->name();

			if (name.empty() || name == old) {
				self->reply() << boost::format("name: %s") % self->name() << std::endl;
			} else {
				auto legal = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789/_.+-";
				if (name.find_first_not_of(legal) == std::string::npos && rename_client(self, name)) {
					self->reply() << boost::format("name: %s") % self->name() << std::endl;
					for (auto user : list_clients()) {
						user->notify() << boost::format("name: %s becomes %s") % old % name << std::endl;
					}
				} else {
					self->reply() << boost::format("failed name: invalid or duplicate") << std::endl;
				}
			}

		} else if (cmd == "who") {
			std::string name;
			parser >> name;

			if (name.empty()) {
				auto out = self->reply();
				out << "who:";
				for (auto user : list_clients()) out << ' ' << user->name();
				out << std::endl;
			} else {
				std::shared_ptr<client> who = find_client(name);
				if (who) {
					error_code ec;
					tcp::endpoint endpoint = who->socket().remote_endpoint(ec);
					std::string from = !ec ? (boost::format("%s:%d") % endpoint.address() % endpoint.port()).str() : "unknown";
					self->reply() << boost::format("who: %s from %s") % name % from << std::endl;
				} else {
					self->reply() << boost::format("failed who: invalid client") << std::endl;
				}
			}

		} else if (cmd == "protocol") {
			std::string version = "0";
			parser >> version;

			if (version == "0") {
				self->reply() << boost::format("protocol: %s") % version << std::endl;
			} else {
				self->reply() << boost::format("failed protocol: unsupported") << std::endl;
			}
		}
	}

	virtual void handle_write(std::shared_ptr<client> self, const std::string& output) {
		logger << self->name() << " << " << output; // output already contains '\n'
	}

	virtual void handle_read_error(std::shared_ptr<client> self, const std::string& buffer, error_code ec) {
		if (self == find_client(self->name())) {
			if (ec == boost::system::errc::success || ec == boost::asio::error::eof) {
			} else {
				logger << boost::format("exception at read error: %s; %s") % ec % buffer << std::endl;
			}
			handle_client_logout(self);
		} else {
			logger << boost::format("mismatched client %s on read error") % self->name() << std::endl;
		}
	}

	virtual void handle_write_error(std::shared_ptr<client> self, const std::string& output, error_code ec) {
		if (self == find_client(self->name())) {
			if (output.find(" > ") != std::string::npos) {
				std::string src = output.substr(0, output.find(" > "));
				std::string msg = output.substr(output.find(" > ") + 3);
				auto source = find_client(src);
				if (source) {
					source->reply() << boost::format("failed chat: remote error") << std::endl;
				}
			}
			logger << boost::format("exception at write error: %s; %s") % ec % output << std::endl;
			handle_client_logout(self);
		} else {
			logger << boost::format("mismatched client %s on write error") % self->name() << std::endl;
		}
	}

protected:
	void handle_client_login(std::shared_ptr<client> self) {
		error_code ec;
		tcp::endpoint endpoint = self->socket().remote_endpoint(ec);
		std::string from = !ec ? (boost::format("%s:%d") % endpoint.address() % endpoint.port()).str() : "unknown";
		if (insert_client(self)) {
			logger << boost::format("login: %s %s") % self->name() % from << std::endl;
			for (auto user : list_clients()) {
				user->notify() << boost::format("login: %s") % self->name() << std::endl;
			}
		} else {
			logger << boost::format("failed to insert client %s %s") % self->name() % from << std::endl;
		}
	}

	void handle_client_logout(std::shared_ptr<client> self) {
		error_code ec;
		tcp::endpoint endpoint = self->socket().remote_endpoint(ec);
		std::string from = !ec ? (boost::format("%s:%d") % endpoint.address() % endpoint.port()).str() : "unknown";
		if (remove_client(self)) {
			logger << boost::format("logout: %s %s") % self->name() % from << std::endl;
			for (auto user : list_clients()) {
				user->notify() << boost::format("logout: %s") % self->name() << std::endl;
			}
		} else {
			logger << boost::format("failed to remove client %s %s") % self->name() % from << std::endl;
		}
	}

private:
	std::shared_ptr<client> find_client(const std::string& name) {
		std::scoped_lock lock(mutex_);
		auto it = clients_.find(name);
		return it != clients_.end() ? it->second : nullptr;
	}
	std::vector<std::shared_ptr<client>> list_clients() {
		std::scoped_lock lock(mutex_);
		std::vector<std::shared_ptr<client>> users;
		users.reserve(clients_.size());
		for (const auto& pair : clients_) users.push_back(pair.second);
		return users;
	}
	bool rename_client(std::shared_ptr<client> user, const std::string& after) {
		std::scoped_lock lock(mutex_);
		if (find_client(user->name()) != user) return false;
		if (find_client(after) != nullptr) return false;
		auto hdr = clients_.extract(user->name());
		hdr.key() = after;
		clients_.insert(std::move(hdr));
		user->name(after);
		return true;
	}
	bool insert_client(std::shared_ptr<client> user) {
		std::scoped_lock lock(mutex_);
		if (find_client(user->name()) != nullptr) return false;
		clients_.insert({user->name(), user});
		return true;
	}
	bool remove_client(std::shared_ptr<client> user) {
		std::scoped_lock lock(mutex_);
		if (find_client(user->name()) != user) return false;
		clients_.erase(user->name());
		return true;
	}

private:
	tcp::acceptor acceptor_;
	std::unordered_map<std::string, std::shared_ptr<client>> clients_;
	std::recursive_mutex mutex_;
	size_t ticket_ = 0;
};

} // namespace chat

int main(int argc, char *argv[]) {
	try {
		logger << "chat::service version 2024-02-02 (protocol 0)" << std::endl;

		boost::asio::io_context io_context;
		chat::server chat(io_context, argc < 2 ? 10000 : std::stoul(argv[1]));
		chat.async_accept();
		io_context.run();

	} catch (std::exception &e) {
		logger << "exception: " << e.what() << std::endl;
	}
	return 0;
}
