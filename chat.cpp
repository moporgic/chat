#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <map>
#include <cctype>
#include <mutex>
#include <memory>
#include <functional>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <boost/asio.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>

using boost::asio::ip::tcp;

class logger {
public:
	void log(const std::string& msg) const {
		std::string info = current_time();
		info.reserve(info.size() + msg.size() + 10);
		info += ' ';
		info += msg;
		std::cerr << info << std::flush;
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

private:
	std::string current_time() const {
		std::chrono::system_clock::time_point tp = std::chrono::system_clock::now();
		time_t raw_time = std::chrono::system_clock::to_time_t(tp);
		std::tm* timeinfo = std::localtime(&raw_time);
		std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch());
		std::stringstream ss;
		ss << std::put_time(timeinfo, "%Y-%m-%d %H:%M:%S.") << std::setfill('0') << std::setw(3) << (ms.count() % 1000);
		return ss.str();
	}
} logger;

class session: public std::enable_shared_from_this<session> {
public:
	session(tcp::socket socket, std::string name) : socket_(std::move(socket)), name_(name) {}

	tcp::socket& socket() { return socket_; }
	const tcp::socket& socket() const { return socket_; }

	const std::string& name() const { return name_; }
	void name(const std::string& name) { name_ = name; }

	class supervisor {
	public:
		virtual void handle_command(std::shared_ptr<session>, const std::string&) = 0;
		virtual void handle_read_error(std::shared_ptr<session>, boost::system::error_code) = 0;
		virtual void handle_write_error(std::shared_ptr<session>, const std::string&, boost::system::error_code) = 0;
	};

	supervisor* super() { return super_; }
	void super(supervisor* s) { super_ = s; }

	class ostream_adapter : public std::stringstream {
	public:
		ostream_adapter(std::shared_ptr<session> self, const std::string& tag = {}) : std::stringstream(), self(self) {}
		ostream_adapter(ostream_adapter&& out) : std::stringstream(std::move(out)), self(out.self) {}
		ostream_adapter(const ostream_adapter&) = delete;
		~ostream_adapter() {
			std::string str = this->str();
			self->async_write(str);
			logger << self->name() << " << " << str;
		}
	private:
		std::shared_ptr<session> self;
	};

	ostream_adapter ostream() { return ostream_adapter(shared_from_this()); }
	ostream_adapter ostream_result() { auto out = ostream(); out << "% "; return out; }
	ostream_adapter ostream_info() { auto out = ostream(); out << "# "; return out; }

	void start() {
		async_read();
	}

private:
	void async_read() {
		auto self(shared_from_this());
		socket_.async_read_some(boost::asio::buffer(data_, sizeof(data_)),
				[this, self](boost::system::error_code ec, std::size_t length) {
					if (!ec) {
						buffer_.append(data_, length);
						auto it = buffer_.find('\n');
						if (it != std::string::npos) {
							super_->handle_command(self, buffer_.substr(0, it));
							buffer_.erase(0, it + 1);
						}
						async_read();
					} else {
						super_->handle_read_error(self, ec);
					}
				});
	}

	void async_write(const std::string& str) {
		auto self(shared_from_this());
		boost::asio::async_write(socket_, boost::asio::buffer(str.c_str(), str.length()), boost::asio::transfer_exactly(str.length()),
			[this, self, str](boost::system::error_code ec, std::size_t length) {
				if (!ec) {
				} else {
					super_->handle_write_error(self, str, ec);
				}
			});
	}

private:
	tcp::socket socket_;
	std::string name_;

	supervisor* super_ = nullptr;
	std::string buffer_;
	char data_[2048];
};

class server : public session::supervisor {
friend class session;
public:
	server(boost::asio::io_context& io_context, unsigned short port) :
			acceptor_(io_context, tcp::endpoint(tcp::v4(), port)) {
		logger << "chat room initialized: " << port << std::endl;
	}

	void async_accept() {
		acceptor_.async_accept(
			[this](boost::system::error_code ec, tcp::socket socket) {
				if (!ec) {
					auto name = "u" + std::to_string(++ticket_);
					auto sess = std::make_shared<session>(std::move(socket), name);
					insert_session(sess);
					on_login(sess);
					sess->start();
				} else {
					logger << boost::format("exception at async_accept: %s") % ec << std::endl;
				}

				async_accept();
			});
	}

protected:
	virtual void handle_command(std::shared_ptr<session> self, const std::string& line) {
		logger << self->name() << " >> " << line << std::endl;

		if (auto it = line.find('<'); it != std::string::npos) { // WHO << MESSAGE
			std::string who = line.substr(0, it);
			std::string msg = line.substr(std::min(line.find_first_not_of('<', it), line.length()));
			boost::trim(who);
			boost::trim(msg);

			std::shared_ptr<session> sess = find_session_by_name(who);
			if (sess) {
				sess->ostream() << boost::format("%s >> %s") % self->name() % msg << std::endl;
			} else {
				self->ostream_result() << boost::format("failed chat: invalid client; %s << %s") % who % msg<< std::endl;
			}
			return;
		}

		std::stringstream parser(line);
		std::string cmd;
		parser >> cmd;

		if (cmd == "name") { // name [NAME]
			std::string name;
			parser >> name;
			std::string old = self->name();

			if (name.empty() || name == old) {
				self->ostream_result() << boost::format("name: %s") % self->name() << std::endl;
				return;
			}

			if (find_session_by_name(name) != nullptr) {
				self->ostream_result() << boost::format("failed name: duplicate") << std::endl;
				return;
			}
			auto legal = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789/_.-";
			if (name.find_first_not_of(legal) != std::string::npos) {
				self->ostream_result() << boost::format("failed name: invalid name") << std::endl;
				return;
			}
			if (rename_session(self, name)) {
				self->ostream_result() << boost::format("name: %s") % self->name() << std::endl;
				for (auto sess : list_sessions()) {
					sess->ostream_info() << boost::format("rename: %s becomes %s") % old % name << std::endl;
				}
			} else {
				self->ostream_result() << boost::format("failed name: unknown") << std::endl;
			}

		} else if (cmd == "who") {
			auto out = self->ostream_result();
			out << "who:";
			for (auto sess : list_sessions()) out << ' ' << sess->name();
			out << std::endl;

		} else if (cmd == "protocol") {
			std::string version = "0";
			parser >> version;

			if (version == "0") {
				self->ostream_result() << boost::format("protocol: %s") % version << std::endl;
			} else {
				self->ostream_result() << boost::format("failed protocol: unsupported") << std::endl;
			}

		}
	}
	virtual void handle_read_error(std::shared_ptr<session> self, boost::system::error_code ec) {
		if (self == find_session_by_name(self->name())) {
			if (ec == boost::system::errc::success || ec == boost::asio::error::eof) {
			} else {
				logger << boost::format("exception at async_read: %s") % ec << std::endl;
			}
			on_logout(self);
		}
	}
	virtual void handle_write_error(std::shared_ptr<session> self, const std::string& str, boost::system::error_code ec) {
		if (self == find_session_by_name(self->name())) {
			if (str.find(" > ") != std::string::npos) {
				std::string src = str.substr(0, str.find(" > "));
				std::string msg = str.substr(str.find(" > ") + 3);
				auto sess = find_session_by_name(src);
				if (sess) {
					sess->ostream_result() << boost::format("failed chat: remote error; %s << %s") % self->name() % msg << std::endl;
				}
			}
			logger << boost::format("exception at async_write: %s; %s") % ec % str << std::endl;
			on_logout(self);
		}
	}

private:
	std::vector<std::shared_ptr<session>> list_sessions() {
		std::lock_guard<std::mutex> lock(sess_mutex_);
		std::vector<std::shared_ptr<session>> sess;
		for (auto& pair : sessions_) sess.push_back(pair.second);
		return sess;
	}
	std::shared_ptr<session> find_session_by_name(const std::string& name) {
		std::lock_guard<std::mutex> lock(sess_mutex_);
		auto it = sessions_.find(name);
		return it != sessions_.end() ? it->second : nullptr;
	}
	bool rename_session(std::shared_ptr<session> sess, const std::string& after) {
		if (sess != find_session_by_name(sess->name())) return false;
		std::lock_guard<std::mutex> lock(sess_mutex_);
		auto hdr = sessions_.extract(sess->name());
		hdr.key() = after;
		sessions_.insert(std::move(hdr));
		sess->name(after);
		return true;
	}
	bool insert_session(std::shared_ptr<session> sess) {
		if (find_session_by_name(sess->name()) != nullptr) return false;
		std::lock_guard<std::mutex> lock(sess_mutex_);
		sessions_.insert({sess->name(), sess});
		sess->super(this);
		return true;
	}
	bool remove_session(std::shared_ptr<session> sess) {
		if (sess != find_session_by_name(sess->name())) return false;
		std::lock_guard<std::mutex> lock(sess_mutex_);
		sessions_.erase(sess->name());
		return true;
	}
	void on_login(std::shared_ptr<session> self) {
		tcp::endpoint endpoint = self->socket().remote_endpoint();
		logger << boost::format("login: %s %s:%d") % self->name() % endpoint.address() % endpoint.port() << std::endl;
		for (auto sess : list_sessions()) {
			sess->ostream_info() << boost::format("login: %s") % self->name() << std::endl;
		}
	}
	void on_logout(std::shared_ptr<session> self) {
		tcp::endpoint endpoint = self->socket().remote_endpoint();
		logger << boost::format("logout: %s %s:%d") % self->name() % endpoint.address() % endpoint.port() << std::endl;
		remove_session(self);
		for (auto sess : list_sessions()) {
			sess->ostream_info() << boost::format("logout: %s") % self->name() << std::endl;
		}
	}

private:

	tcp::acceptor acceptor_;
	std::map<std::string, std::shared_ptr<session>> sessions_;
	std::mutex sess_mutex_;
	size_t ticket_ = 0;
};

int main(int argc, char *argv[]) {
	try {
		boost::asio::io_context io_context;
		server s(io_context, argc < 2 ? 10000 : std::stoul(argv[1]));
		s.async_accept();
		io_context.run();

	} catch (std::exception &e) {
		logger << "exception: " << e.what() << std::endl;
	}

	return 0;
}
