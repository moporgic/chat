#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <map>
#include <cctype>
#include <mutex>
#include <memory>
#include <functional>
#include <boost/asio.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>

using boost::asio::ip::tcp;

class session: public std::enable_shared_from_this<session> {
public:
	session(tcp::socket socket, std::string name) : socket_(std::move(socket)), name_(name) {}

	void start() {
		tcp::endpoint endpoint = socket_.remote_endpoint();
		std::cerr << "session opened: " << endpoint.address() << ':' << endpoint.port() << std::endl;

		for (auto sess : super_->list_sessions()) {
			sess->ostream() << "# client " << this->name() << " logged in" << std::endl;
		}

		do_read();
	}

	const std::string& name() const { return name_; }
	void name(const std::string& name) { name_ = name; }

	class supervisor {
	public:
		virtual std::vector<std::shared_ptr<session>> list_sessions() = 0;
		virtual std::shared_ptr<session> find_session_by_name(const std::string&) = 0;
		virtual bool rename_session(std::shared_ptr<session>, const std::string&) = 0;
		virtual bool insert_session(std::shared_ptr<session>) = 0;
		virtual bool remove_session(std::shared_ptr<session>) = 0;
	};

	supervisor* super() { return super_; }
	void super(supervisor* s) { super_ = s; }

	class ostream_adapter {
	public:
		ostream_adapter(std::shared_ptr<session> self) : self(self) {}
		ostream_adapter(const ostream_adapter&) = delete;
		ostream_adapter(ostream_adapter&&) = default;
		~ostream_adapter() { self->do_write(buf.str(), hdr); }

		ostream_adapter& handler(std::function<void(boost::system::error_code, std::size_t)> hdr = {}) { this->hdr = hdr; return *this; }

		template<typename type>
		std::ostream& operator <<(const type& t) { return buf << t; }
	private:
		std::shared_ptr<session> self;
		std::function<void(boost::system::error_code, std::size_t)> hdr;
		std::stringstream buf;
	};

	ostream_adapter ostream() { return ostream_adapter(shared_from_this()); }

private:
	void handle_command(std::string line) {
		if (line.find('<') != std::string::npos) { // WHO < MESSAGE
			std::string who = line.substr(0, line.find('<'));
			std::string message = line.substr(line.find('<') + 1);
			boost::trim(who);
			boost::trim(message);

			auto self = shared_from_this();
			std::shared_ptr<session> sess = super_->find_session_by_name(who);
			if (sess) {
				sess->ostream().handler([this, self, who, message](boost::system::error_code ec, std::size_t length) {
					if (!ec) {
					} else {
						self->ostream() << boost::format("# unable to chat with %s: %s") % who % message << std::endl;
					}

				}) << this->name() << " > " << message << std::endl;
			} else {
				this->ostream() << "# " << who << " is invalid" << std::endl;
			}

		} else {

			std::stringstream parser(line);
			std::string cmd;
			parser >> cmd;

			if (cmd == "name") { // name [NAME]
				std::string name;
				parser >> name;
				try {
					std::string old = this->name();
					if (name.size() && name != old) {
						if (super_->find_session_by_name(name) != nullptr)
							throw std::runtime_error("duplicate");
						const char* legal = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789/_.-";
						if (name.find_first_not_of(legal) != std::string::npos)
							throw std::runtime_error("invalid character " + std::string(1, name[name.find_first_not_of(legal)]));

						auto self(shared_from_this());
						super_->rename_session(self, name);

						ostream() << "% using name " << this->name() << std::endl;
						for (auto sess : super_->list_sessions()) {
							if (sess != self)
								sess->ostream() << "# client " << old << " renamed as " << name << std::endl;
						}
					} else {
						ostream() << "% using name " << this->name() << std::endl;
					}

				} catch (std::exception& e) {
					ostream() << "% unable to name: " << e.what() << std::endl;
				}
			} else if (cmd == "who") {
				auto sessions = super_->list_sessions();
				std::stringstream sess_list;
				for (auto sess : sessions) {
					tcp::endpoint endpoint = sess->socket_.remote_endpoint();
					sess_list << "% " << sess->name() << ' ' << endpoint.address() << ':' << endpoint.port() << std::endl;
				}
				ostream() << "% " << sessions.size() << " online clients" << std::endl << sess_list.str();
			} else if (cmd == "protocol") {
				std::string version = "0";
				parser >> version;

				if (version == "0") {
					ostream() << "% using protocol " << version << std::endl;
				} else {
					ostream() << "% unable to use protocol: unsupported" << std::endl;
				}
			} else if (cmd == "help") {

			}

		}
	}

	void do_read() {
		auto self(shared_from_this());
		socket_.async_read_some(boost::asio::buffer(data_, sizeof(data_)),
				[this, self](boost::system::error_code ec, std::size_t length) {
					if (!ec) {
						buffer_.append(data_, length);
						auto it = buffer_.find('\n');
						if (it != std::string::npos) {
							handle_command(buffer_.substr(0, it));
							buffer_.erase(0, it + 1);
						}
						do_read();
					} else if (self == super_->find_session_by_name(self->name())) {
						if (ec == boost::system::errc::success || ec == boost::asio::error::eof) {
							tcp::endpoint endpoint = self->socket_.remote_endpoint();
							std::cerr << "session closed: " << endpoint.address() << ':' << endpoint.port() << std::endl;
						} else {
							std::cerr << "exception: " << ec << std::endl;
						}
						super_->remove_session(self);

						for (auto sess : super_->list_sessions()) {
							sess->ostream() << "# client " << self->name() << " logged out" << std::endl;
						}
					}
				});
	}

	void do_write(const std::string& str, std::function<void(boost::system::error_code, std::size_t)> hdr = {}) {
		auto self(shared_from_this());
		boost::asio::async_write(socket_, boost::asio::buffer(str.c_str(), str.length()), boost::asio::transfer_exactly(str.length()),
			[this, self, str, hdr](boost::system::error_code ec, std::size_t length) {
				if (hdr) hdr(ec, length);
				if (!ec && str.length() == length) {
				} else if (self == super_->find_session_by_name(self->name())) {
					std::cerr << "exception: " << ec << std::endl;
					super_->remove_session(self);
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
public:
	server(boost::asio::io_context &io_context, short port) :
			acceptor_(io_context, tcp::endpoint(tcp::v4(), port)) {
		std::cerr << "chat room: " << port << std::endl;
		do_accept();
	}

public:
	virtual std::vector<std::shared_ptr<session>> list_sessions() {
		std::vector<std::shared_ptr<session>> sess;
		for (auto& pair : sessions) sess.push_back(pair.second);
		return sess;
	}
	virtual std::shared_ptr<session> find_session_by_name(const std::string& name) {
		auto it = sessions.find(name);
		return it != sessions.end() ? it->second : nullptr;
	}
	virtual bool rename_session(std::shared_ptr<session> sess, const std::string& after) {
		if (sess != find_session_by_name(sess->name())) return false;
		auto hdr = sessions.extract(sess->name()); // TODO: lock client list?
		hdr.key() = after;
		sessions.insert(std::move(hdr));
		sess->name(after);
		return true;
	}
	virtual bool insert_session(std::shared_ptr<session> sess) {
		if (find_session_by_name(sess->name()) != nullptr) return false;
		sessions.insert({sess->name(), sess});
		sess->super(this);
		return true;
	}
	virtual bool remove_session(std::shared_ptr<session> sess) {
		if (sess != find_session_by_name(sess->name())) return false;
		sessions.erase(sess->name());
		return true;
	}

private:
	void do_accept() {
		acceptor_.async_accept(
			[this](boost::system::error_code ec, tcp::socket socket) {
				if (!ec) {
					static size_t ticket = 0;
					auto name = "u" + std::to_string(++ticket);
					auto sess = std::make_shared<session>(std::move(socket), name);
					insert_session(sess);
					sess->start();
				} else {
					// TODO: print error
				}

				do_accept();
			});
	}

	tcp::acceptor acceptor_;
	std::map<std::string, std::shared_ptr<session>> sessions;
};

int main(int argc, char *argv[]) {
	try {

		boost::asio::io_context io_context;

		server s(io_context, argc < 2 ? 10000 : std::stoul(argv[1]));

		io_context.run();

	} catch (std::exception &e) {
		std::cerr << "Exception: " << e.what() << "\n";
	}

	return 0;
}
