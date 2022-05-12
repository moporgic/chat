#include <iostream>
#include <string>
#include <thread>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;



void echo(tcp::iostream&& stream) {
	try {
		tcp::endpoint endpoint = stream.socket().remote_endpoint();
		std::cerr << "session opened: " << endpoint.address() << ':' << endpoint.port() << std::endl;

		for (char ch; stream.read(&ch, 1); stream.write(&ch, 1));

		boost::system::error_code ec = stream.error();
		if (ec == boost::asio::error::eof) {
			std::cerr << "session closed: " << endpoint.address() << ':' << endpoint.port() << std::endl;
		} else {
			throw boost::system::system_error(ec);
		}
	} catch (const std::exception& e) {
		std::cerr << "exception: " << e.what() << std::endl;
	}
}

int main(int argc, char *argv[]) {
	try {
		boost::asio::io_context io_context;
		tcp::endpoint endpoint(tcp::v4(), argc < 2 ? 10000 : std::stoul(argv[1]));
		tcp::acceptor acceptor(io_context, endpoint);

		std::cerr << "echo server: " << endpoint.address() << ':' << endpoint.port() << std::endl;

		while (true) {
			tcp::iostream stream;
			acceptor.accept(stream.socket());

			boost::asio::socket_base::keep_alive keep_alive(true);
			stream.socket().set_option(keep_alive);

			std::thread(&echo, std::move(stream)).detach();
		}

	} catch (const std::exception& e) {
		std::cerr << "exception: " << e.what() << std::endl;
	}

	return 0;
}
