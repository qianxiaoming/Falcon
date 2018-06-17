#ifndef FALCON_HTTP_BASE_H
#define FALCON_HTTP_BASE_H

#include <cstdlib>
#include <algorithm>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <boost/smart_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/function.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/config.hpp>

namespace falcon {

using tcp = boost::asio::ip::tcp;
namespace http = boost::beast::http;

typedef boost::function<std::string (http::verb, 
	                                 const std::string&,
	                                 const std::string&,
	                                 http::status&)> HttpHandler;

class Listener : public std::enable_shared_from_this<Listener>
{
public:
	Listener(boost::asio::io_context& ioc, tcp::endpoint endpoint, HttpHandler handler);

	void Accept();

	void Stop();

public:
	void DoAccept();

	void OnAccept(boost::system::error_code ec);

	bool IsListening() const { return acceptor.is_open(); }

private:
	tcp::acceptor acceptor;
	tcp::socket   socket;
	HttpHandler   handler;
};
typedef std::shared_ptr<Listener> ListenerPtr;

class Session : public std::enable_shared_from_this<Session>
{
public:
	explicit Session(tcp::socket socket, HttpHandler handler);

	void Run();

	void DoRead();

	void OnRead(boost::system::error_code ec, std::size_t bytes_transferred);

	void OnWrite(boost::system::error_code ec, std::size_t bytes_transferred, bool close);

	void DoClose();

private:
	struct SendLambdaFunc
	{
		Session& self;

		explicit SendLambdaFunc(Session& s) : self(s) { }

		template<bool is_request, class Body, class Fields>
		void operator()(http::message<is_request, Body, Fields>&& msg) const
		{
			auto sp = std::make_shared<http::message<is_request, Body, Fields>>(std::move(msg));
			self.response = sp;

			http::async_write(
				self.socket,
				*sp,
				boost::asio::bind_executor(
					self.strand,
					std::bind(
						&Session::OnWrite,
						self.shared_from_this(),
						std::placeholders::_1,
						std::placeholders::_2,
						sp->need_eof())));
		}
	};
	SendLambdaFunc lambda_func;

	tcp::socket socket;
	boost::asio::strand<boost::asio::io_context::executor_type> strand;
	boost::beast::flat_buffer buffer;
	http::request<http::string_body> request;
	std::shared_ptr<void> response;

	HttpHandler handler;
};

}

#endif
