#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include "HttpBase.h"

namespace falcon {

Listener::Listener(boost::asio::io_context& ioc, tcp::endpoint endpoint, HttpHandler h)
	: acceptor(ioc), socket(ioc), handler(h)
{
	boost::system::error_code ec;
	acceptor.open(endpoint.protocol(), ec);
	if (ec)
	{
		LOG(ERROR) << "Failed to open port: " << ec.message();
		return;
	}

	try {
		acceptor.set_option(boost::asio::socket_base::reuse_address(true));
		acceptor.bind(endpoint, ec);
		if (ec)
			throw boost::system::system_error(ec, "Failed to bind port: ");

		acceptor.listen(boost::asio::socket_base::max_listen_connections, ec);
		if (ec)
			throw boost::system::system_error(ec, "Failed to listen port: ");
	}
	catch (boost::system::system_error& err) {
		acceptor.close();
		LOG(ERROR) << err.what() << err.code().message();
	}
	catch (std::exception& ex) {
		acceptor.close();
		LOG(ERROR) << ex.what();
	}
}

void Listener::Accept()
{
	if (!acceptor.is_open())
		return;
	DoAccept();
}

void Listener::Stop()
{
	if (acceptor.is_open())
		acceptor.close();
}

void Listener::DoAccept()
{
	acceptor.async_accept(
		socket,
		std::bind(
			&Listener::OnAccept,
			shared_from_this(),
			std::placeholders::_1));
}

void Listener::OnAccept(boost::system::error_code ec)
{
	if (ec)
	{
		if (ec.value() != ERROR_OPERATION_ABORTED)
			LOG(ERROR) << "Failed to accept connection: " << ec.message();
	}
	else
		std::make_shared<Session>(std::move(socket), handler)->Run();

	if (acceptor.is_open())
		DoAccept();
}

Session::Session(tcp::socket s, HttpHandler h)
	: socket(std::move(s)), strand(socket.get_executor()), lambda_func(*this), handler(h)
{
	remote_address = socket.remote_endpoint().address().to_string();
}

void Session::Run()
{
	DoRead();
}

void Session::DoRead()
{
	// Make the request empty before reading,
	// otherwise the operation behavior is undefined.
	request = {};

	// Read a request
	http::async_read(socket, buffer, request,
		boost::asio::bind_executor(
			strand,
			std::bind(
				&Session::OnRead,
				shared_from_this(),
				std::placeholders::_1,
				std::placeholders::_2)));
}

template<typename Body, typename Allocator, typename Send>
void HandleRequest(const std::string& remote, http::request<Body, http::basic_fields<Allocator>>&& req, Send&& send, HttpHandler handler)
{
	// Returns a bad request response
	auto const bad_request = [&req](boost::beast::string_view why)
	{
		http::response<http::string_body> res{ http::status::bad_request, req.version() };
		res.set(http::field::server, "Falcon");
		res.set(http::field::content_type, "text/html");
		res.keep_alive(req.keep_alive());
		res.body() = why.to_string();
		res.prepare_payload();
		return res;
	};

	// Returns a not found response
	auto const not_found = [&req](boost::beast::string_view target)
	{
		http::response<http::string_body> res{ http::status::not_found, req.version() };
		res.set(http::field::server, "Falcon");
		res.set(http::field::content_type, "text/html");
		res.keep_alive(req.keep_alive());
		res.body() = "The resource '" + target.to_string() + "' was not found.";
		res.prepare_payload();
		return res;
	};

	// Returns a server error response
	auto const server_error = [&req](boost::beast::string_view what)
	{
		http::response<http::string_body> res{ http::status::internal_server_error, req.version() };
		res.set(http::field::server, "Falcon");
		res.set(http::field::content_type, "text/html");
		res.keep_alive(req.keep_alive());
		res.body() = "An error occurred: '" + what.to_string() + "'";
		res.prepare_payload();
		return res;
	};

	// Make sure we can handle the method
	if (req.method() != http::verb::get &&
		req.method() != http::verb::post)
		return send(bad_request("Unsupported HTTP-method"));

	if (req.target().empty())
		return send(bad_request("Illegal request-target"));

	http::status status = http::status::ok;
	std::string response;
	try {
		response = handler(remote, req.method(), std::string(req.target()), req.body(), status);
	} catch (std::exception& ex) {
		return send(server_error(ex.what()));
	}

	http::response<http::string_body> res{ status, 11 };
	res.set(http::field::server, "Falcon");
	res.set(http::field::content_type, "application/json");
	res.keep_alive(req.keep_alive());
	res.body() = response;
	res.prepare_payload();
	return send(std::move(res));
}

void Session::OnRead(boost::system::error_code ec, std::size_t bytes_transferred)
{
	boost::ignore_unused(bytes_transferred);

	if (ec)
		return DoClose();

	HandleRequest(remote_address, std::move(request), lambda_func, handler);
}

void Session::OnWrite(boost::system::error_code ec, std::size_t bytes_transferred, bool close)
{
	boost::ignore_unused(bytes_transferred);

	response = nullptr;
	if (ec || close)
		return DoClose();

	DoRead();
}

void Session::DoClose()
{
	boost::system::error_code ec;
	socket.shutdown(tcp::socket::shutdown_send, ec);
}

}
