package net.mardambey.jsondb;

import static org.jboss.netty.handler.codec.http.HttpHeaders.is100ContinueExpected;
import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.COOKIE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.SET_COOKIE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.Map;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.Cookie;
import org.jboss.netty.handler.codec.http.CookieDecoder;
import org.jboss.netty.handler.codec.http.CookieEncoder;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;

class HttpRequestHandler extends SimpleChannelUpstreamHandler
{
	protected Map< String, HttpServer.RequestHandler > m_mapHandlers;
	private HttpRequest request;
	private boolean readingChunks;

	/** Buffer that stores the response content */
	private final StringBuilder buf = new StringBuilder();

	public HttpRequestHandler(Map< String, HttpServer.RequestHandler > mapHandlers)
	{
		m_mapHandlers = mapHandlers;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
	{
		boolean isJson = true;
		
		if (!readingChunks)
		{
			HttpRequest request = this.request = (HttpRequest) e.getMessage();
			if (is100ContinueExpected(request))
			{
				send100Continue(e);
			}

			buf.setLength(0);

			for (Map.Entry< String, HttpServer.RequestHandler > entry : m_mapHandlers.entrySet())
			{
				String strUrl = entry.getKey();

				if (!request.getUri().startsWith(strUrl))
				{
					continue;
				}

				HttpServer.RequestHandler handler = entry.getValue();
				buf.append(handler.handle(request));
				break;
			}

			if (buf.toString().startsWith("<html>"))
			{
				isJson = false;
			}
			if (request.isChunked())
			{
				readingChunks = true;
			}
			else
			{
				writeResponse(e, isJson);
			}
		}
		else
		{
			HttpChunk chunk = (HttpChunk) e.getMessage();
			if (chunk.isLast())
			{
				readingChunks = false;
				writeResponse(e, isJson);
			}
		}
	}

	private void writeResponse(MessageEvent e, boolean isJson)
	{
		// Decide whether to close the connection or not.
		boolean keepAlive = isKeepAlive(request);

		// Build the response object.
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
		response.setContent(ChannelBuffers.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
		if (isJson)
		{
			response.setHeader(CONTENT_TYPE, "application/javascript");
		}
		else
		{
			response.setHeader(CONTENT_TYPE, "text/html; charset=UTF-8");
		}

		if (keepAlive)
		{
			// Add 'Content-Length' header only for a keep-alive connection.
			response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());
		}

		// Encode the cookie.
		String cookieString = request.getHeader(COOKIE);
		if (cookieString != null)
		{
			CookieDecoder cookieDecoder = new CookieDecoder();
			Set< Cookie > cookies = cookieDecoder.decode(cookieString);
			if (!cookies.isEmpty())
			{
				// Reset the cookies if necessary.
				CookieEncoder cookieEncoder = new CookieEncoder(true);
				for (Cookie cookie : cookies)
				{
					cookieEncoder.addCookie(cookie);
				}
				response.addHeader(SET_COOKIE, cookieEncoder.encode());
			}
		}

		// Write the response.
		ChannelFuture future = e.getChannel().write(response);

		// Close the non-keep-alive connection after the write operation is
		// done.
		if (!keepAlive)
		{
			future.addListener(ChannelFutureListener.CLOSE);
		}
	}

	private void send100Continue(MessageEvent e)
	{
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, CONTINUE);
		e.getChannel().write(response);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
	{
		Channel ch = e.getChannel();
		Throwable cause = e.getCause();
		if (cause instanceof TooLongFrameException)
		{
			sendError(ctx, BAD_REQUEST);
			return;
		}

		cause.printStackTrace();
		if (ch.isConnected())
		{
			sendError(ctx, INTERNAL_SERVER_ERROR);
		}

	}

	private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status)
	{
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
		response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
		response.setContent(ChannelBuffers.copiedBuffer("Failure: " + status.toString() + "\r\n", CharsetUtil.UTF_8));

		// Close the connection as soon as the error message is sent.
		try
		{
			ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
		}
		catch(Exception e)
		{	
		}
	}
}
