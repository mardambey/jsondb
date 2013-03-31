package net.mardambey.jsondb;

import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpRequest;

public class HttpServer
{
	protected Integer m_intPort;

	@SuppressWarnings("unchecked")
	protected Map< String, HttpServer.RequestHandler > m_mapHandlers = new ConcurrentSkipListMap< String, HttpServer.RequestHandler >(new Comparator()
	{
		@Override
		public int compare(Object o1, Object o2)
		{
			// force everything to the end of the list
			return 1;
		}

	});

	public HttpServer(Integer intPort)
	{
		m_intPort = intPort;
	}

	public HttpServer start()
	{
		// Configure the server.
		ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));

		// Set up the event pipeline factory.
		bootstrap.setPipelineFactory(new HttpServerPipelineFactory(m_mapHandlers));

		// Bind and start to accept incoming connections.
		bootstrap.bind(new InetSocketAddress(m_intPort));
		
		return this;
	}

	public interface RequestHandler
	{
		public String handle(HttpRequest request);
	}
	
	public HttpServer addHandler(String strRoute, HttpServer.RequestHandler handler)
	{
		m_mapHandlers.put(strRoute, handler);
		return this;
	}

	public Map< String, HttpServer.RequestHandler > getHandlers()
	{
		return m_mapHandlers;
	}
}
