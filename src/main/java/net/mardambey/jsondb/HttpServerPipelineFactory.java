package net.mardambey.jsondb;

import static org.jboss.netty.channel.Channels.pipeline;

import java.util.Map;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

class HttpServerPipelineFactory implements ChannelPipelineFactory
{
	protected Map< String, HttpServer.RequestHandler > m_mapHandlers;

	public HttpServerPipelineFactory(Map< String, HttpServer.RequestHandler > mapHandlers)
	{
		m_mapHandlers = mapHandlers;
	}

	@Override
	public ChannelPipeline getPipeline() throws Exception
	{
		ChannelPipeline pipeline = pipeline();
		pipeline.addLast("decoder", new HttpRequestDecoder());
		pipeline.addLast("encoder", new HttpResponseEncoder());
		pipeline.addLast("deflater", new HttpContentCompressor());
		pipeline.addLast("handler", new HttpRequestHandler(m_mapHandlers));
		return pipeline;
	}
}
