package net.mardambey.jsondb

import akka.pattern.{ ask, pipe }
import akka.actor._
import akka.routing.SmallestMailboxRouter
import akka.util.Timeout
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import org.jboss.netty.handler.codec.http.HttpRequest
import java.net.URLDecoder
import scala.concurrent.Await
import java.util.concurrent.TimeoutException

/**
 * TODO: parse SQL for better caching
 * TODO: implement cache expiry
 */

object JsonDB extends App {
  val log = java.util.logging.Logger.getLogger(getClass.getName)
  val SQLQuery = """/query\.jsonp?\?sql=(.*)""".r
  val AliasQuery = """/query\.jsonp?\?alias=(.*)""".r
  val QSTORE_CLASS = "jsondb.qstore.class"
  val HTTPD_PORT = 8080
  implicit val timeout = Timeout(600 seconds)
  
  log.info("Checking configuration for QueryStore implementation")
  
  if (Config().hasPath(QSTORE_CLASS)) {
    val c = Config().getString(QSTORE_CLASS) 
    log.info("Found QueryStore class: %s".format(c))
    
    try {
      QueryStore.init()
      QueryStore().get.getQueries().flatMap(Scheduler.schedule(_))
    } catch {
      case e:Exception => log.severe("Failed while loading QueryStore configuration: %s - %s\n%s".format(e.getClass, e.getMessage, e.getStackTraceString))
    }
  }
      
  log.info("Binding Http server to port %s".format(HTTPD_PORT))
  
  val httpd = new HttpServer(HTTPD_PORT)
  	.addHandler("/query.jsonp", new net.mardambey.jsondb.HttpServer.RequestHandler() {
  	  def handle(req:HttpRequest) : String = "jsonp(%s)".format(handleRequest(req))
    })
    .addHandler("/query.json", new net.mardambey.jsondb.HttpServer.RequestHandler() {
	  def handle(req:HttpRequest) : String = handleRequest(req)
	})
	
  httpd.start()
  
  def handleRequest(req:HttpRequest) : String = try {
    req.getUri() match {
      case SQLQuery(q) => {
        val future = Database() ? Load(Query(Some(URLDecoder.decode(q, "UTF-8")), alias=None, refreshInterval=0), refresh = false)
        val result = Await.result(future, timeout.duration).asInstanceOf[Option[Result]]
        result.getOrElse(EmptyResult()).toJson
      }
      
      case AliasQuery(q) => {
        val future = Database() ? Load(Query(None, alias=Some(q), refreshInterval=0), refresh = false)
        val result = Await.result(future, timeout.duration).asInstanceOf[Option[Result]]
        result.getOrElse(EmptyResult()).toJson
      }
      
      case u => """"["unknown":"%s"]""".format(u)
    }
  } catch { 
    case e:TimeoutException => {
      log.warning("Query taking more than 600 seconds: %s".format(req.getUri))
      """"["Query taking more than 600 seconds": "%s"]""".format(req.getUri)
    }
  }
}	
