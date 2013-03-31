package net.mardambey.jsondb

import akka.pattern.{ ask, pipe }
import akka.actor._
import akka.routing.SmallestMailboxRouter
import akka.util.Timeout

import com.jolbox.bonecp.BoneCP
import com.jolbox.bonecp.BoneCPConfig

import net.mardambey.jsondb.HttpServer.RequestHandler

import org.jboss.netty.handler.codec.http.HttpRequest

import scala.concurrent.duration._
import scala.collection.JavaConversions._
import scala.concurrent.Await

import java.net.URLDecoder
import java.util.LinkedList
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeoutException

case class Query(q:String)

object Database {
  val cache = new ConcurrentHashMap[Int, Result]()
	val system = ActorSystem("JsonDB")
	val WORKERS_MAX_NUM = 5
	val database = system.actorOf(Props[Database].withRouter(SmallestMailboxRouter(nrOfInstances=WORKERS_MAX_NUM)))
	
	sys.addShutdownHook {
		system.shutdown()
	}
	
	def apply() = database
}

class Database extends Actor {
  val log = java.util.logging.Logger.getLogger(getClass.getName)
  Class.forName("com.mysql.jdbc.Driver")
	
	val config = new BoneCPConfig()	
	config.setJdbcUrl("jdbc:mysql://aspen.evo:3306/general?zeroDateTimeBehavior=round&amp;jdbcCompliantTruncation=false")
	config.setUsername("slave")
	config.setPassword("password")
	config.setMinConnectionsPerPartition(5)
	config.setMaxConnectionsPerPartition(10)
	config.setPartitionCount(1)
	
	val pool = new BoneCP(config)
	
	def receive = {
	  case Query(q) => {
			try {

			  if (Database.cache.containsKey(q.hashCode)) {
				  println("Cache hit: %s".format(q))
				  sender ! Database.cache(q.hashCode)
			  } else {			  
			  println("cache=%s".format(Database.cache.keySet.mkString(",")))
			  val conn = pool.getConnection()
			  val st = conn.createStatement()
			  val rs = st.executeQuery(q)
			  
			  val rsmd = rs.getMetaData()
			  
			  val colNames = new LinkedList[String]()
			  val colValues = new LinkedList[LinkedList[String]]()
			  
			  colValues.append(colNames)
			  
			  for (i <- 1 to rsmd.getColumnCount()) {			    
			      colNames.add(rsmd.getColumnLabel(i))
			  }
			  		
			  println(colNames.mkString(","))
			  while(rs.next()) {
			    val row = new LinkedList[String]()
			    
			    try {
				    for (i <- 1 to rsmd.getColumnCount()) {			      
				      val v = rs.getString(i)
				      row.add(if (v == null) "" else v)
				    }
				    
				    colValues.add(row)
			    } catch {
			      case _:Throwable =>
			    }
			  }
			  
			  val r = new Result(colValues)
			  println("caching %s".format(q.hashCode))
			  Database.cache += (q.hashCode -> r)
			  sender ! r
			  }
			} catch {
  				case e:Exception => log.severe("Could not execute query: %s\n%s".format(e.getMessage, e.getStackTrace().mkString("\n")))
			}
		}
				
		case x => log.warning("Received unknown message %s".format(x))
	}
}

class Result(data:LinkedList[LinkedList[String]]) {
  def toJson() : String = {
    "[" + data.map(row => {
      "[" + row.map(v => """"""" + v.replace(""""""", """\"""") + """"""").mkString(",") + "]"
    }).mkString(",\n") + "]"    
  }
}

object JsonDB extends App {
  val log = java.util.logging.Logger.getLogger(getClass.getName)
  val SQLQuery = """/query\?sql=(.*)""".r
  implicit val timeout = Timeout(600 seconds)
  
  val httpd = new HttpServer(8080)
  	httpd.addHandler("/query", new RequestHandler() {
  	  
	  def handle(req:HttpRequest) : String = {
	    try {
		    req.getUri() match {
		      case SQLQuery(q) => {
		        val future = Database() ? Query(URLDecoder.decode(q, "UTF-8"))
		        val result = Await.result(future, timeout.duration).asInstanceOf[Result]
		        "jsonp(%s)".format(result.toJson)	        
		      }
		      case u => "unknown: %s".format(u)
		    }
	    } catch {
	      case e:TimeoutException => {
	        log.warning("Query taking more than 600 seconds: %s".format(req.getUri))
	        "Query taking more than 600 seconds: %s".format(req.getUri)
	      }
	    }
	  }	   
	})
	
	httpd.start()
}
