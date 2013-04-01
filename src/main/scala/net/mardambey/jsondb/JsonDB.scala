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
import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream
import java.io.ObjectInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream
import java.io.ObjectOutputStream
import java.util.zip.DeflaterOutputStream
import java.util.zip.InflaterInputStream
import org.mapdb.DBMaker
import java.io.File
import java.util.concurrent.ConcurrentNavigableMap
import com.typesafe.config.ConfigFactory

/**
 * TODO: add config file
 * TODO: return JSON or JSONP
 * TODO: parse SQL for better caching
 * TODO: implement cache expiry
 * TODO: stored queries (user gives in query alias)
 * TODO: refreshable stored queries
 */

case class Query(q:String)

object Config {
  protected val config = ConfigFactory.load()
  
  def apply() = config
}

trait Cache {
  def put(key:Int, r:Result)
  def get(key:Int) : Result
  def exists(key:Int) : Boolean
  def shutdown() {}
}

class DiskCache extends Cache {
  
  protected val cacheDbFile = Config().getString("jsondb.db.file") match {
    case null => "/tmp/jsondb.dat"
    case s => s
  }
  
  protected val cacheDb = DBMaker.newFileDB(new File(cacheDbFile))
  .cacheLRUEnable()
  .compressionEnable()
  .closeOnJvmShutdown()               
  .make()
               
  protected val cache = cacheDb.getTreeMap("jsondb-cache").asInstanceOf[ConcurrentNavigableMap[Int,Array[Byte]]]
  
  def put (key:Int, r:Result) {
    val baos = new ByteArrayOutputStream();
    val oos = new ObjectOutputStream(baos);
    try {
    oos.writeObject(r)
    oos.close()    
    cache += (key -> baos.toByteArray())
    cacheDb.commit()
    } catch {
      case e:Exception => println("could not add to cache %s %s".format(e.getMessage(), e.getStackTraceString)) 
    }
  }
  
  def get(key:Int) : Result = {
    val bytes = cache.get(key)
    bytes match {
      case null => null
      case _ => {
        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
        ois.readObject().asInstanceOf[Result]
      }
    }
  }
  
  def exists(key:Int) : Boolean = cache.containsKey(key)
  
  override def shutdown() {
    cacheDb.close()
  }
}

class InMemoryCache extends Cache {
  
  protected val cache = new ConcurrentHashMap[Int, Array[Byte]]()
  
  def put (key:Int, r:Result) {
    val baos = new ByteArrayOutputStream();
    val oos = new ObjectOutputStream(new GZIPOutputStream(baos));
    try {
    oos.writeObject(r)
    oos.close()
    cache += (key -> baos.toByteArray())
    }
    catch {
      case e:Exception => println("could not add to cache %s %s".format(e.getMessage(), e.getStackTraceString)) 
    }
  }
  
  def get(key:Int) : Result = {
    val bytes = cache.get(key)
    
    bytes match {
      case null => null
      case _ => {
        val ois = new ObjectInputStream(new GZIPInputStream(new ByteArrayInputStream(bytes)));
        ois.readObject().asInstanceOf[Result]
      }
    }
  }
  
  def exists(key:Int) : Boolean = cache.containsKey(key)  
}

object Database {
  val cache = new DiskCache()
  val system = ActorSystem("JsonDB")
  val WORKERS_MAX_NUM = 5
  val database = system.actorOf(Props[Database].withRouter(SmallestMailboxRouter(nrOfInstances=WORKERS_MAX_NUM)))
	
  sys.addShutdownHook {
	  system.shutdown()
	  cache.shutdown()
  }
	
  def apply() = database
}

class Database extends Actor {
    
  val log = java.util.logging.Logger.getLogger(getClass.getName)
  
  Class.forName("com.mysql.jdbc.Driver")
  
  val config = new BoneCPConfig()
  config.setJdbcUrl(Config().getString("jsondb.db.url"))
  config.setUsername(Config().getString("jsondb.db.username"))
  config.setPassword(Config().getString("jsondb.db.password"))
  config.setMinConnectionsPerPartition(5)
  config.setMaxConnectionsPerPartition(10)
  config.setPartitionCount(1)
	
  val pool = new BoneCP(config)
	
  def receive = {
    case Query(q) => {
      try {
        if (Database.cache.exists(q.hashCode)) {
          println("Cache hit: %s".format(q))
		  sender ! Database.cache.get(q.hashCode)
        } else {          
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
		  
		  try {
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
		  } catch {
		    case e:Exception => log.severe("Error fetching next row: %s\n%s".format(e.getMessage, e.getStackTraceString))
		  }
		  
		  val r = new Result(colValues)
		  println("caching %s".format(q.hashCode))
		  Database.cache.put(q.hashCode,r)
		  println("caching %s done".format(q.hashCode))
		  sender ! r
		
        }      
      } catch {
		case e:Exception => log.severe("Could not execute query: %s\n%s".format(e.getMessage, e.getStackTrace().mkString("\n")))
      }
	}
				
    case x => log.warning("Received unknown message %s".format(x))
  }
}

@serializable class Result(data:LinkedList[LinkedList[String]]) {
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
