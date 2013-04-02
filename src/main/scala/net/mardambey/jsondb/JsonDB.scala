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
import java.io.Externalizable
import java.io.ObjectOutput
import java.io.ObjectInput
import scala.collection.mutable.ListBuffer
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.core.`type`.TypeReference

/**
 * TODO: return JSON or JSONP
 * TODO: parse SQL for better caching
 * TODO: implement cache expiry
 * TODO: stored queries (user gives in query alias)
 * TODO: automatically refreshable stored queries
 * TODO: optimize Result object
 * TODO: serialize calls to the same query / alias
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
  
  val log = java.util.logging.Logger.getLogger(getClass.getName)
  
  protected val cacheDbFile = Config().getString("jsondb.db.file") match {
    case null => "/tmp/jsondb.dat"
    case s => s
  }
  
  protected val cacheDb = DBMaker.newFileDB(new File(cacheDbFile))
  .cacheLRUEnable()
  .compressionEnable()
  .checksumEnable()
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
      case e:Exception => log.severe("Could not add to cache %s: %s\n%s".format(e.getClass(), e.getMessage(), e.getStackTraceString)) 
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
  
  protected val log = java.util.logging.Logger.getLogger(getClass.getName)
  
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
      case e:Exception => log.severe("Could not add to cache %s %s".format(e.getMessage(), e.getStackTraceString)) 
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
          log.fine("Cache hit: %s".format(q))
		  sender ! Database.cache.get(q.hashCode)
        } else {          
		  val conn = pool.getConnection()
		  val st = conn.createStatement()
		  val rs = st.executeQuery(q)
		  
		  val rsmd = rs.getMetaData()
		  
		  val colNames = ListBuffer[String]()
		  val colValues = ListBuffer[List[String]]()
		  		  		  
		  for (i <- 1 to rsmd.getColumnCount()) {			    
		      colNames.add(rsmd.getColumnLabel(i))
		  }
		  
		  colValues.add(colNames.toList)
		  		
		  log.fine(colNames.mkString(","))
		  
		  try {
			  while(rs.next()) {
			    val row = new ListBuffer[String]()
			    
			    try {
				    for (i <- 1 to rsmd.getColumnCount()) {			      
				      val v = rs.getString(i)
				      row.add(if (v == null) "" else v)
				    }
				    
				    colValues.add(row.toList)
			    } catch {
			      case _:Throwable =>
			    }
			  }
		  } catch {
		    case e:Exception => log.severe("Error fetching next row: %s\n%s".format(e.getMessage, e.getStackTraceString))
		  }
		  
		  val r = new Result(colValues.toList)
		  log.fine("caching %s".format(q.hashCode))
		  Database.cache.put(q.hashCode,r)
		  log.fine("caching %s done".format(q.hashCode))
		  sender ! r
		
        }      
      } catch {
		case e:Exception => log.severe("Could not execute query: %s\n%s".format(e.getMessage, e.getStackTrace().mkString("\n")))
      }
	}
				
    case x => log.warning("Received unknown message %s".format(x))
  }
}

object Json {
  
  val log = java.util.logging.Logger.getLogger(getClass.getName)
  
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  
  def toJson(o:Any) : String = {
    mapper.writeValueAsString(o)
  }
  
  def fromJson(json:String, clz:Class[_]) : Any = {
    mapper.readValue(json, clz)
  }
  
  def toJsonAsBytes(o:Any) : Array[Byte] = {
    log.fine("writing json: %s".format(toJson(o)))
    toJson(o).getBytes("UTF-8")
  }
  
  def fromJsonBytes(bytes:Array[Byte], clz:Class[_]) : Any = {
    log.fine("deserializing: %s".format(new String(bytes, "UTF-8")))
    fromJson(new String(bytes, "UTF-8"), clz)
  }
}

@serializable class Result(var data:List[List[String]]) extends Externalizable {
  
  val log = java.util.logging.Logger.getLogger(getClass.getName)

  val BYTE_SIZE = 4
  
  def this() {
    this(List[List[String]]())
  }
  
  def toJson() : String = {
    Json.toJson(data)
  }
    
  def writeExternal(out:ObjectOutput) {
    log.fine("serializing data=%s".format(data))
    val b = Json.toJsonAsBytes(data)
    log.fine("writing byte[] of length %s".format(b.length))
    out.writeInt(b.length)
    out.write(b)
  }
  
  def readExternal(in:ObjectInput) {
    
    val size = in.readInt()
    data = if (size <= 0) {
      List[List[String]]()
    } else {      
      log.fine("reading byte[] of length %s".format(size))
      val bytes = new Array[Byte](size)
      in.readFully(bytes)
        
      try {
        Json.fromJsonBytes(bytes, classOf[List[List[String]]]).asInstanceOf[List[List[String]]]
      } catch {
        case e:Exception => {
          log.severe("Could not desrialize object from the cache %s: %s\n%s"
              .format(e.getMessage, e.getClass, e.getStackTraceString))
          List[List[String]]()
        }
      }
    }
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
