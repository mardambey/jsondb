package net.mardambey.jsondb

import akka.actor.Actor
import com.jolbox.bonecp.BoneCPConfig
import com.jolbox.bonecp.BoneCP
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import java.io.Externalizable
import java.io.ObjectOutput
import java.io.ObjectInput
import akka.actor.ActorSystem
import akka.actor.Props
import akka.routing.SmallestMailboxRouter

case class Query(q:String, refreshInterval:Int = 0)
case class Load(q:Query, refresh:Boolean = false)

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
	
  def load(q:Query) : Result = {
    val conn = pool.getConnection()
    val st = conn.createStatement()
    val rs = st.executeQuery(q.q)
  
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

    new Result(colValues.toList)
  }
  
  def receive = {
    case Load(q, true)=> { //refresh
      val r = load(q)
      
    }
    case Load(q, false) => { // don't refresh
      try {
        if (Database.cache.exists(q.hashCode)) {
          
          log.fine("Cache hit: %s".format(q))
		  sender ! Database.cache.get(q.hashCode)
        } else {
          
          val r = load(q)
		  log.fine("caching %s".format(q.hashCode))
		  Database.cache.put(q.hashCode, r)
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
