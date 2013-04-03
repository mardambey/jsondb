package net.mardambey.jsondb

import java.io.ObjectInputStream
import java.util.concurrent.ConcurrentHashMap
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.util.zip.GZIPOutputStream
import java.util.zip.GZIPInputStream
import scala.collection.JavaConversions._
import org.mapdb.DBMaker
import java.util.concurrent.ConcurrentNavigableMap
import java.io.ByteArrayInputStream
import java.io.File

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
