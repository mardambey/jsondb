package net.mardambey.jsondb

import com.typesafe.config.ConfigException
import scala.collection.JavaConversions._
import java.util.logging.Logger

trait QueryStore {
  
  def getQueries() : Option[Map[String, Query]]
  
  def loadAll()
  
  @throws[UnsupportedOperationException]("if this store can't save queries")
  def save(q:Query) : Boolean
  
  @throws[UnsupportedOperationException]("if this store can't load individual queries")
  def load(alias:String) : Option[Query]
}

class ConfigQueryStore extends QueryStore {
  
  protected val log = Logger.getLogger(getClass.getName)
  protected val QUERY = "query"
  protected val REFRESH = "refresh"
  protected val QUERIES = "jsondb.qstore.queries"
  protected var queries:Option[Map[String, Query]] = None
  
  def getQueries() : Option[Map[String, Query]] = queries
  
  def loadAll() {
    val config = Config().getConfig(QUERIES)

    // get unique key names: alias.query, alias.refresh give alias
    val keys = config.entrySet().map(_.getKey.split("\\.").head)
                 
    queries = Some(keys.map(qKey => {
      val qProps = config.getConfig(qKey)
      log.info("Adding query:%s -> refreshInterval:%s".format(qKey, qProps.getInt(REFRESH)))
      qKey -> new Query(qProps.getString(QUERY), Some(qKey), qProps.getInt(REFRESH))      
    }).toMap[String, Query])
  }
  
  def save(q:Query) : Boolean = {
    throw new UnsupportedOperationException("Can't save queries back into config file.")    
  }
  
  def load(alias:String) : Option[Query] = {
    throw new UnsupportedOperationException("Can't load individual queries from config file.")
  }
}

object QueryStore {
  var storeClass:Option[String] = None
  var store:Option[QueryStore] = None
  
  @throws[ConfigException.WrongType]("if invalid configuration is encountered")
  def init() {    
    storeClass = Some(Config().getString("jsondb.qstore.class"))
    store = Some(Class.forName(storeClass.get).newInstance().asInstanceOf[QueryStore])
    store.get.loadAll()
  }
  
  def apply() = store
}