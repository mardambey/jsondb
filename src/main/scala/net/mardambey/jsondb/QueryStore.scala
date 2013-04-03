package net.mardambey.jsondb

trait QueryStore {
  def loadAll() : Map[String, Query]
  
  @throws[UnsupportedOperationException]("if this store can't save queries")
  def save(q:Query) : Boolean
  
  def load(alias:String) : Option[Query]
}

class ConfigQueryStore extends QueryStore {
  def loadAll() : Map[String, Query] = {
    Map[String, Query]()
  }
  
  def save(q:Query) : Boolean = {
    throw new UnsupportedOperationException("Can't save queries back into config file.")    
  }
  
  def load(alias:String) : Option[Query] = {
    None
  }
}

object QueryStore {

}