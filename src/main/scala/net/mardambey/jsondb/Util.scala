package net.mardambey.jsondb

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.ConfigFactory

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

object Config {
  protected val config = ConfigFactory.load()
  
  def apply() = config
}

