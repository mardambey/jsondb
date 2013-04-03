package net.mardambey.jsondb

import akka.actor.ActorSystem
import scala.concurrent.duration._
import akka.actor.Props
import akka.actor.Actor
import java.util.logging.Logger

object Scheduler {
  
    implicit val system = ActorSystem("JsonDB")
    import system.dispatcher // ExecutionContext
        
    def schedule(q:Query) = if (q.refreshInterval > 0) {
      system.scheduler.schedule(0 seconds, q.refreshInterval seconds, QueryReloader(), Reload(q))      
      true
    } else false    
}

case class Reload(q:Query)

object QueryReloader {
  implicit val system = ActorSystem("JsonDB")
  val queryReloader = system.actorOf(Props[QueryReloader])
  
  def apply() = queryReloader
}

class QueryReloader extends Actor {
  
  val log = Logger.getLogger(getClass.getName)
  
  def receive = {
    case Reload(q) => {
      log.info("Reloading %s".format(q))
      Database() ! Load(q, refresh = true)
    }
  }
}
