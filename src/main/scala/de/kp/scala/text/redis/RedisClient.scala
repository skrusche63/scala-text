package de.kp.scala.text.redis

import redis.clients.jedis.Jedis
import de.kp.scala.text.Configuration

object RedisClient {

  def apply():Jedis = {

    val (host,port) = Configuration.redis
    new Jedis(host,port.toInt)
    
  }
  
}