package de.kp.scala.text.sink

import de.kp.scala.text.model._
import de.kp.scala.text.redis.RedisClient

import scala.collection.JavaConversions._

class RedisSink {

  val client  = RedisClient()

  /*
   * This method adds a document to the Redis instance; note, that
   * the documents are persisted as a list 
   */
  def addDocument(uid:String,document:Document) {
    
    val k = "doc:text:" + uid + ":" + document.group.toString
    val v = Serializer.serializeDocument(document)
    
    client.rpush(k,v)
    
  }
  
  def documentsExist(uid:String,group:Int):Boolean = {
    
    val k = "doc:text:" + uid + ":" + group.toString
    client.exists(k)
    
  }
  
  def documentsTotal(uid:String,group:Int):Long = {

    val k = "doc:text:" + uid + ":" + group.toString
    client.llen(k)
    
  }
  
  def documents(uid:String,group:Int,start:Long,end:Long):String = {
    
    val k = "doc:text:" + uid + ":" + group.toString
    val documents = client.lrange(k, start, end)
    
    documents.mkString("[",",","]")
    
  }
  
  /*
   * A group is an identifier of a cluster of text artifacts
   * that have a similar content
   */
  def addGroup(uid:String,group:Int) {

    val k = "group:text:" + uid
    val v = group.toString
    
    client.rpush(k,v)
    
  }
 
  def groups(uid:String):List[Int] = {

    val k = "group:text:" + uid
    val groups = client.lrange(k, 0, -1)

    if (groups.size() == 0) {
      return List.empty[Int]
    
    } else 
      groups.map(_.toInt).toList
    
  }
  
  def groupsExist(uid:String):Boolean = {

    val k = "group:text:" + uid
    client.exists(k)
    
  }
  
}