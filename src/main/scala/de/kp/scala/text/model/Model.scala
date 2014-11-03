package de.kp.scala.text.model
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
 * 
 * This file is part of the Scala-Text project
 * (https://github.com/skrusche63/scala-text).
 * 
 * Scala-Text is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * Scala-Text is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with
 * Scala-Text. 
 * 
 * If not, see <http://www.gnu.org/licenses/>.
 */

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

case class ServiceRequest(
  service:String,task:String,data:Map[String,String]
)
case class ServiceResponse(
  service:String,task:String,data:Map[String,String],status:String
)

case class Document(
    /* 
     * Unique identifier to distinguish a certain document from
     * all other documents of a corpus
     */
    uid:String,
    /*
     * Unique identifier of a text cluster or group that is used
     * to gather documents that have similar content
     */
    group:Int,
    /*
     * Unique identifier of the topic that semantic describes the
     * respective document; the value is used to distinguish topics
     */
    topic:Int,
    /*
     * Score (0..1) describes the semantic relevance of the topic
     * for the respective document
     */
    score:Double,
    /*
     * A list of terms that specifies a certain topic
     */
    terms:List[String]
)

/*
 * Service requests are mapped onto job descriptions and are stored
 * in a Redis instance
 */
case class JobDesc(
  service:String,task:String,status:String
)

case class Pair(cluster:Int, distance:Double)

case class Topics()

object ProcessMode {
  
  /*
   * Text processing decides to different processing nodes:
   * 
   * WITHOUT_CLUSTERS: perform text analysis on a flat list 
   *                   of text artifacts
   *                   
   * WITH_CLUSTERS: perform text analysis on a structured or
   *                clusttered corpus of text artifacts; the
   *                respective clusters or groups are part of
   *                the directory name; ie.g. text artifacts
   *                that refer to the same cluster are grouped
   *                in the same HDFS directory
   */
  val WITH_CLUSTERS:String    = "WITH_CLUSTERS"
  val WITHOUT_CLUSTERS:String = "WITHOUT_CLUSTERS"

}

object ProcessStage {
  
  val CLUSTERING:String     = "CLUSTERING"
    
  val PARTITIONING:String   = "PARTITIONING"
  
  val TOPIC_DUMPING:String  = "TOPIC_DUMPING"  
  val TOPIC_MODELING:String = "TOPIC_MODELING"
  
  val VECTORIZATION:String  = "VECTORIZATION"
    
  val NO_STAGE:String = "NO_STAGE"
  
}

object Serializer {
    
  implicit val formats = Serialization.formats(NoTypeHints)

  def serializeDocument(document:Document):String = write(document)

  /*
   * Support for serialization and deserialization of job descriptions
   */
  def serializeJob(job:JobDesc):String = write(job)

  def deserializeJob(job:String):JobDesc = read[JobDesc](job)
  
  /*
   * Support for serialization of a service response and deserialization
   * of a certain serice request
   */
  def serializeResponse(response:ServiceResponse):String = write(response)
  
  def deserializeRequest(request:String):ServiceRequest = read[ServiceRequest](request)
  
}

object Algorithms {
  /*
   * Latent Dirichlet Allocation (LDA) is used in combination
   * with KMEANS to detect semantic topics from heterogeneous
   * textual artifacts
   */
  val LDA:String = "LDA"
  /*
   * Factorization Machines(FM) are used in combination with
   * KMEANS clustering to predict semantic concepts 
   */
  val FM:String = "FM"
    
  private val algorithms = List(FM,LDA)
  
  def isAlgorithm(algorithm:String):Boolean = algorithms.contains(algorithm)
  
}

object Sources {
  
  val HDFS:String = "HDFS"
  val ELASTIC:String = "ELASTIC" 

  private val sources = List(ELASTIC,HDFS)
  
  def isSource(source:String):Boolean = sources.contains(source)
  
}

object Messages {

  def ALGORITHM_IS_UNKNOWN(uid:String,algorithm:String):String = String.format("""Algorithm '%s' is unknown for uid '%s'.""", algorithm, uid)

  def CONTENT_DETECTION_STARTED(uid:String):String = String.format("""Content detection started for uid '%s'.""", uid)

  def GENERAL_ERROR(uid:String):String = String.format("""A general error appeared for uid '%s'.""", uid)

  def INVALID_GROUP_PROVIDED(uid:String):String = String.format("""Invalid group provided for uid '%s'.""", uid)
  
  def MISSING_PARAMETERS(uid:String):String = String.format("""Missing parameters for uid '%s'.""", uid)
 
  def NO_ALGORITHM_PROVIDED(uid:String):String = String.format("""No algorithm provided for uid '%s'.""", uid)

  def NO_GROUP_PROVIDED(uid:String):String = String.format("""No group provided for uid '%s'.""", uid)

  def NO_PARAMETERS_PROVIDED(uid:String):String = String.format("""No parameters provided for uid '%s'.""", uid)

  def NO_SOURCE_PROVIDED(uid:String):String = String.format("""No source provided for uid '%s'.""", uid)

  def REQUEST_IS_UNKNOWN():String = String.format("""Unknown request.""")

  def SOURCE_IS_UNKNOWN(uid:String,source:String):String = String.format("""Source '%s' is unknown for uid '%s'.""", source, uid)

  def TASK_ALREADY_STARTED(uid:String):String = String.format("""The task with uid '%s' is already started.""", uid)

  def TASK_DOES_NOT_EXIST(uid:String):String = String.format("""The task with uid '%s' does not exist.""", uid)

  def TASK_IS_UNKNOWN(uid:String,task:String):String = String.format("""The task '%s' is unknown for uid '%s'.""", task, uid)
 
  def TOPICS_DO_NOT_EXIST(uid:String):String = String.format("""Topics do not exist for uid '%s'.""", uid)

}

object TextStatus {
    
  val STARTED:String = "started"
  val FINISHED:String = "finished"
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
    
}