package de.kp.scala.text.io
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

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

object ElasticBuilderFactory {
  /*
   * Definition of supported topic parameters
   */
  val TIMESTAMP_FIELD:String = "timestamp"

  /*
   * The unique identifier of the mining task that created the
   * respective topics
   */
  val UID_FIELD:String = "uid"

  /*
   * The unique identifier of the cluster or other
   * grouping mechanism the respective text document
   * is assigned to
   */  
  val GROUP_FIELD:String = "group"

  /*
   * The unique identifier of the text document that is
   * described by the extracted topics
   */  
  val DOC_FIELD:String = "doc"

    /*
   * The unique identifier of the topic assigned to the
   * respective text document 
   */  
  val TOPIC_FIELD:String = "topic"

    /*
   * The score characterizes the relevance of the topic
   * for the respective text document
   */  
  val SCORE_FIELD:String = "score"

  /*
   * The terms that specify the respective topic
   */  
  val TERM_FIELD:String = "term"
    
  def createBuilder(mapping:String):XContentBuilder = {
    /*
     * Define mapping schema for index 'index' and 'type'; note, that
     * we actually support the following common schema for rule and
     * also series analysis: timestamp, site, user, group and item.
     * 
     * This schema is compliant to the actual transactional as well
     * as sequence source in spark-arules and spark-fsm
     */
    val builder = XContentFactory.jsonBuilder()
                      .startObject()
                      .startObject(mapping)
                        .startObject("properties")

                          /* timestamp */
                          .startObject(TIMESTAMP_FIELD)
                            .field("type", "long")
                          .endObject()

                          /* uid */
                          .startObject(UID_FIELD)
                            .field("type", "string")
                            .field("index", "not_analyzed")
                          .endObject()

                          /* group */
                          .startObject(GROUP_FIELD)
                            .field("type", "integer")
                          .endObject()
                          
                          /* doc */
                          .startObject(DOC_FIELD)
                            .field("type", "string")
                            .field("index", "not_analyzed")
                          .endObject()
                          
                          /* topic */
                          .startObject(TOPIC_FIELD)
                            .field("type", "integer")
                          .endObject()
                          
                          /* score */
                          .startObject(SCORE_FIELD)
                            .field("type", "double")
                          .endObject()

                          /* term */
                          .startObject(TERM_FIELD)
                            .field("type", "string")
                          .endObject()
                        
                        .endObject() // properties
                      .endObject()   // mapping
                    .endObject()
                    
    builder

  }
  
}