package de.kp.scala.text.sink
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-Text project
* (https://github.com/skrusche63/scala-text).
* 
* Spark-Text is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-Text is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-Text. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import java.util.{Date,UUID}

import de.kp.scala.text.model._
import de.kp.scala.text.io.{ElasticBuilderFactory => EBF,ElasticWriter}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class ElasticSink {

  def addTopics(req:ServiceRequest,topics:Topics) {

    val uid = req.data("uid")
    
    val index   = req.data("index")
    val mapping = req.data("type")
    
    /*
     * Determine timestamp for the actual
     * set of topics to be indexed
     */
    val builder = EBF.createBuilder(mapping)
    val writer = new ElasticWriter()
    
    /* Prepare index and mapping for write */
    val readyToWrite = writer.open(index,mapping,builder)
    if (readyToWrite == false) {
      
      writer.close()
      
      val msg = String.format("""Opening index '%s' and maping '%s' for write failed.""",index,mapping)
      throw new Exception(msg)
      
    }
    
    val now = new Date()
    val timestamp = now.getTime()
   
    // TODO
    
    writer.close()
    
  }

}