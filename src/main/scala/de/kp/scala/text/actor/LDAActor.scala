package de.kp.scala.text.actor
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

import com.twitter.scalding.Args

import de.kp.scala.text.Detector

import de.kp.scala.text.model._
import de.kp.scala.text.redis.RedisCache

class LDAActor extends BaseActor {
 
  def receive = {

    case req:ServiceRequest => {
      
      val params = properties(req)
      val missing = (params == null)
      
      /* Send response to originator of request */
      sender ! response(req, missing)

      if (missing == false) {
        /* Register status */
        RedisCache.addStatus(req,TextStatus.STARTED)
 
        try {          
          new Detector().detect(params)
        
        } catch {
          case e:Exception => RedisCache.addStatus(req,TextStatus.FAILURE)          
        }

      }
      
      context.stop(self)
      
    }
    
    case _ => {
      
      log.error("Unknown request.")
      context.stop(self)
      
    }
    
  }
  
  private def properties(req:ServiceRequest):Args = {
    
    /*
     * Text Analysis requires a dynamically provided path to the 
     * input data directory where the text artifacts to be evaluated
     * are persisted
     */
    val data = req.data
    
    /* 
     * The request must specify where to retrieve the textual artifacts
     * from; actually the respective data source is the HDFS file system
     */
    if (data.contains("path") == false) {
      return null
    }
    
    val input = data("path")
    
    /*
     * The request initiates a complete run and must therefore always
     * start with process mode = 'WITHOUT_CLUSTERS'
     */
    val mode = ProcessMode.WITHOUT_CLUSTERS
    
    /*
     * The request does not require clustering parameters; if these 
     * parameters are not set, default values are used
     */
    val delta = if (data.contains("delta")) data("delta") else "0.01"
    val iter  = if (data.contains("iter")) data("iter") else "20"
    
    /*
     * The request does not require topic modeling parameters; if these
     * parameters are not set, default values are used
     */
    val max_iter =  if (data.contains("max_iter")) data("max_iter") else "1"
    val num_topics =  if (data.contains("num_topics")) data("num_topics") else "50"
    
    /*
     * The request does not require topic dumping parameters; if these parameters
     * are not set, default values are used
     */
    val num_terms =  if (data.contains("num_terms")) data("num_terms") else "5"
    val params = List(
        /*
         * The unique identifier of the respective machine learning job; 
         * this identifier must be used to retrieve the status as well as
         * the result of the job
         */
        "--uid",data("uid"),
        
        /* Directory that holds text artifacts */
        "--input", input,
        
        /* Text Analysis always starts with mode = 'WITHOUT_CLUSTERS' */        
        "--mode",  mode,
        
        /* Clustering parameters */
 
        /* convergence delta */
        "--delta", delta,
        /* number of iterations */
        "--iter", iter,

        /* Topic modeling parameters */
        "--maxIter", max_iter, 
        "--num_topics", num_topics,
        
        /* Topic dumping parameters */
        "--num_terms", num_terms
        
    )   
      
    Args(params)
    
  }
  
  private def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data("uid")
    
    if (missing == true) {
      val data = Map("uid" -> uid, "message" -> Messages.MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,TextStatus.FAILURE)	
  
    } else {
      val data = Map("uid" -> uid, "message" -> Messages.CONTENT_DETECTION_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,TextStatus.STARTED)	
  
    }

  }

}