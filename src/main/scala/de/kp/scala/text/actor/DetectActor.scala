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

import akka.actor.{Actor,ActorLogging}

import com.twitter.scalding.Args

import de.kp.scala.text.Detector

import de.kp.scala.text.model._
import de.kp.scala.text.redis.RedisCache

class DetectActor extends Actor with ActorLogging {
 
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
      
    try {

      // TODO
      
      return null
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
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