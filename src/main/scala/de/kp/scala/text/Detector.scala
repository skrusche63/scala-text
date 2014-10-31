package de.kp.scala.text
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
import com.twitter.scalding.{Args,Job}

import de.kp.scala.text.model._
import de.kp.scala.text.job.VectorizeJob

import de.kp.scala.text.redis.RedisCache

class Detector {
  
  def detect(args:Args) {

    val start = System.currentTimeMillis()
    
    println("Text processing started...")
    
    val job = new VectorizeJob(args)
    val res = job.run
    
    if (res == true) {
      
      val uid = args("uid")
      
      println(job.getClass().getName() + " sucessfully performed.")
      executeNext(job,start,uid)
      
    } else {

      println(job.getClass().getName() + " failed. Text processing has stopped.")

      val end = System.currentTimeMillis()           
      println("Total time: " + (end-start) + " ms")
    
    }
    
    
  }

  private def executeNext(job:Job,start:Long,uid:String) {
      
    job.next match {
        
      case None => {
        /*
         * No further processing stage
         */        
        println("Text processing successfully finished.")
        
        /* Determine output directory */
        val args = job.args
        
        val input = args("input")
        val mode  = args("mode")
        
        val dir = mode match {
          /*
           * In case of a clustered job; the directory is set
           * to the one that holds the textual artifacts; this
           * mechanism helps to distinguish between the other
           * process mode by simply evaluating the postfix
           */
          case ProcessMode.WITH_CLUSTERS => input
          case ProcessMode.WITHOUT_CLUSTERS => input + "-out"
          
          case _ => null
          
        }
        
        /* Add reference to output directory to RedisCache */
        if (dir != null) RedisCache.addTopics(uid,dir)
        
        /* Finally update status */
        RedisCache.addStatus(uid,"train",TextStatus.FINISHED)
        
        val end = System.currentTimeMillis()           
        println("Total time: " + (end-start) + " ms")
        
      }
        
      case Some(nextJob) => {

        val res = nextJob.run
        if (res == true) {

          println(job.getClass().getName() + " sucessfully performed.")
          executeNext(job,start,uid)
          
        } else {

          println(job.getClass().getName() + " failed. Text processing has stopped.")

          val end = System.currentTimeMillis()           
          println("Total time: " + (end-start) + " ms")
          
        }

      }
    
    } 
  
  }
}