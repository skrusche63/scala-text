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

import de.kp.scala.text.model.ProcessMode
import de.kp.scala.text.job.VectorizeJob

class Detector {
  
  def detect(args:Args) {

    val start = System.currentTimeMillis()
    
    println("Text processing started...")
    
    val job = new VectorizeJob(args)
    val res = job.run
    
    if (res == true) {

      println(job.getClass().getName() + " sucessfully performed.")
      executeNext(job,start)
      
    } else {

      println(job.getClass().getName() + " failed. Text processing has stopped.")

      val end = System.currentTimeMillis()           
      println("Total time: " + (end-start) + " ms")
    
    }
    
    
  }

  private def executeNext(job:Job,start:Long) {
      
    job.next match {
        
      case None => {
        /*
         * No further processing stage
         */
        println("Text processing successfully finished.")
        
        val end = System.currentTimeMillis()           
        println("Total time: " + (end-start) + " ms")
        
      }
        
      case Some(nextJob) => {

        val res = nextJob.run
        if (res == true) {

          println(job.getClass().getName() + " sucessfully performed.")
          executeNext(job,start)
          
        } else {

          println(job.getClass().getName() + " failed. Text processing has stopped.")

          val end = System.currentTimeMillis()           
          println("Total time: " + (end-start) + " ms")
          
        }

      }
    
    } 
  
  }
}