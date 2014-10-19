package de.kp.scala.text.app

import com.twitter.scalding.{Args,Job}
import de.kp.scala.text.model.ProcessMode
import de.kp.scala.text.job.VectorizeJob

object DetectApp {
  
  def main(args : Array[String]) {

    val input = "/Work/cxense/text"
    
    val params = List(
        
        /* Directory that holds text artifacts */
        "--input", input,
        
        /* Text Analysis always starts with mode = 'WITHOUT_CLUSTERS' */        
        "--mode",  ProcessMode.WITHOUT_CLUSTERS,
        
        /* Clustering parameters */
 
        /* convergence delta */
        "--delta", "0.01",
        /* number of iterations */
        "--iter", "20",

        /* Topic modeling parameters */
        "--maxIter",    "1", 
        "--num_topics", "50",
        
        /* Topic dumping parameters */
        "--num_terms", "5" 
     
    )
    
    val args = Args(params)
    detect(args)
    
  }

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