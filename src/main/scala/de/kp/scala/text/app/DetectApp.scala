package de.kp.scala.text.app

import com.twitter.scalding.Args

import de.kp.scala.text.model._
import de.kp.scala.text.Detector

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
    new Detector().detect(args)
    
  }

}