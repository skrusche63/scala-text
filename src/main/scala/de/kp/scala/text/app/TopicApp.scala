package de.kp.scala.text.app
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
import de.kp.scala.text.model.ProcessMode
import de.kp.scala.text.job.TopicJob

object TopicApp {
  
  def main(args : Array[String]) {

    val input = "/Work/cxense/text"
    
    val params = List(
        "--input", input,
        "--mode",  ProcessMode.WITHOUT_CLUSTERS,
        "--num_terms", "5" 
    )
    val args = Args(params)
    
    val start = System.currentTimeMillis()
    
    val extractor = new TopicJob(args)
    extractor.run
    
    val end = System.currentTimeMillis()           
    println("Total time: " + (end-start) + " ms")
    
  }

}
