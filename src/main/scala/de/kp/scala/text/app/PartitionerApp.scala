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
import de.kp.scala.text.job.PartitionerJob

object PartitionerApp {
  
  def main(args : Array[String]) {

    val input = "/Work/cxense/text"
    val output = "/Work/cxense/group"
      
    val cluster = "Work/cxense/text-cls/final/clusteredPoints/part-m-00000"

    val params = List(
        /*
         * A directory that holds the corpus of text artifactes
         * as a flat list of files; there is no sub directory
         * structure assumed
         */
        "--input",  input,
        /*
         * A directory that holds all text artifacts that refer to
         * the same cluster centroid in the same sub directory; it
         * is the output directory of the partitioner job
         */
        "--output", output,
        "--cluster",cluster
    )
    
    val args = Args(params)
    
    val start = System.currentTimeMillis()
    
    val job = new PartitionerJob(args)
    job.run
    
    val end = System.currentTimeMillis()           
    println("Total time: " + (end-start) + " ms")
    
  }

}