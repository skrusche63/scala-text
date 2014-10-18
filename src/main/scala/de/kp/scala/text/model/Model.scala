package de.kp.scala.text.model
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

case class Pair(cluster:Int, distance:Double)

object ProcessMode {
  
  /*
   * Text processing decides to different processing nodes:
   * 
   * WITHOUT_CLUSTERS: perform text analysis on a flat list 
   *                   of text artifacts
   *                   
   * WITH_CLUSTERS: perform text analysis on a structured or
   *                clusttered corpus of text artifacts; the
   *                respective clusters or groups are part of
   *                the directory name; ie.g. text artifacts
   *                that refer to the same cluster are grouped
   *                in the same HDFS directory
   */
  val WITH_CLUSTERS:String    = "WITH_CLUSTERS"
  val WITHOUT_CLUSTERS:String = "WITHOUT_CLUSTERS"

}

object ProcessStage {
  
  val CLUSTERING:String     = "CLUSTERING"
    
  val PARTITIONING:String   = "PARTITIONING"
  
  val TOPIC_DUMPING:String  = "TOPIC_DUMPING"  
  val TOPIC_MODELING:String = "TOPIC_MODELING"
  
  val VECTORIZTAION:String  = "VECTORIZATION"
    
  val NO_STAGE:String = "NO_STAGE"
  
}
