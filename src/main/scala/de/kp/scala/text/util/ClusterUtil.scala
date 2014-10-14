package de.kp.scala.text.util
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

import org.apache.hadoop.mapred.{JobConf,RecordReader,OutputCollector}
import org.apache.hadoop.io.{IntWritable,Text}

import cascading.flow.hadoop.HadoopFlowProcess
import cascading.scheme.hadoop.WritableSequenceFile

import cascading.tap.Tap
import cascading.tap.hadoop.Hfs
import cascading.tuple.Fields

import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable
import org.apache.mahout.math.NamedVector

import de.kp.scala.text.model._
import scala.collection.mutable.{ArrayBuffer,HashMap}

class ClusterUtil {
 
  type HadoopTap = Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]

  def index(input:String, jobConf:JobConf):Map[String,Pair] = {
		
    val scheme = new WritableSequenceFile(new Fields("ix","vector"),classOf[IntWritable],classOf[WeightedPropertyVectorWritable]) //new CSequenceFile(new Fields("ix","vector"))
    val tap = new Hfs(scheme,input).asInstanceOf[HadoopTap]

	val iter = new HadoopFlowProcess(jobConf).openTapForRead(tap)

	val values = HashMap.empty[String,Pair]
	
	while (iter.hasNext()) {
	  	
	  val entry = iter.next()
	  
      val k = entry.getObject(0).asInstanceOf[IntWritable]
      val v = entry.getObject(1).asInstanceOf[WeightedPropertyVectorWritable]
	  
	  
      val weight = v.getWeight
      val props  = v.getProperties
      val vector = v.getVector.asInstanceOf[NamedVector]        

      val distance = props.get(new Text("distance")).toString.toDouble
      val name = vector.getName().replace("/","").replace(".cx","")
        
      values += name -> Pair(k.get(), distance)
 
	}

	iter.close()
	values.toMap
	
  }
  /**
   * Retrieve unique list of cluster identifier
   */
  def clusters(input:String, jobConf:JobConf):List[Int] = {
		
    val scheme = new WritableSequenceFile(new Fields("ix","vector"),classOf[IntWritable],classOf[WeightedPropertyVectorWritable]) //new CSequenceFile(new Fields("ix","vector"))
    val tap = new Hfs(scheme,input).asInstanceOf[HadoopTap]

	val iter = new HadoopFlowProcess(jobConf).openTapForRead(tap)

	val values = ArrayBuffer.empty[Int]
	
	while (iter.hasNext()) {
	  	
	  val entry = iter.next()
	  
      val k = entry.getObject(0).asInstanceOf[IntWritable]
      values += k.get()
      
	}

	iter.close()
    values.toList.distinct
    
  }

}