package de.kp.scala.text.hadoop
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

import java.io.DataInput
import java.io.DataOutput
import org.apache.hadoop.io.{IntWritable,DoubleWritable,Writable}

class PairWritable extends Writable {

  private var cluster:IntWritable = null
  private var distance:DoubleWritable = null
  
  def this(pair:(Int,Double)) {
    this()
      
    cluster = new IntWritable(pair._1)
    distance = new DoubleWritable(pair._2)

  }

  override def readFields(in:DataInput) {
    
    cluster = new IntWritable()
	cluster.readFields(in)
	
	distance = new DoubleWritable()
    distance.readFields(in)
  }

  override def write(out:DataOutput) {
	cluster.write(out)
	distance.write(out)
  }
	
  def get():(Int,Double) = (cluster.get,distance.get)
  
}