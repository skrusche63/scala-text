package de.kp.scala.text.job
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
import com.twitter.scalding._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem,FileUtil,Path}

import org.apache.hadoop.mapred.JobConf

import org.apache.mahout.common.HadoopUtil

import de.kp.scala.text.util.ClusterUtil
import de.kp.scala.text.model._

class PartitionerJob(args:Args) extends Job(args) {

  val conf = new Configuration()
  override implicit val mode = new Hdfs(true, conf) 

  override def next: Option[Job] = {
    
    val nextStep = args("next")
    nextStep match {
      
      case ProcessStage.NO_STAGE => None
      
      case ProcessStage.VECTORIZATION => {
        /* Specify job that follows VectorizeJob */
        val nextArgs = args + ("next", Some(ProcessStage.TOPIC_MODELING))
        Some(new VectorizeJob(nextArgs))
      
      }
      
      case _ => None
      
    }
    
  }
  /*
   * Override 'run' avoids errors from cascading that no
   * source and source tap is defined and no processing pipe
   */
  override def run: Boolean = {
    
    val i = args("input")    
    val input = if (i.endsWith("/")) i.substring(0, i.length - 1) else i

    val o = args("output")    
    val output = if (o.endsWith("/")) o.substring(0, o.length - 1) else o

    /*
     * Retrieve list of cluster identifier from
     * previous KMeans clustering job
     */
    val cluster = args("cluster")
    val index = clusters(cluster)
    
    val fs = FileSystem.get(conf)

    /* 
	 * The directory of the text artifacts
	 */
	val txtfiles = new Path(input)

	val groupfiles = new Path(output)
    HadoopUtil.delete(conf,groupfiles)
    
    /*
     * Create directory for all grouped text artifacts
     */
    val res = fs.mkdirs(groupfiles)
    if (res == false) {
      throw new Exception("Directory for clustered files cannot be created.")
    }
    
    val status = fs.listStatus(txtfiles)
    
    for (i <-0 until status.length) {
      
      val stat = status(i)
      val infile = stat.getPath()
      
      val id = infile.getName().replace(".cx","")
      index.get(id) match {
        
        case None => throw new Exception("Document '" + infile.getName() + "' has to valid cluster assigned.")
        case Some(pair) => {
          
          val cls = pair.cluster.toString
          val dir = new Path(groupfiles.toString + "/" + cls)
          
          if (fs.exists(dir) == false) fs.mkdirs(dir)
          
          val outfile = new Path(dir.toString + "/" + infile.getName())
          FileUtil.copy(fs, infile, fs, outfile, false, conf)
          
        }
      }
      
    }
    true
  }
  
  private def clusters(path:String):Map[String,Pair] = {
 
    val jobConf = new JobConf()
    new ClusterUtil().index(path,jobConf)

  }
  
}