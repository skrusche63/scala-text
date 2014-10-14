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
import org.apache.hadoop.fs.{FileSystem,Path}

import org.apache.mahout.common.HadoopUtil

import org.apache.mahout.utils.vectors.RowIdJob
import org.apache.mahout.clustering.lda.cvb.CVB0Driver

import de.kp.scala.text.model.ProcessMode

class LDAJob(args:Args) extends Job(args) {

  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  override implicit val mode = new Hdfs(true, conf) 
  /*
   * Override 'run' avoids errors from cascading that no
   * source and source tap is defined and no processing pipe
   */
  override def run: Boolean = {

    val mode = args("mode")
    mode match {
      
      /* 
       * Train from a single directory with a flat list of 
       * text artifacts; it is assumed that there is no
       * sub directory structure
       */
      case ProcessMode.WITH_CLUSTERS => withCluster(args)
      
      /*
       * Train from a directory with a sub structure defined
       * by the cluster identifiers discovered in a previous
       * clustering job 
       */
      case ProcessMode.WITHOUT_CLUSTERS => withoutCluster(args)
      
      case _ => throw new Exception("Process mode '" + mode + "' is not supported.")
      
    }
    
    true
    
  }

  private def withoutCluster(args:Args) {
    
    val i = args("input")    
    val input = if (i.endsWith("/")) i.substring(0, i.length - 1) else i
    /* 
	 * The directory of the text artifacts; it is expected
	 * that this is a flat list of files with no subordinate
	 * directory structure
	 */
	val txtfiles = new Path(input)
    topics(args,txtfiles)
    
  }
  
  private def withCluster(args:Args) {

    val i = args("input")    
    val input = if (i.endsWith("/")) i.substring(0, i.length - 1) else i

    /* 
	 * The main directory of the clustered text artifacts
	 */
	val groupfiles = new Path(input)
    /*
     * The mechanism below performs topic modeling sequentially;
     * this is replaced soon by Akka actors to enable parallel
     * processing
     */
    val superiors = fs.listStatus(groupfiles)    
    for (i <-0 until superiors.length) {
      
      val superior = superiors(i)
      if (superior.isDir() == true) {
        
        /*
         * The vectorization is perform for every 
         * sub directory (or cluster) individually
         */
        val subordinates = fs.listStatus(superior.getPath())
        for (j <- 0 until subordinates.length) {
          
          val subordinate = subordinates(j)
          if (subordinate.isDir) {
            throw new Exception("Directory structure is not appropriate for clustered topic modeling.")            
          } else {
            
            val txtfiles = subordinate.getPath()
            topics(args,txtfiles)
            
          }
        }
                
      } else {
        throw new Exception("Directory structure is not appropriate for clustered topic modeling.")
        
      }
    }
    
  }
    
  private def topics(args:Args,txtfiles:Path) {

    /*
     * Directory that holds the results of the vectorization
     * job; we actually used the TFIDF vectors as an input
     * for topic modeling
     */
    val vecfiles = new Path(txtfiles.toString + "-vec")
    /*
     * The output directory of the RowId job
     */
    val rowfiles = new Path(txtfiles.toString + "-row")
    HadoopUtil.delete(conf, rowfiles)

    val rowArgs = Array(
      /*
       * TF-IDF vectors are the default input of the topic modeling 
       * algorithm; as an alternative also TF vectors may be used
       */
      "-i", vecfiles.toString + "/tfidf-vectors",
      "-o", rowfiles.toString
    )

    new RowIdJob().run(rowArgs)
    /*
     * Invoke Mahout's CVB0 algorithm to build the topic model
     */
    val ldafiles = new Path(txtfiles.toString + "-lda")
    HadoopUtil.delete(conf, ldafiles)

    val dicfile  = new Path(vecfiles.toString() + "/dictionary.file-0")

    /*
     * Dictionary holds the association between documents and topics
     */
    val docfiles = new Path(txtfiles.toString + "-doc")
    HadoopUtil.delete(conf, docfiles)
    
    val tmpfiles  = new Path(txtfiles.toString + "-tmp");
    HadoopUtil.delete(conf, tmpfiles)

	val cvbArgs = Array(			
	  /* Document to term matrix */
	  "--input",  rowfiles.toString() + "/matrix",  
	  "--output", ldafiles.toString(),
	  /* Term dictionary holds term to vector position */
	  "--dictionary", dicfile.toString(),
      "--topic_model_temp_dir", tmpfiles.toString,
      "--doc_topic_output",docfiles.toString, 

      /* Default values  */
      "--convergenceDelta",     "0.0", 
      "--doc_topic_smoothing",  "0.0001", 
      "--term_topic_smoothing", "0.0001",
      "--iteration_block_size", "10", 
      "--num_train_threads",    "4", 
      "--num_update_threads",   "1", 
      "--max_doc_topic_iters",  "10", 
      "--num_reduce_tasks",     "10", 
      "--test_set_fraction",    "0.0", 

      /* Dynamic variables */
      "--maxIter",    args("max_iter"), 
      "--num_topics", args("num_topics") 
	)
	
	new CVB0Driver().run(cvbArgs)
    
  }

}