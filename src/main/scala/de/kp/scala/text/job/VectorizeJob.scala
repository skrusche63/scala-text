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

import org.apache.mahout.text.SequenceFilesFromDirectory
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles

import de.kp.scala.text.model._

/**
 * Vectorization of a corpus of text artifacts is performed 
 * into two different processing modes; one is prior to cluster
 * analysis to prepare the artifacts as TF-IDF vectors for the
 * respective clustering; second is after cluster analysis is
 * done and re-performs vectorization as the starting point of
 * topic modeling on every cluster of text artifacts individually
 * and independently. Clustering is a means to achieve a more
 * homogeneous text corpus with improved topic results.
 */
class VectorizeJob(args:Args) extends Job(args) {

  val conf = new Configuration()
  val fs = FileSystem.get(conf)
  
  override implicit val mode = new Hdfs(true, conf) 

  override def next: Option[Job] = {
    
    val nextStep = args("next")
    nextStep match {
      
      case ProcessStage.NO_STAGE => None
      
      case ProcessStage.TOPIC_MODELING => {
        /* Specify job that follows LDAJob */
        val nextArgs = args + ("next", Some(ProcessStage.TOPIC_DUMPING))
        Some(new LDAJob(nextArgs))
      
      }

      case ProcessStage.CLUSTERING => {
        
        val input  = args("input") + "-vec"
        val output = args("input") + "-cls"
        
        val mode = ProcessMode.WITH_CLUSTERS
        /* Specify job that follows KMeansjob */
        val nextArgs = args + ("input", Some(input)) + ("output", Some(output)) + ("mode", Some(mode)) + ("next", Some(ProcessStage.PARTITIONING))
        Some(new KMeansJob(nextArgs))
    
      }
      
      case _ => None
      
    }
    
  }

  /*
   * Override 'run' avoids errors from cascading that no
   * source and source tap is defined and no processing pipe
   */
  override def run: Boolean = {
    
    val mode = args("mode")
    mode match {
      
      /* 
       * Vectorize a single directory with a flat list of 
       * text artifacts; it is assumed that there is no
       * sub directory structure
       */
      case ProcessMode.WITHOUT_CLUSTERS => withCluster(args)
      
      /*
       * Vectorize a directory with a sub structure defined
       * by the cluster identifiers discovered in a previous
       * clustering job 
       */
      case ProcessMode.WITH_CLUSTERS => withoutCluster(args)
      
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
    vectorize(args,txtfiles)
    
  }
  
  private def withCluster(args:Args) {

    val i = args("input")    
    val input = if (i.endsWith("/")) i.substring(0, i.length - 1) else i

    /* 
	 * The main directory of the clustered text artifacts
	 */
	val groupfiles = new Path(input)

    /*
     * The mechanism below performs vectorization sequentially;
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
            throw new Exception("Directory structure is not appropriate for clustered vectorization.")            
          } else {
            
            val txtfiles = subordinate.getPath()
            vectorize(args,txtfiles)
            
          }
        }
                
      } else {
        throw new Exception("Directory structure is not appropriate for clustered vectorization.")
        
      }
    }
    
  }
    
  private def vectorize(args:Args,txtfiles:Path) {

    val seqfiles = new Path(txtfiles.toString + "-seq")
    val vecfiles = new Path(txtfiles.toString + "-vec")
    
    /*
     * STEP 1: Transform a directory of text artifacts into a directory
     * of Hadoop sequence sequence files; e.g. for clustered artifacts,
     * the following structure is expected:
     * 
     * --+ base
     *   :
     *   :
     *   +---- base/825 (txtfiles)
     *   :
     *   :     This directory holds all text artifacts that refer to 
     *   :     the cluster '825'; this is the input directory of the
     *   :     vectorization job
     *   :
     *   +---- base/825-seq (seqfiles)
     *   :
     *   :
     *   +---- base/825-vec (vecfiles)
     */
    val seqArgs = Array(
    	"-c", "UTF-8",			
    	"-i", txtfiles.toString,
    	"-o", seqfiles.toString
    )

    HadoopUtil.delete(conf, seqfiles)
    new SequenceFilesFromDirectory().run(seqArgs)
		
    val vecArgs = Array(
        /*
         * Text files (directory of) are used as an input for the vectorization
         * algorithm; this algorithm is backed by the respective Mahout drivers
         */
		"-i", seqfiles.toString,
		"-o", vecfiles.toString,
				
		"-x", "70", 
		/* Number of n-grams */
		"-ng", "2",
		/* 
		 * Produce named vectors; the name is the relative document file name   
		 * and is used as a unique identifier for every text artifact
		 */    
		"-nv", 
		/*
		 * Assign a text analyzer to the vectorization job; actually the
		 * Lucene Whitespace Analyzer (Tokenizer) is used. Here, we have
		 * to integrate more sophisticated text analyzers to improve the
		 * result of the overall topic modeling mechanism.
		 */
		"-a",
 		"org.apache.lucene.analysis.core.WhitespaceAnalyzer"				
	)

    HadoopUtil.delete(conf, vecfiles)
	new SparseVectorsFromSequenceFiles().run(vecArgs)
    
  }
}