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

import com.twitter.scalding.{Args, Job, TextLine}
import com.twitter.scalding._

import cascading.pipe.joiner._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem,Path}

import org.apache.hadoop.io.{IntWritable,Text}
import org.apache.mahout.math.VectorWritable

import de.kp.scala.text.model.ProcessMode

import scala.collection.mutable.{ArrayBuffer,HashMap}

/**
 * The TopicJob is performed after the LDAJob and describes for
 * every text artifact in the text corpus the most relevant topic 
 * and the most relevant terms to characterize the topic.
 */
class TopicJob(args:Args) extends Job(args) {
    
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
       * Vectorize a single directory with a flat list of 
       * text artifacts; it is assumed that there is no
       * sub directory structure
       */
      case ProcessMode.WITH_CLUSTERS => withCluster(args)
      
      /*
       * Vectorize a directory with a sub structure defined
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
            throw new Exception("Directory structure is not appropriate for clustered topic retrieval.")            
          } else {
            
            val txtfiles = subordinate.getPath()
            topics(args,txtfiles)
            
          }
        }
                
      } else {
        throw new Exception("Directory structure is not appropriate for clustered topic retrieval.")
        
      }
    }
    
  }
    
  private def topics(args:Args,txtfiles:Path) {

    /*
     * The number of terms indicate how many most relevant
     * terms will be used to specify a certain topic
     * 
     */
    val num_terms = args("num_terms").toInt
    
    /*
     * Extract the document index from HDFS which associates an 
     * integer (ix) with the respective document identifier (id)
     */
    val ixfile = txtfiles.toString + "-row/docIndex"
    val docPos = WritableSequenceFile(ixfile,('ix,'id)).read
      .mapTo(('ix,'id) -> ('ix,'id)) {
        value:(IntWritable,Text) => {
      
          val ix = value._1     
          /*
           * Remove prefix and suffix to extract the
           * original document identifier
           */
          val id = value._2.toString.replace("/","").replace(".cx","")
          (ix,id)
        
      }

    }.rename('ix -> 'ix1)

    /*
     * Assign the topic with the maximum support to the respective document;
     * the maximum support is interpreted as the most relevant topic of the
     * document.
     * 
     * This strategy may be change to set of topics that are e.g. above an
     * external defined threshold
     */
    val docfile = txtfiles.toString + "-doc/part-m-00000"
    val docTopic = WritableSequenceFile(docfile,('ix,'vector)).read
      .joinWithSmaller('ix -> 'ix1,docPos, joiner=new OuterJoin).discard('ix1)
      .mapTo(('id,'ix,'vector) -> ('id,'topic,'score)) {
        value:(String,IntWritable,VectorWritable) => {
        
          val id = value._1
          val vector = value._3.get()
        
          val elements = Array.fill[Double](vector.size())(0.0)
          for (i <- 0 until vector.size()) {
            elements(i) = vector.get(i)
          }

          val max = elements.max
          val topic = new IntWritable(elements.indexOf(max))
        
          (id,topic,max)

        }
     
      }

    val dicfile  = txtfiles.toString + "-vec/dictionary.file-0"
    val dictionary = WritableSequenceFile(dicfile,('term,'pos)).read
      .mapTo(('term,'pos) -> ('pos,'term)) {
        value:(Text,IntWritable) => {
        
          val term = value._1.toString
          val pos  = value._2
        
          (pos,term)
        
        }
    
      }
    
    /*
     * Read topic model from HDFS: this model has the topic identifier
     * as key and the respective distribution of terms as value.
     * 
     * Assign term or word position to a certain document and the respective 
     * topic (by identifier); the number of words are actually restricted to
     * the most relevant ones
     */
    val ldafile = txtfiles.toString + "-lda//part-m-00000"  
    val docTopicWordPos = WritableSequenceFile(ldafile,('ix,'terms)).read
      .joinWithSmaller('ix -> 'topic, docTopic,joiner=new OuterJoin).discard('topic)
      .flatMapTo(('ix,'terms,'id,'score) -> ('id,'topic,'score,'pos1)) {
         value:(IntWritable,VectorWritable,String,Double) => {
    
           val topic = value._1.get()
           val terms = value._2.get()
         
           val id = value._3
           val score = value._4

           val elements = HashMap.empty[Int,Double]
           for (i <- 0 until terms.size()) {
             elements += i -> terms.get(i)
           }
         
           val words = elements.toList.sortBy(_._2).reverse.take(num_terms)
           words.map(word => {
             (id,topic,score,new IntWritable(word._1))
           
           })

         }
       
      }
    
    val docTopicWords = docTopicWordPos.joinWithLarger('pos1 -> 'pos,dictionary)
      .mapTo(('id,'topic,'score,'pos,'pos1,'term) -> ('id,'topic,'score,'term)) {
        value:(String,Int,Double,IntWritable,IntWritable,String) => {

          val (id,topic,score,pos,pos1,term) = value
          (id,topic,score,term)
        
        }
    
      }
      .groupBy('id,'topic,'score) {
      
        val terms = ArrayBuffer.empty[String]
        _.sortBy('term).foldLeft('term -> 'terms)(terms) {
          (words:ArrayBuffer[String],word:String) => {
            words += word
            words
          }
        }
    
      }
      .mapTo( ('id,'topic,'score,'terms) -> 'line) {
        value:(String,Int,Double,ArrayBuffer[String]) => {
        
          val line = value._1 + ";" + value._2 + ";" + value._3 + ";" + value._4.mkString(" ")
          line
        
        }
      }

    val outfile = txtfiles.toString + "-out"
    docTopicWords.write(TextLine(outfile))
    
  }
  
}