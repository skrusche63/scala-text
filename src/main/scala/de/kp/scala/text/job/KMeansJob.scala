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
import org.apache.hadoop.fs.Path

import org.apache.mahout.common.HadoopUtil
import org.apache.mahout.common.distance.CosineDistanceMeasure

import org.apache.mahout.clustering.kmeans.{KMeansDriver,RandomSeedGenerator}

/*
 * This Mahout based KMeans algorithm requires term vectors
 * as TF or TFID vectors; this implies, that are respective
 * vectorization job has to be run before.
 */
class KMeansJob(args:Args) extends Job(args) {

  val conf = new Configuration()
  override implicit val mode = new Hdfs(true, conf) 
  /*
   * Override 'run' avoids errors from cascading that no
   * source and source tap is defined and no processing pipe
   */
  override def run: Boolean = {
    
    val i = args("input")    
    val input = if (i.endsWith("/")) i.substring(0, i.length - 1) else i

    val o = args("output")    
    val output = if (o.endsWith("/")) o.substring(0, o.length - 1) else o
  
    /* Directory path for input data points */
    val vecfiles = new Path(input + "/tfidf-vectors")
  
    /* Directory to hold the initial clusters */
    val initialfiles = new Path(output +"/initial")
    HadoopUtil.delete(conf,initialfiles)
   
    /* Directory to hold the clustering result */
    val finalfiles = new Path(output + "/final")
    HadoopUtil.delete(conf,finalfiles)
  
    val measure = new CosineDistanceMeasure()
  
    val convergenceDelta = args("delta").toDouble
    val maxIterations = args("iter").toInt
    /*
     * Clustering strictness / outlier removal parameter. Its value should be 
     * between 0 and 1. Vectors having pdf below this value will not be clustered.
     */
    val clusterClassificationThreshold = 0.0

    val numClusters = 5
    /*
     * Build random initial clusters
     */
    RandomSeedGenerator.buildRandom(conf, vecfiles, initialfiles, numClusters, measure)

    val runClustering = true
    val runSequential = false

    KMeansDriver.run(conf, vecfiles, initialfiles, finalfiles, convergenceDelta, maxIterations, runClustering, clusterClassificationThreshold, runSequential)

    true
  
  }
}