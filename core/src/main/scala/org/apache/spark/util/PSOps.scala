/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.util

import org.apache.spark._
import org.apache.spark.rdd.{ParameterRDD, RDD, TrainingRDD}

import scala.reflect.ClassTag

/**
 *
 * @param sc sparkContext
 * @tparam ET line elements type
 * @tparam PT parameter element type
 */
class PSOps[ET: ClassTag, PT: ClassTag](sc: SparkContext) extends Logging {

  private var parameterRDD: ParameterRDD[PT] = _
  private var trainigRDD: TrainingRDD[ET, PT] = _

  /**
   * create context by conf
   * @param conf a org.apache.spark.SparkConf object specifying Spark parameters
   */
  def this(conf: SparkConf) = {
    this(new SparkContext(conf))
  }

  def loadParameter(seq: Seq[PT]): this.type = {
    parameterRDD = new ParameterRDD[PT](sc.parallelize(seq))
    this
  }

  def loadParameter(rdd: RDD[PT]): this.type = {
    parameterRDD = new ParameterRDD[PT](rdd)
    this
  }

  def loadParameter(rdd: ParameterRDD[PT]): this.type = {
    parameterRDD = new ParameterRDD[PT](rdd)
    this
  }

  /**
   * build [[TrainingRDD]] from dfs with [[ParameterRDD]]
   * @param path  dfs file path
   * @param dataPartitionNum min partition number
   * @return
   */
  def buildFromDfs(path: String,
    dataPartitionNum: Int = sc.defaultMinPartitions,
    preproccessFunc: String => ET,
    iterProtocol: PSProtocol,
    trainingPartitioner: Option[Partitioner] = None): this.type = {

    val rdd: RDD[ET] = sc.textFile(path, dataPartitionNum).map {
      line => preproccessFunc(line)
    }
    trainigRDD = new TrainingRDD[ET, PT](rdd, parameterRDD, iterProtocol, trainingPartitioner)
    this
  }

  def buildFromSeq(
    seq: Seq[ET],
    iterProtocol: PSProtocol,
    dataPartitionNum: Int = sc.defaultMinPartitions,
    trainingPartitioner: Option[Partitioner] = None
  ): this.type = {

    assert(parameterRDD != null, "parameterRDD should not be null")
    val dataRDD = sc.parallelize(seq, dataPartitionNum)
    trainigRDD = new TrainingRDD[ET, PT](dataRDD, parameterRDD, iterProtocol, trainingPartitioner)
    this
  }

  /**
   * start parameter training job
   * @param func function of map data to model partition
   */
  def startTraining(func: (TaskContext, Iterator[ET]) => PT) = {
    sc.runJob(trainigRDD, (context: TaskContext, iter: Iterator[ET]) => func(context, iter))
  }

  /**
   * get parameter from ParameterTask
   * @param pids parameter rdd partition ids
   * @param context ps task context
   * @return
   */
  def getParameter(pids: Seq[Int], context: TaskContext): Option[Array[Array[PT]]] = {
    val tc = context.asInstanceOf[PSTaskContextImpl]
    tc.fetchParameter[PT](pids)
  }
}
