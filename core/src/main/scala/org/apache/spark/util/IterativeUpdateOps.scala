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

import org.apache.spark.rdd.{ModelRDD, RDD}
import org.apache.spark.{Logging, Partitioner, SparkConf, SparkContext}

import scala.reflect.{ClassTag, classTag}

/**
 *
 * @param sc sparkContext
 * @tparam ET line elements type
 * @tparam RT result element type
 */
class IterativeUpdateOps[@specialized(Long, Int, Double) ET: ClassTag,
RT: ClassTag](sc: SparkContext) extends Logging {

  /** result element type */
  lazy val rtTag: ClassTag[RT] = classTag[RT]

  /**
   * create context by conf
   * @param conf a org.apache.spark.SparkConf object specifying Spark parameters
   */
  def this(conf: SparkConf) = {
    this(new SparkContext(conf))
  }

  /**
   *
   * @param path  source file path
   * @param lineSep line separator
   * @param dataPartitionNum min partition number
   * @param locationFunc how to localtion pre RDD partition data
   *                     param: (partitionId, indexOfPartition, modelTotalParti, dataTotoalParti,
   *                     ResultType) =>  (pidOfDataRDD, indexSeqOfDataRDDPartition)
   * @return
   */
  def buildFromDfs(path: String, lineSep: String,
    dataPartitionNum: Int = sc.defaultMinPartitions)(
    locationFunc: (Int, Int, Int, Int, RT) => (Int, Int),
    iterProtocol: IterativeProtocol,
    modelPartitioner: Option[Partitioner] = None): ModelRDD[String, RT] = {

    val rdd: RDD[String] = sc.textFile(path, dataPartitionNum)
    new ModelRDD[String, RT](rdd, locationFunc, iterProtocol, modelPartitioner)
  }

  def buildFromSeq(seq: Seq[ET],
    dataPartitionNum: Int = sc.defaultMinPartitions)(
    locationFunc: (Int, Int, Int, Int, RT) => (Int, Int),
    iterProtocol: IterativeProtocol,
    modelPartitioner: Option[Partitioner] = None
  ): ModelRDD[ET, RT] = {
    val dataRDD = sc.parallelize(seq, dataPartitionNum)

    new ModelRDD[ET, RT](dataRDD, locationFunc, iterProtocol, modelPartitioner)
  }
}
