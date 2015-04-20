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
package org.apache.spark.rdd

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.IterativeProtocol

import scala.reflect.ClassTag

/**
 * Parameter RDD
 * Created by shijinkui on 15/4/15.
 */
private[spark] class ModelRDD[T: ClassTag, RT: ClassTag](prev: RDD[T],
  // (partitionId, indexOfPartition, modelTotalParti, dataTotalParti, ResultType) =>
  // (pidOfDataRDD, indexSeqOfDataRDDPartition)
  _locationFunc: (Int, Int, Int, Int, RT) => (Int, Int),
  _iterProtocol: IterativeProtocol,
  part: Option[Partitioner] = None) extends RDD[RT](prev.context,
  List(new IterativeUpdateDependency(prev))) {

  //  use for task running
  var iterations: Int = _
  //  var accumulator: Option[Accumulator] = _
  //  var broadcast: Option[Broadcast] = _

  val preRDD = prev
  val iterProtocol = _iterProtocol
  val locationFunc = _locationFunc
  val totalPartiNumPreRDD = prev.partitions.length

  override val partitioner = part

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[RT] = {
    firstParent[RT].iterator(split, context)
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = firstParent[T].partitions
}

