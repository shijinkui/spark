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

package org.apache.spark.scheduler

import java.io._
import java.nio.ByteBuffer

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{RDD, TrainingRDD}

import scala.reflect.ClassTag

/**
 * A task that sends back the output to the driver application.
 *
 * See [[Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcasted version of the serialized RDD and the function to apply on each
 *                   partition of the given RDD. Once deserialized, the type should be
 *                   (RDD[T], (TaskContext, Iterator[T]) => U).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param outputId index of the task in this job (a job can launch tasks on only a subset of the
 *                 input RDD's partitions).
 */
private[spark] class TrainingTask[ET: ClassTag, PT: ClassTag](
  stageId: Int,
  taskBinary: Broadcast[Array[Byte]],
  partition: Partition,
  @transient locs: Seq[TaskLocation],
  val outputId: Int)
  extends Task[PT](stageId, partition.index) with Serializable {

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(tc: TaskContext): PT = {
    // Deserialize the RDD and the func using the broadcast variables.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (_rdd, func) = ser.deserialize
      [(RDD[ET], (TaskContext, Iterator[ET]) => PT)](
        ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

    metrics = Some(tc.taskMetrics())
    val rdd: TrainingRDD[ET, PT] = _rdd.asInstanceOf[TrainingRDD[ET, PT]]

    //  execute iterative updated task
    val protocol = rdd.protocol
    val it: Iterator[ET] = rdd.iterator(partition, tc)
    val result = new Array[PT](it.length)
    val context = PSTaskContextImpl(tc)

    for (i <- 0 until protocol.getIterations) {

      //  training parameter with current partition and parameters
      val parameter = func(context, it)
      println(s"training result of iteration $i, result:$parameter")
      //  update parameter delta data
//      msgRpcRef.get.send()
      context.updateParameter()
    }

    result.head
  }


  // This is only callable on the driver side.
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ResultTask(" + stageId + ", " + partitionId + ")"
}
