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
package org.apache.spark

import java.io.Serializable
import java.nio.ByteBuffer

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{Task, TaskLocation}

/**
 * parameter task
 * provide data fetch and update service
 */
private[spark] class ParameterTask[T, U](
  stageId: Int,
  taskBinary: Broadcast[Array[Byte]],
  partition: Partition,
  @transient locs: Seq[TaskLocation],
  val outputId: Int) extends Task[T](stageId, partition.index) with Serializable {

  override def runTask(context: TaskContext): T = {
    // Deserialize the RDD and the func using the broadcast variables.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

  }
}
