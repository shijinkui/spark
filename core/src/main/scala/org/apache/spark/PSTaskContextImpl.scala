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

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.ps.{MemoryStorage, OplogReq, NettyServer}

import scala.reflect.ClassTag

private[spark] class PSTaskContextImpl[PT: ClassTag](
  override val stageId: Int,
  override val partitionId: Int,
  override val taskAttemptId: Long,
  override val attemptNumber: Int,
  override val runningLocally: Boolean = false,
  override val taskMetrics: TaskMetrics = TaskMetrics.empty
) extends TaskContextImpl(stageId, partitionId, taskAttemptId,
  attemptNumber, runningLocally, taskMetrics) {

  private lazy val rpcservice: NettyServer = new MemoryStorage[PT]()

  def fetchParameter(pids: Seq[Int]): Option[Array[Array[PT]]] = {

    None
  }

  def updateParameter(oplog: OplogReq): Boolean = {

    true
  }
}

private[spark] object PSTaskContextImpl {
  def apply[PT: ClassTag](tc: TaskContext): PSTaskContextImpl[PT] = {
    new PSTaskContextImpl[PT](
      tc.stageId(), tc.partitionId(), tc.taskAttemptId(),
      tc.attemptNumber(), tc.isRunningLocally(), tc.taskMetrics())
  }
}
