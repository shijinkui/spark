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

import org.apache.spark.util.{AsyncIter, IterativeProtocol, SyncIter}

/**
 * scheduler of [[IterativeUpdateTask]]
 * Created by sjk on 17/4/15.
 */
private[spark] class IterativeUpdateScheduler {

  //  pid -> (currentIterator, totalCost)
  private lazy val progress = scala.collection.mutable.Map[Int, (Int, Long)]()
  private lazy val default_waiting_time = 1000L //  milliseconds

  /**
   * finish one iterate
   * @param pid partition id
   * @param curIter current running iteration of [[IterativeUpdateTask]]
   * @param cost the finished iteration time cost, milliseconds
   * @return
   */
  private def finishIteration(pid: Int, curIter: Int, cost: Long) = {
    val totalCost = if (progress.contains(pid)) progress(pid)._2 else default_waiting_time
    progress.put(pid, (curIter, totalCost + cost))
  }

  /**
   * whether run next iteration, or wait until fit sync protocol
   * @param pid partition id
   * @param iterProtocol iteration protocol, sync or async
   * @param curIter current running iteration of [[IterativeUpdateTask]]
   * @return boolean and wainting milliseconds averaged
   */
  private def canRunNext(pid: Int, curIter: Int,
    iterProtocol: IterativeProtocol): (Boolean, Long) = {

    val averageCost = progress.getOrElse(pid, (curIter, 0L))._2 / curIter

    iterProtocol match {
      case prot: SyncIter => {
        val can = progress.exists(f => curIter > f._2._1)
        //  return whether can run next, and average cost of every iteration
        (can, averageCost)
      }
      case prot: AsyncIter => {
        val max = progress.maxBy(_._2._1)
        val can = curIter - max._2._1 > prot.thresholdValue
        (can, averageCost)
      }
    }
  }
}
