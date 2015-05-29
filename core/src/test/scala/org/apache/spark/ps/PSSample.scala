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
package org.apache.spark.ps

import org.apache.spark.util.{PSOps, SyncIter}
import org.apache.spark.{SparkConf, TaskContext}

import scala.util.Random

/**
 * sample
 * Created by sjk on 15/4/15.
 */
object PSSample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark PS Sample")

    //  1.  training data
    val trainData: Seq[Int] = Array.tabulate(1000)(_ * Random.nextInt(100000)).toSeq

    //  2.  parameter data
    val parameterData: Seq[Int] = Array.tabulate(1000)(_ * Random.nextInt(100000)).toSeq

    //  3.  training  function
    val ops = new PSOps[Int, Int](conf)
    val trainingFn = (context: TaskContext, it: Iterator[Int]) => {
      //  get parameter data
      val parameters = ops.getParameter(Seq(11), context)
      //  usage: use define arithmetic progress, output parameter value
      val trainingDataPid = context.partitionId()

      println(s"training data partition id:$trainingDataPid, context:${context.getClass}," +
        s"parameters: $parameters")

      it.sum
    }

    //  4.  execute training
    ops.loadParameter(parameterData).buildFromSeq(trainData, SyncIter(10)).startTraining(trainingFn)
  }
}
