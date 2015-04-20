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

import org.apache.spark.util.{IterativeUpdateOps, SyncIter}

import scala.util.Random

/**
 * sample
 * Created by sjk on 15/4/15.
 */
object PSSample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark PS Sample")

    //  1. 准备训练数据
    val trainData: Seq[Array[Int]] = Array.tabulate(1000, 100)((m,
    n) => m * Random.nextInt(100000)).toSeq

    //  2. 定义定位函数, 根据modelRDD partiton中的一条数据, 定位到相应的训练数据
    val locationFunc = (modelPID: Int, indexOfModelPartition: Int, totalModelParti: Int,
    totalDataParti: Int, rt: Int) => {
      //  用户指定取数据的规则
      (modelPID, indexOfModelPartition)
    }

    //  3. 定义训练函数, 一条训练数据 到 一条模型数据的映射
    val func = (it: Iterator[Int]) => it.sum

    //  4. 执行
    val ops = new IterativeUpdateOps[Array[Int], Int](conf)
    ops.buildFromSeq(trainData)(locationFunc, SyncIter()).iterativeUpdate[Int](10, func, Some("/tmp/"))
  }
}
