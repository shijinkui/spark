package org.apache.spark.ps

import org.apache.spark.SparkConf

import scala.util.Random

/**
 * sample
 * Created by sjk on 15/4/15.
 */
object PSSample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark PS Sample")
    val ctx = new ParameterOps(conf)
    val slices = if (args.length > 0) args(0).toInt else 5
    val n = math.min(100000L * slices, Int.MaxValue).toInt

    val trainData: Seq[Array[Int]] = Array.tabulate(1000, 100)((m,
    n) => m * Random.nextInt(100000)).toSeq

    val locationFunc = (modelPID: Int, indexOfModelPartition: Int, totalModelParti: Int,
    totalDataParti: Int, rt: Int) => {
      //  用户指定取数据的规则
      Seq((modelPID, Seq(indexOfModelPartition)))
    }

    ctx.buildFromSeq[Array[Int], Int](trainData)(locationFunc).runJob(10, "/tmp/")
  }
}
