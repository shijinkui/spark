package org.apache.spark.ps

import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, Partitioner, SparkConf, SparkContext}

class ParameterOps(sc: SparkContext) extends Logging {

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
   * @param partitionNum min partition number
   * @tparam ET line elements type
   * @return
   */
  def buildFromHDFS[ET, RT](path: String, lineSep: String,
    partitionNum: Int = sc.defaultMinPartitions)(
    modelPartitioner: Option[Partitioner] = None,
    //  (partitionId, indexOfPartition, ResultType) => (pidOfDataRDD, indexSeqOfDataRDDPartition)
    locationFunc: (Int, Int, Int, Int, RT) => Seq[(Int, Seq[Int])]): ModelRDD[Array[ET], RT] = {

    val rdd: RDD[Array[ET]] = sc.textFile(path, partitionNum).map {
      line => {
        line.split(lineSep).map(_.trim.asInstanceOf[ET])
      }
    }

    new ModelRDD[Array[ET], RT](rdd, locationFunc, modelPartitioner)
  }

  def buildFromSeq[ET, RT](seq: Seq[ET],
    partitionNum: Int = sc.defaultMinPartitions)(
    //  (partitionId, indexOfPartition, modelTotalParti, dataTotoalParti, ResultType) => (pidOfDataRDD, indexSeqOfDataRDDPartition)
    locationFunc: (Int, Int, Int, Int, RT) => Seq[(Int, Seq[Int])],
    modelPartitioner: Option[Partitioner] = None): ModelRDD[ET, RT] = {
    val dataRDD = sc.parallelize(seq, partitionNum)

    new ModelRDD[ET, RT](dataRDD, locationFunc, modelPartitioner)
  }


}
