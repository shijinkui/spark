package org.apache.spark.ps

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Parameter RDD
 * Created by shijinkui on 15/4/15.
 */
class ModelRDD[T: ClassTag, RT: ClassTag](prev: RDD[T],
  //  (partitionId, indexOfPartition, ResultType) => (pidOfDataRDD, indexSeqOfDataRDDPartition)
  locationFunc: (Int, Int, Int, Int, RT) => Seq[(Int, Seq[Int])],
  part: Option[Partitioner] = None, broadcast: Option[Broadcast] = None,
  accumulator: Option[Accumulator] = None) extends RDD[T](prev.context,
  List(new IterateUpdateDependency(prev))) {

  var iterations: Int = _

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

  def runJob(iterations: Int, resultPath: String) = {
    this.iterations = iterations
    collectPartitions()
    saveAsTextFile(resultPath)
  }
}