/**
 * A simple distributed Erdos-Renyi random graph generator
 */
import java.util.Random
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

class ErdosRenyi extends Serializable{

  val random_seed = 12345

  def edgeSelection(index: Int, nodes: Iterator[Long], max_nodes: Long, p: Double): Iterator[(Long, Long)] ={
    val rand = new Random(index + random_seed)
    val edges: ListBuffer[(Long, Long)] = ListBuffer()
    for (node <- nodes){
      for (i <- node+1 to max_nodes){
        val q = rand.nextDouble()
        if ( q <= p){
          edges.append((node, i))
        }
      }
    }
    edges.iterator
  }

  def G(nodes: Long, p: Double, sc: SparkContext, partitions: Int): RDD[(Long, Long)] = {
    val n: List[Long] = (0L to nodes).toList
    val nodeRDD = sc.parallelize(n, partitions)
    nodeRDD.mapPartitionsWithIndex((idx, iter) => edgeSelection(idx, iter, nodes, p))
  }

}