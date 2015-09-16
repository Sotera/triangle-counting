/*
This code approximates the number of triangles and clustering coefficient of a graph using wedge sampling.
For a reference see:
(1) Seshadhri, C., Pinar, A., & Kolda, T. G. (2013). Triadic Measures on Graphs : The Power of Wedge Sampling ∗. Proceedings of the 2013 SIAM International Conference on Data Mining, 10–18.
(2) Kolda, T. G., Pinar, A., Plantenga, T., Seshadhri, C., & Task, Christine, M. (2013). Counting Triangles in Massive Graphs with MapReduce, 36(5), 1–30. Retrieved from http://arxiv.org/abs/1301.5887
 */

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random


case class WedgeBinData(nVertex: Long, nWedges: Long)
case class VertexData(degree: Long, nWedgesBin: Long, nSamples: Int, sampled: Boolean)
case class BinnedResults(var q0: Long, var q1: Long, var q2: Long, var q3: Long, var nSamples: Long)

class ApproxTriangles(val k: Long) extends Serializable{

  /* Parameters for the exponential binning */
  val omega: Long = 2
  val tau: Long = 1000
  val random_seed: Long = 123456  // TODO: probably replace this with the current time?

  /*
  Given a node with degree deg returns a bin ID where the bins grow exponentially
   */
  def expBinId(deg: Long): Long ={
    if (deg < tau){
      return deg
    }
    return math.floor(math.log(1 + (omega-1)*(deg-tau)/math.log(omega))).toLong + tau
  }


  /*
  Samples wedges to give an approximate triangle count
   */
  def count(nodes: RDD[Long], edges: RDD[(Long, Long)], sc: SparkContext): Long ={
    // Step (1)
    // calculate degree, wedges per bin
    val degrees = edges.flatMap(edge => List((edge._1, 1), (edge._2, 1))).reduceByKey(_+_)
    val wedges_per_bin_rdd = degrees.map( id_deg => {
      val (id, deg) = id_deg
      (expBinId(deg), WedgeBinData(1, deg*(deg-1)/2))
    }).reduceByKey((a,b) => WedgeBinData(a.nVertex+b.nVertex, a.nWedges+b.nWedges))
    val wedges_per_bin = wedges_per_bin_rdd.collect.toMap
    val broadcast_wedge_bin = sc.broadcast(wedges_per_bin)  // distribute the info to all workers

    // debug step
    //println("\nDegrees")
    //println("-------")
    //degrees.collect.foreach(println)
    //println("\nWedge binning info")
    //println("------------------")
    //wedges_per_bin.foreach(println)

    // Step (2a) Sample the wedge centers
    val wedge_centers = VertexRDD(degrees.mapPartitionsWithIndex({ (idx, iter) =>
      val rand = new Random(idx + random_seed)
      val wedge_bin = broadcast_wedge_bin.value
      iter.map(x => {
        val deg = x._2
        var p = 0L
        var q = 0.0
        var sampled = false
        if (deg >= 2) {
          // We can only sample wedges from nodes with wedges (not degree 0 or 1)
          p = wedge_bin(expBinId(deg)).nWedges
          val y = rand.nextDouble()
          val q_star: Double = deg * (deg - 1) / 2.0 * (k / p.toDouble)
          q = if (y >= (q_star - math.floor(q_star))) {
            math.floor(q_star)
          } else {
            math.ceil(q_star)
          }
          //println("y = " + y + " d = " + deg + " p = " + p + " k = " + k + " q_star = "+ q_star + " q = " + q)
          sampled = if (q >= 1) {true
          } else {
            false
          }
        }
        (x._1, VertexData(deg, p, q.toInt, sampled))
      })
    }))

    // debug
    //wedge_centers.collect()
    //println("\nWedge Center Sampling")
    //println("---------------------")
    //wedge_centers.foreach(x => {
    //  val data = x._2
    //  println("Vertex id = " + x._1 + " degree = " + data.degree + " nWedgesBin = " + data.nWedgesBin + " Sampled? = " +
    //  data.sampled + " # of samples = " + data.nSamples)
    //})

    // skipping step 2b

    // Step (2c) Create sample wedges using aggregateMessages
    val graph: Graph[VertexData, Long] = Graph(wedge_centers, edges.map(line => Edge(line._1, line._2, 0L))).partitionBy(PartitionStrategy.RandomVertexCut)
    graph.cache()
    val neighs = graph.aggregateMessages[List[(Long, VertexData)]](
    // mapper message
    triplet => {
      if (triplet.srcAttr.sampled){
        triplet.sendToSrc(List((triplet.dstId, triplet.dstAttr)))
      }
      if (triplet.dstAttr.sampled){
        triplet.sendToDst(List((triplet.srcId, triplet.srcAttr)))
      }
    },
    // reducer message
    (msg1, msg2) => {msg1 ++ msg2}
    )

    // debug
    //println("\nAggregated neighbors for sampled vertices")
    //println("-----------------------------------------")
    //neighs.collect.foreach(println)


    val sample_wedges = neighs.innerJoin(wedge_centers)((id, vertex_list, data) => (data, vertex_list)).mapPartitionsWithIndex({ (idx, iter) =>
      val rand = new Random(idx + random_seed)
      iter.flatMap(x => {
        val (id, (data, neighbors)) = x
        val randomized_neigh = rand.shuffle(neighbors)
        var samples = randomized_neigh.combinations(2).take(data.nSamples)
        samples.map(sample => ((sample(0)._1, sample(1)._1), (id, data, sample(0)._2, sample(1)._2))) // key, value format: ((b,c), (a, a_data, b_data, c_data))
      })
    })

    //println("\nSampled wedges")
    //println("--------------")
    //sample_wedges.collect.foreach(println)


    // Step (3) Check for the closing edges
    sample_wedges.persist()
    val edges_for_join = edges.flatMap(line => List(((line._1, line._2), 1), ((line._2, line._1),1)))
    val closed_wedges = sample_wedges.leftOuterJoin(edges_for_join).map(x =>{
      val ((b,c), ((a, a_data, b_data, c_data), flag)) = x
      val closed = if ( flag.getOrElse(0) == 1){true} else {false}
      (expBinId(a_data.degree), (a, b, c, a_data, b_data, c_data, closed))
    })
    sample_wedges.unpersist()

    //println("\nTriangles in the sampled wedges")
    //println("-------------------------------")
    //closed_wedges.collect.foreach(println)

    // Step (4) Aggregate (by bin) to estimate coupling coeff/# triangles per bin
    val estimates_by_bin = closed_wedges.aggregateByKey(BinnedResults(0,0,0,0,0))(
      // sequential aggregation
      (agg, data) => {
        val (a,b,c,a_data,b_data,c_data,closed) = data
        if (closed) {
          val abin = expBinId(a_data.degree)
          val bbin = expBinId(b_data.degree)
          val cbin = expBinId(c_data.degree)
          if ( (abin==bbin) &&  (abin==cbin) ){
            agg.q3 += 1
          } else if ( (abin==bbin) || (abin==cbin)){
            agg.q2 += 1
          } else {
            agg.q1 += 1
          }
        } else {
          agg.q0 += 1
        }
        agg.nSamples += 1
        agg
      },
      // combining aggregators
      (agg1, agg2) => {
        agg1.q0 += agg2.q0
        agg1.q1 += agg2.q1
        agg1.q2 += agg2.q2
        agg1.q3 += agg2.q3
        agg1.nSamples += agg2.nSamples
        agg1
      }
    ).map(x =>{
      val (binid, result) = x
      val wedge_bin = broadcast_wedge_bin.value
      val nWedge = wedge_bin(binid).nWedges
      val c: Double = (result.q1 + result.q2 + result.q3) / (result.q0 + result.q1 + result.q2 + result.q3).toDouble
      val t = nWedge * (result.q1 + result.q2/2.0 + result.q3/3.0) / (result.q0 + result.q1 + result.q2 + result.q3).toDouble
      (binid, c, t, result.nSamples)
    })

    println("\nEstimates by bin")
    println("----------------")
    val results_by_bin = estimates_by_bin.collect.sortBy(_._1)
    for(x <- results_by_bin){
      println("Bin " + x._1 + " coupling coefficient = " + x._2  + " triangles = " + x._3 + " total # of sampled wedges = " + x._4 + " +/- error (99%) = " + math.sqrt(math.log(2/0.01)/(2*x._4)))
    }


    val total_wedges = wedges_per_bin.map(x=> x._2.nWedges).reduce(_+_)
    val global_ccoef = results_by_bin.map(x => {
      val (bin, cc, tri, samples) = x
      val this_bin_wedge = wedges_per_bin(bin).nWedges
      (this_bin_wedge * cc)
    }).reduce(_+_) / (total_wedges.toDouble)

    println("\n")
    println("Global clustering coefficient estimate = "+ global_ccoef)
    println("Total number of triangles estimate = " + global_ccoef * total_wedges/3)
    return (global_ccoef * total_wedges/3).toLong
  }

}