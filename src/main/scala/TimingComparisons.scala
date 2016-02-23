import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * A class to run timing comparisons between the approximate triangle counting algorithm
 * from Sandia and GraphX's exact triangle count
 */


object TimingComparisons {

  /**
   * Times a given block of code and outputs the elapsed wall time
   * @param block
   * @tparam R
   * @return
   */
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("[TIMING] Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  def main(args: Array[String]): Unit = {
    // Spark setup
    val conf = new SparkConf().setAppName("TimingComparisons")
    conf.set("spark.default.parallelism", (8).toString)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.logConf", "true")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    val graph_generator = new ErdosRenyi()
    val p = 0.1
    val partitions = 5
    val approx_counter = new ApproxTriangles(10000)
    val graph_sizes = Array(20000, 30000,40000, 50000)

    for (n <- graph_sizes){
      val edges = graph_generator.G(n, p, sc, partitions)
      val nodes = sc.parallelize(0 to n).map(x=>x.toLong)
      val t0 = System.nanoTime()
      approx_counter.count(nodes, edges, sc)
      val t1 = System.nanoTime()
      println("[TIMING] Size: " + n.toString + " Approx time: " + (t1 - t0) + "ns")
      val graph = Graph.fromEdges(edges.map(x=>Edge(x._1, x._2, 0.0)), defaultValue = 0)
      val t2 = System.nanoTime()
      graph.triangleCount()
      val t3 = System.nanoTime()
      println("[TIMING] Size: " + n.toString + " Exact time: " + (t3 - t2) + "ns")
    }
  }

}
