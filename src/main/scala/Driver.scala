import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Level, Logger}


object Driver {

  def main(args: Array[String]): Unit ={
    // Start the Spark Context and the set the logging to something "reasonable"
    val conf = new SparkConf().setAppName("ApproxTriangles").setMaster("local[2]")
    conf.set("spark.default.parallelism", (8).toString)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.logConf", "true")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)



    // Load the graph from an edge list
    //
    // For the moment I'm assuming the edges are just lists of IDs (longs)
    // in the format id1, id2 and that there are no attributes on anything
    val filename = args(0)
    val text = sc.textFile(filename).map(line => line.split(","))
    val edges = text.map(ids => (ids(0).trim.toLong, ids(1).trim.toLong))
    val nodes = edges.flatMap(edge => List(edge._1, edge._2)).distinct()

    // Count!
    val counter = new ApproxTriangles(10000)
    println("# of triangles = " + counter.count(nodes, edges, sc).toString)

  }
}
