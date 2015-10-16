import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Level, Logger}
import com.google.common.hash.Hashing
import com.google.common.base.Charsets

object Driver {
	def hashId(str:String) = {
		Hashing.md5().hashString(str,Charsets.UTF_8).asLong()
	
	}

	def totuple(msg: Any) = {
  		msg match {
    		case tuple @ (a: Any, b: Any) => tuple
  		}
	}
	def tolong(anyval:Any):Long = {
	val a:Option[Any] = Some(anyval) // Note using Any here
	val i = (a match {
  		case Some(x:Long) => x // this extracts the value in a as an Int
  		case _ => 0
	})
	i
	}



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
	val edges = text.map(ids => (hashId(ids(0).trim), hashId(ids(1).trim)))
	
	//get all the opposite edges, these will contain all pairs 
	val intersect = edges.map(e => (e._1,e._2)).intersection(edges.map(e=> (e._2,e._1)))


//get rid of one pair of duplicates
	val dups = intersect.map(a => if(a._1<a._2) (a._2,a._1) else (0,0))

//remove empty lists
	val dupsexact = dups.filter(d=> (d._1 != 0)&&(d._2 !=0))
//get distinct  - this returns Array[Any]
	val distdups1 = dupsexact.distinct
// convert Array[Any] to [Any,Any]
	val distdups2 = distdups1.map(a => totuple(a))

//[Any,Any] to [Long,Long]
	val distdups = distdups2.map(a => (tolong(a._1),tolong(a._2)))

//get the set of undirected edges
	val undedges = edges.subtract(distdups)


    #val text = sc.textFile(filename).map(line => line.split(","))
    #val edges = text.map(ids => (hashId(ids(0).trim), hashId(ids(1).trim.toLong)))
    val nodes = undedges.flatMap(edge => List(edge._1, edge._2)).distinct()
    
   // Count!
    val counter = new ApproxTriangles(10000)
    println("# of triangles = " + counter.count(nodes, undedges, sc).toString)

  }
}
