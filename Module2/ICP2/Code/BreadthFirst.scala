import org.apache.spark.{SparkConf, SparkContext}

object BreadthFirstAlgorithm {

  System.setProperty("hadoop.home.dir", "c:\\winutil\\")

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BreadthFirstAlgorithm").setMaster("local[*]")
    val sc = new SparkContext(conf)


    type Vertex = Int
    type Graph = Map[Vertex, List[Vertex]]


    val g: Graph = Map(1 -> List(2,3,5,6,7), 2 -> List(1,3,4,6,7), 3 -> List(1,2), 4 -> List(2,5,7),5 -> List(1,6,7),6 -> List(1,2,5,7),7 -> List(1,2,4,5,6))

    def BreadthFirst(start: Vertex, g: Graph): List[List[Vertex]] =
    {

      def BreadthFirst0(elems: List[Vertex],visited: List[List[Vertex]]): List[List[Vertex]] =
      {
        val Neighbors = elems.flatMap(g(_)).filterNot(visited.flatten.contains).distinct
        if (Neighbors.isEmpty) visited
        else BreadthFirst0(Neighbors, Neighbors :: visited)
      }


      BreadthFirst0(List(start),List(List(start))).reverse
    }

    val BreadthFirstresult=BreadthFirst(1,g)
    println("Output of my BreadthFirst Algorithm is",BreadthFirstresult.mkString(","))
  }
}