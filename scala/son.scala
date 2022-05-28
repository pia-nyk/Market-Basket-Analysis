import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}
import java.util.concurrent.TimeUnit
import scala.collection.mutable

object son {

  def getSparkContext(): SparkContext = {
    val spark = new SparkConf().setAppName("task1").setMaster("local")
    val sc = new SparkContext(spark)
    sc.setLogLevel("WARN");
    sc
  }

  def bfs(vertex: String, heightInfo: collection.mutable.Map[Int, List[String]],
          parents: collection.mutable.Map[String, List[String]], shortestPathInfo:
          collection.mutable.Map[String, Int],
          edges: collection.mutable.Map[String, List[String]]): Unit = {

    //initialize a queue for bfs
    val queue = mutable.Queue[String]()

    //initialize dictionary for heights (user -> height)
    val heights = collection.mutable.Map[String, Int]()

    //create a set to store all visited nodes
    val visited = collection.mutable.Set[String]()

    //add the first node to the queue
    queue.enqueue(vertex)
    visited += vertex

    //add the first node in the heights info dict
    heights += (vertex -> 1)
    heightInfo(1) = vertex :: heightInfo(1)

    //make the shortest path to the root node as 1
    shortestPathInfo(vertex) = 1

    //process all elements in the queue
    while (queue.length > 0) {
      val item = queue.dequeue()
      val height = heights(item)

      //visit all vertices which have an edge with the current vertex
      edges.getOrElse(item, List[String]()).foreach(v => {
        if (!visited.contains(v)) {
          parents(v) = item :: parents(v)
          heightInfo(height+1) = v :: heightInfo(height+1)
          heights(v) = height+1
          visited += v
          queue.enqueue(v)
          shortestPathInfo(v) = shortestPathInfo(item)
        } else if (heights(v) > heights(item)) {
          shortestPathInfo(v) += shortestPathInfo(item)
          parents(v) = item :: parents(v)
        }
      })
    }
  }

  def getBetweennessScore(vertices: List[String], edges: collection.mutable.Map[String, List[String]]): collection.mutable.Map[Tuple2[String, String], Double] = {
    val edgeScores = collection.mutable.Map[Tuple2[String, String], Double]().withDefaultValue(0.0f)

    //iterate all vertices
    vertices.foreach(vertex => {
      //initialize local variables
      val heightInfo = scala.collection.mutable.Map[Int, List[String]]().withDefaultValue(List[String]())
      val parents = collection.mutable.Map[String, List[String]]().withDefaultValue(List[String]())
      val shortestPathInfo = collection.mutable.Map[String, Int]()
      val creditsInfo = collection.mutable.Map[String, Double]().withDefaultValue(0.0)

      //run a bfs to get info about the structure of the graph starting at this vertex
      bfs(vertex, heightInfo, parents, shortestPathInfo, edges)

      //assign credit scores to each of the vertex and scores to the edges based on structure of the graph
      assignScores(heightInfo, parents, shortestPathInfo, creditsInfo, edgeScores)
    })

    edgeScores.keySet.foreach(key => {
      edgeScores(key) /= 2
    })

    edgeScores
  }


  def assignScores(heightsInfo: scala.collection.mutable.Map[Int, List[String]],
                   parents: collection.mutable.Map[String, List[String]], shortestPathInfo: collection.mutable.Map[String, Int],
                   creditsInfo: collection.mutable.Map[String, Double], edgeScores: collection.mutable.Map[Tuple2[String, String], Double]): Unit = {

    //max height of the tree
    var maxHeight = heightsInfo.size

    //starting from the bottom, going to the top, assign credit scores to each of the nodes in the graph
    for (height <- maxHeight to 1 by -1) {
      heightsInfo.getOrElse(height, List[String]()).foreach(vertex => {
        creditsInfo(vertex) += 1

        //iterate all parents of this vertex to update their credit scores
        parents.getOrElse(vertex, List[String]()).foreach(parent => {
          var score: Double = creditsInfo.getOrElse(vertex, 0.0) * (shortestPathInfo.getOrElse(parent, 0).toDouble / shortestPathInfo.getOrElse(vertex, 0).toDouble)
          creditsInfo(parent) += score
          val sortedKey = List(parent, vertex).sorted
          edgeScores(Tuple2(sortedKey(0), sortedKey(1))) += score
        })
      })
    }
  }

  def calculateDegrees(edges: collection.mutable.Map[String, List[String]]): collection.mutable.Map[String, Int] = {
    var degrees = collection.mutable.Map[String, Int]().withDefaultValue(0)
    edges.keySet.foreach(key => {
      degrees(key) = edges.getOrElse(key, List()).length
    })
    degrees
  }

  def calculateModularityForCommunity(degrees: collection.mutable.Map[String, Int],
                                      community: List[String], localEdges: collection.mutable.Map[String, List[String]],
                                      totalEdges: Int): Double = {
    var sum = 0.0
    community.foreach(i => {
      val ki = degrees.getOrElse(i, 0)
      community.foreach(j => {
        val kj = degrees.getOrElse(j, 0)
        var isValidEdge = 0
        if (localEdges.getOrElse(i, List[String]()).contains(j)){
          isValidEdge = 1
        }
        var value = isValidEdge - ((ki * kj) / (2 * totalEdges).toDouble )
        sum += value
      })
    })
    sum
  }

  def bfsForCommunities(vertex: String, edges: collection.mutable.Map[String, List[String]], visited: collection.mutable.Set[String]): List[String] = {
    //initialize queue for bfs
    var queue = mutable.Queue[String]()
    queue.enqueue(vertex)
    visited += vertex

    //keep track of all nodes visited from start node to get the community
    var community = List[String]()
    community = vertex :: community

    while (queue.nonEmpty) {
      var item = queue.dequeue()

      //go through all vertices which have an edge with the current node
      edges.getOrElse(item, List[String]()).foreach(v => {
        if (!visited.contains(v)) {
          visited += v
          community = v :: community
          queue.enqueue(v)
        }
      })
    }
    community
  }

  def calculateOverallModularity(localEdges: collection.mutable.Map[String, List[String]],
                                 degrees:  collection.mutable.Map[String, Int], totalEdges: Int,
                                 vertices: List[String]): Tuple2[Double, List[List[String]]] = {

    var visited = collection.mutable.Set[String]()

    //get all communities present in the graph at this point
    var communities = List[List[String]]()

    vertices.foreach(vertex => {
      if (!visited.contains(vertex)) {
        var community = bfsForCommunities(vertex, localEdges, visited).sorted
        communities = community :: communities
      }
    })

    var modularity = 0.0
    communities.foreach(community => {
      var modularityLocal = calculateModularityForCommunity(degrees, community, localEdges, totalEdges)
      //      println("Community " + community + " Score: " + modularityLocal)
      modularity += modularityLocal
    })
    Tuple2(modularity / (2 * totalEdges).toDouble, communities)
  }

  def countEdges(localEdges: collection.mutable.Map[String, List[String]]): Int = {
    var totalEdges = 0
    for ((k,v) <- localEdges) {
      totalEdges += v.length
    }
    totalEdges
  }

  def findCommunitiesWithMaxModularity(edges: collection.mutable.Map[String, List[String]], degrees: collection.mutable.Map[String, Int],
                                       totalEdges: Int, vertices: List[String]): List[List[String]] = {
    //make copy of edges of the graph
    var localEdges = edges.clone()

    //initialize max modularity
    var maxModularity = Integer.MIN_VALUE.toDouble

    var optCommunities = List[List[String]]()

    while(countEdges(localEdges) > 0) {
      var scores = getBetweennessScore(localEdges.keySet.toList, localEdges)
      //      println("Scores: " + scores)

      var largestScore = scores.maxBy(_._2)
      //      println("Largest Score: " + largestScore)

      var edgesWithLargestScore = List[(String, String)]()

      //get all keys with value = largest value
      for ((k,v) <- scores) {
        if(v == largestScore._2) {
          edgesWithLargestScore = (k._1, k._2) :: edgesWithLargestScore
        }
      }

      //remove all the edges with max valuea
      for (edge <- edgesWithLargestScore) {
        var u = edge._1
        var v = edge._2

        var temp = localEdges.getOrElse(u, List[String]()).filter(_ != v)
        localEdges(u) = temp

        temp = localEdges.getOrElse(v, List[String]()).filter(_ != u)
        localEdges(v) = temp

      }

      val (modularity, communities) = calculateOverallModularity(localEdges, degrees, totalEdges, vertices)

      if (modularity > maxModularity) {
        maxModularity = modularity
        optCommunities = communities
      }
    }
    optCommunities
  }

  def main(arg: Array[String]): Unit = {

    val start = System.nanoTime()
    //get spark context for map reduce
    val sparkContext = getSparkContext()

    val threshold = arg(0).toInt

    val betweennessFile = arg(2)

    //load the data from the file
    var textFile = arg(1)
    var rdd = sparkContext.textFile(textFile)

    //remove the headers
    var headers = rdd.first()
    rdd = rdd.filter(line => line != headers)

    //extract values from the line
    var newRdd = rdd.map(line => (line.split(",")(0), line.split(",")(1)))

    //get all unique users
    var users = newRdd.map(line => line._1).distinct()

    //group business ids by user ids
    var reviews = newRdd.map(line => (line._1, List(line._2))).reduceByKey(_++_).map(line => (line._1, line._2.distinct.sorted)).collectAsMap()

    //get all pairs of users
    val pairs = users.cartesian(users).filter(pair => pair._1 < pair._2)

    //get all valid edges
    val filteredPairs = pairs.map(pair => (pair, reviews.getOrElse(pair._1, List()), reviews.getOrElse(pair._2, List()))).filter(line => line._2.toSet.intersect(line._3.toSet).size >= threshold).map(line => line._1)

    //get the vertices
    val vertices = filteredPairs.flatMap(line => List(line._1, line._2)).distinct().collect()

    //get the edges
    val edges = filteredPairs.flatMap(line => List(Tuple2(line._1, line._2), Tuple2(line._2, line._1))).distinct().groupByKey().map(line => (line._1, line._2.toList)).collectAsMap()

    //
    //    var vertices = List("A", "B", "C", "D", "E", "F", "G")
    //
    //    var edges = collection.mutable.Map("A" -> List("B", "C"),
    //      "B" -> List("A", "C", "D"),
    //      "C" -> List("A", "B"),
    //      "D" -> List("B", "G", "E", "F"),
    //      "E" -> List("D", "F"),
    //      "F" -> List("E", "G", "D"),
    //      "G" -> List("D", "F"))

    //calculate the betweenness scores
    val scores = sparkContext.parallelize(getBetweennessScore(vertices.toList, collection.mutable.Map(edges.toSeq: _*)).toSeq).map(line => (line._1,  Math.round(line._2 * 100000.0) / 100000.0))
      .sortBy(line => (-line._2, line._1.toString(), line._2.toString()))

    //    val formatter = new DecimalFormat("#.#####")

    val bwFile = new File(arg(2))
    val pbw = new PrintWriter(bwFile)

    scores.collect().foreach(score => {
      pbw.write("(\'" + score._1._1 + "\', \'" + score._1._2 + "\')," + score._2 + "\n")
    })
    pbw.close()

    val degrees = calculateDegrees(collection.mutable.Map(edges.toSeq: _*))

    val communities = sparkContext.parallelize(findCommunitiesWithMaxModularity(collection.mutable.Map(edges.toSeq: _*), degrees, scores.collectAsMap().keySet.size, vertices.toList))
      .sortBy(line => (line.length, line(0))).collect()


    val communityFile = arg(3)
    val file = new File(communityFile)
    val pw = new PrintWriter(file)

    communities.foreach(community => {
      pw.write("\'" + community(0) + "\'")
      community.slice(1, community.length).foreach(v => {
        pw.write(", \'" + v + "\'")
      })
      pw.write("\n")
    })

    pw.close()

    val finish = System.nanoTime() - start
    println("Duration:" + TimeUnit.SECONDS.convert(finish, TimeUnit.NANOSECONDS).toString)

  }

}