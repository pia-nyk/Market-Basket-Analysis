import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.graphframes.GraphFrame

import java.io.{File, PrintWriter}
import java.util.concurrent.TimeUnit
import scala.collection.Map

object getFrequentItems {
  def getSparkContext(): SparkContext = {
    val spark = new SparkConf().setAppName("task1").setMaster("local")
    val sc = new SparkContext(spark)
    sc.setLogLevel("WARN");
    sc
  }

  def getSparkSession(): SparkSession = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("SparkByExamples.com")
      .getOrCreate()
    spark
  }

  def main(arg: Array[String]): Unit = {

    var start = System.nanoTime()

    //get spark context for map reduce
    val sparkContext = getSparkContext()

    //get the spark session to create dataframes
    val sparkSession = getSparkSession()

    val threshold = arg(0).toInt

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
    var mappedData = newRdd.map(line => (line._1, List(line._2))).reduceByKey(_++_).map(line => (line._1, line._2.distinct))

    //    //remove the users which do not havent rated atleast 7 businesses
    mappedData = mappedData.filter(line => line._2.size >= threshold)


    //get the mapped data as dictionary
    val mappedDataDict: Map[String, List[String]] = mappedData.collectAsMap()

    //get all pairs of users
    val pairs = users.cartesian(users).filter(pair => pair._1 < pair._2)

    //get all valid edges
    val edgeCandidates = pairs.map(pair => (pair, mappedDataDict.getOrElse(pair._1, List()), mappedDataDict.getOrElse(pair._2, List()))).filter(line => line._2.toSet.intersect(line._3.toSet).size >= threshold).map(line => line._1)

    //get all the vertices
    val verticesCandidates = edgeCandidates.map(line => (1, List(Tuple2(line._1, line._1), Tuple2(line._2, line._2)))).reduceByKey(_++_).mapValues(_.distinct).collect()(0)._2

    //add the relationships to the edges
    val edgesCandidatesWithRelation = edgeCandidates.map(line => (line._1, line._2, "friend"))

    //create edges & vertices to be fed to the graph
    val vertices = sparkSession.createDataFrame(verticesCandidates).toDF("id", "name")
    val edges = sparkSession.createDataFrame(edgesCandidatesWithRelation).toDF("src", "dst", "relationship")

    //create the graph
    val g = GraphFrame(vertices, edges)


    //get communities using label propagation
    val result = g.labelPropagation.maxIter(5).run()
    val resultRdd = result.rdd.map(line => (line(2), List(line(0).toString))).reduceByKey(_++_).mapValues(_.distinct.sorted).map(line => line._2).sortBy(line => (line.length, line.head)).collect()


    //get the output file name
    val outputFile = arg(2)

    //write the results to the file
    val fileObj = new File(outputFile)
    val printWriter = new PrintWriter(fileObj)
    resultRdd.foreach(row => {
      printWriter.write("\'" + row.head + "\'")
      row.slice(1, row.length).foreach(value => {
        printWriter.write(", \'" + value + "\'")
      })
      printWriter.write("\n")
    })
    printWriter.close()

    var finish = System.nanoTime() - start
    println("Duration:" + TimeUnit.SECONDS.convert(finish, TimeUnit.NANOSECONDS).toString)

  }

}