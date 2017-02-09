/**
  * Created by marti on 09/02/2017.
  */

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random



object RandomText {

  var selectedLines = 10
  var textParsed = false
  var lines = new ArrayBuffer[String]
  var words = new ArrayBuffer[String]


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
          .setMaster("local[*]")
             .setAppName("Random text generator")
                .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    //generateSentence

    //val list = sc.parallelize(Seq("C:\\Users\\marti\\Documents\\IdeaProjects\\big-data-spark\\src\\main\\resources\\parseable.txt"))

    val list = sc.textFile(args(0))

    val counts = list.flatMap(line => line.split("\n"))
          //.map(word => (word, 1))
            //.reduceByKey(_+_)
    var random = new Random()
    for (i <- 0 until counts.take(20).size) {
      var index = random.nextInt(counts.take(60).size)
      words += counts.collect().apply(index)
    }

    words.foreach(println)

    counts.foreach(println)
    counts.saveAsTextFile("random_text")

    sc.stop()

  }

  /*
  def sentence: Unit = {
    selectedLines = 10
    generateSentence

  }


  def getWords = words

  def generateSentence: Unit = {
    var random = new Random
    if(!textParsed) {getLines; textParsed = true}
    for (i <- 0 until selectedLines) {
      var index = random.nextInt(lines.length)
      words += lines(index)
      lines.remove(index)
    }

  }

  def getLines: Unit = {
    for (line <- Source.fromFile("C:\\Users\\marti\\Documents\\IdeaProjects\\big-data-spark\\src\\main\\resources\\parseable.txt").getLines()){
      lines += line
    }
  }

*/




}
