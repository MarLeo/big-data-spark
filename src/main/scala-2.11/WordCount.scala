/**
  * Created by marti on 08/02/2017.
  */

import org.apache.spark.{SparkConf, SparkContext}
// for spark context and conf


object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf() // Defining spark configuration object
      .setMaster("local[*]") // Setting spark as local Master
      .setAppName("word count") // Spark Application name
      .set("spark.executor.memory", "2g") // Setting spak execution memory size


    val sc = new SparkContext(conf) // Actual starting execution point

    val lines = sc.parallelize(Seq("This is first line", "This is second line", "This is third line"))
    // Defining parallelize action on spark context

    val counts = lines.flatMap(line => line.split(" ")) // flatmap operation
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.foreach(println) // Count word and display
    counts.saveAsTextFile("WordCount-Results")

  }


}
