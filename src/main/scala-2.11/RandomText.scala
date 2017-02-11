/**
  * Created by marti on 09/02/2017.
  */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random



object RandomText {

  var selectedLines = 10
  var textParsed = false
  var lines = new ArrayBuffer[String]
  var arrayWords = new ArrayBuffer[String]()
  var numBytesToWrite = 2000
  var items : Int = 0


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
          .setMaster("local[*]")
             .setAppName("Random text generator")
                .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    val start = System.currentTimeMillis()

    println("Job Started :" + start)

    val list = sc.textFile(args(0))

    val counts = list.flatMap(line => line.split("\n"))


    while (numBytesToWrite > 0) {
      val words = counts.takeSample(true, 16, System.nanoTime.toInt)

      val line = new StringBuilder()

      arrayWords += words.addString(line, ", ").toString()

      arrayWords.foreach(println)
     /*
      val text = sc.makeRDD(Seq(line))            //sc.parallelize(Seq(line))

      text.foreach(println)

      val hadoopConfig = new Configuration()
      val hdfs = FileSystem.get(hadoopConfig)
      val sourcePath = new Path(args(1))
      val outputPath = new Path(args(1))
      if (hdfs.exists(outputPath)) {
        hdfs.delete(outputPath, true)
      }
      */
      //text.saveAsTextFile(sourcePath.toString)

      // merge
      // FileUtil.copyMerge(hdfs, sourcePath, hdfs, outputPath, true, hadoopConfig, null)

      numBytesToWrite -= words.length

      items += 1

      if (items % 200 == 0) {
        println("Wrote " + items +", " + numBytesToWrite + " bytes left")
      }

    }

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val outputPath = new Path(args(1))
    if (hdfs.exists(outputPath)) {
      hdfs.delete(outputPath, true)
    }

    val out = sc.parallelize(List(arrayWords))

    out.repartition(3).saveAsTextFile(outputPath.toString)

    val end = System.currentTimeMillis()

    println("Job Ended :" + end)
    println("Done with " + items + " records")
    println("The job took " + (end - start) / 1000  + " seconds" )

    sc.stop()

  }

  def merge(srcPath: String, dstPath: String, fileName: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val destinationPath = new Path(dstPath)
    if (hdfs.exists(destinationPath)) {
      hdfs.delete(destinationPath, true)
    }
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath + "/" + fileName), false, hadoopConfig, null)
  }


  def saveAsTextFileAndMerge[T](hdfsServer: String, fileName: String, rdd: RDD[T]): Unit = {
    val random = new Random()
    val sourceFile = hdfsServer + "/tmp/" + random.nextInt(200000)
    rdd.saveAsTextFile(sourceFile)
    val dstPath = hdfsServer + "/final/"
    merge(sourceFile, dstPath, fileName)
  }
































}
