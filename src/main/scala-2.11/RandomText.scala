/**
  * Created by marti on 09/02/2017.
  */

import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random



object RandomText {

  var numBytesToWrite = 30 * Math.pow(1024, 2)
  var items : Int = 0

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Random text generator")
    .set("spark.executor.memory", "2g")

  val sc = new SparkContext(conf)


  def main(args: Array[String]): Unit = {



    val start = System.currentTimeMillis()

    println("Job Started :" + start)

    val list = sc.textFile(args(0))

    val counts = list.flatMap(line => line.split("\n"))//.coalesce(10, true)


    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    /*val outputPath = new Path(args(2))
    if (hdfs.exists(outputPath)) {
      hdfs.delete(outputPath, true)
    }
    */


    //val out = hdfs.create(outputPath)

    for (a <- 0 until (10) ) {
      val random = new Random()
      val file = new File(args(1) + "file" + a + ".txt")
      file.createNewFile()
      val writer = new PrintWriter(new FileOutputStream(file, true))
      do {
        items += 1
        writer.write(counts.takeSample(false, 3000, System.nanoTime()).mkString(" ") + ".\n")
      } while (file.length() < numBytesToWrite)
      writer.close()
    }

    val end = System.currentTimeMillis()

    println("Job Ended :" + end)
    println("Done with " + items + " records")
    println("The job took " + (end - start) / 1000  + " seconds" )

    sc.stop()

  }


  def addFile(source : String, dest : String, conf : Configuration): Unit = {

    val fileSystem = FileSystem.get(conf)

    // Get the filename out of the file path
    val filename = source.substring(source.lastIndexOf('/') + 1, source.length)

    var dst : String = null

    // Create the destination path including the filename.
    if(dest.charAt(dest.length() - 1) != '/') {
      dst = dest + "/" + filename
    } else {
      dst = dest + filename
    }

    // Check if te file already exists
    val path = new Path(dst)
    if (fileSystem.exists(path)) {
      return ;
    }

    // Create a new file and write to it.
    val out = fileSystem.create(path)
    val in = new BufferedInputStream(new FileInputStream(new File(source)))

    val b = new Array[Byte](1024)
    var numBytes = 0
    while (in.read(b) > 0) {
      numBytes = in.read(b)
      out.write(b, 0, numBytes)
    }


  }

}
