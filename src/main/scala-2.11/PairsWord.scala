import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by marti on 11/02/2017.
  */


/* Custom TextOuput Format */
class RDDMultipleTextOutputFormat extends  MultipleTextOutputFormat[Any, Any] {

  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    return key.asInstanceOf[String] + "-" + name
}



object PairsWord {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("words pairs occurences")
    .set("spark.executor.memory", "2g")
  // context
  val sc = new SparkContext(conf)

  var num : Int = 0

  def main(args: Array[String]): Unit = {

    // input and output paths
    val inputPath = new Path(args(0))
    val outputPath = new Path(args(1))

    // start time
    val start = System.currentTimeMillis()
    println("Job Started :" + start)

    // input text files
    val data =  sc.wholeTextFiles(inputPath.toString) //sc.textFile(inputPath.toString)

    // final output RDD
    var output : org.apache.spark.rdd.RDD[(String, String)] = sc.emptyRDD

    // list of files to process
    val files = data.map{ case (file, content) => file}

    // apply create pairs on each files
    files.collect().foreach( fileName => { output = output.union(CreatePairs(fileName))  } )

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    if (hdfs.exists(outputPath)) {
      hdfs.delete(outputPath, true)
    }


  //  files.collect().foreach(filename => { createPairs(filename, outputPath.toString)})

    // custom output format
    output.saveAsHadoopFile(outputPath.toString, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])


    val end = System.currentTimeMillis()

    println("Job Ended :" + end)
    println("The job took " + (end - start) / 1000  + " seconds" )

    sc.stop()
  }



  object CreatePairs extends Serializable {

    def apply(file : String): org.apache.spark.rdd.RDD[(String, String)] = {

    val path = file.split('/').last

    val content = sc.textFile(file)

    val pairs = content.map(line => line.split(" "))
      .flatMap(value => value.sliding(2))
      .map(list => list(0) -> list(1))

    val result = pairs.map(item => item -> 1)
      .reduceByKey(_+_)

    val output = result.map(out => (path, out._1 + " " + out._2))

    output
    }
  }

  def createPairs(file : String, path : String): Unit = {

    val content = sc.textFile(file)

    val pairs = content.map(line => line.split(" "))
      .flatMap(value => value.sliding(2))
      .map(list => list(0) -> list(1))

    val result = pairs.map(item => item -> 1)
      .reduceByKey(_+_)

    num += 1

    val _path = path + num
    val out = FileSystem.get(new Configuration()).create(new Path(_path))
   // out.writeBytes(result.toString())
    result.saveAsTextFile(out.toString())

  }



}
