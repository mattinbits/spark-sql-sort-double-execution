package doubleexecution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}


object Main extends App {

  val config = new SparkConf().setAppName("testing provider").setMaster("local[*]").set("spark.sql.shuffle.partitions", "5")
  val sc = new SparkContext(config)
  val sqlContext = new HiveContext(sc)
  val df = sqlContext
    .read
    .format("example.doubleexecution")
    .load("foo")
  df.registerTempTable("my_table")

  println("##########################")
  println("Collecting a data-source without sorting")
  println("##########################")
  val unsorted = sqlContext.sql("SELECT * FROM my_table")
  unsorted.collect()

  println("##########################")
  println("Collecting a data-source with sorting")
  println("##########################")
  val sorted = sqlContext.sql("SELECT * FROM my_table ORDER BY name")
  sorted.collect()


  println("##########################")
  println("data-source to RDD without sorting")
  println("##########################")
  unsorted.rdd

  println("##########################")
  println("data-source to RDD with sorting")
  println("##########################")

  sorted.rdd
}
