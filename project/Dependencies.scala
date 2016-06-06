import sbt._

object Dependencies {

  val sparkV = "1.6.1"

  val spark = "org.apache.spark" %% "spark-core" % sparkV
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkV
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkV
}