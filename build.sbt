import Dependencies._

name := "spark-sql-sort-double-execution"

version := "1.0"

scalaVersion := "2.10.6"


fork in run := true
javaOptions in run := Seq("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005")

cancelable in Global := true

libraryDependencies ++= Seq(
  spark, sparkSql, sparkHive)
    