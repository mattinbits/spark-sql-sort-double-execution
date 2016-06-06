package example.doubleexecution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{TaskContext, Partition, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{TableScan, BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

import scala.util.Random

class DefaultSource extends RelationProvider with SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    createRelation(sqlContext, parameters, null)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    parameters.getOrElse("path", sys.error("'path' must be specified for our data."))
    return new LegacyRelation(parameters.get("path").get, schema)(sqlContext)
  }
}

class LegacyRelation(location: String, userSchema: StructType)
                    (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with TableScan
    with Serializable {
  override def schema: StructType = {
    if (this.userSchema != null) {
      return this.userSchema
    }
    else {
      return StructType(Seq(StructField("name", StringType, true),
        StructField("age", IntegerType, true)))
    }
  }

  override def buildScan(): RDD[Row] = new ReportingRDD(sqlContext.sparkContext)
}

case object MyPartition extends Partition {
  override def index: Int = 0
}

class ReportingRDD(sc: SparkContext)extends RDD[Row](sc, Nil) {

  println(s"RDD created with id ${this.id}")

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    println(s"${this.id} I am being computed")
    val it = Seq.fill(10000)(Row.fromTuple((Random.nextString(5), Random.nextInt(20)))).toIterator
    new Iterator[Row] {

      var i = 0

      override def hasNext: Boolean = it.hasNext

      override def next(): Row = {
        i = i+1
        if (i % 2000 == 0) println(s"Returned ${i} records so far")
        it.next()
      }
    }
  }

  override protected def getPartitions: Array[Partition] = Array(MyPartition)
}
