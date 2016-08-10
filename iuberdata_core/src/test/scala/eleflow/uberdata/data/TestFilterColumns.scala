package eleflow.uberdata.data

import eleflow.uberdata.core.data.Dataset
import org.apache.spark.rpc.netty.BeforeAndAfterWithContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by dirceu on 22/10/14.
 */
class TestFilterColumns extends FlatSpec with Matchers with BeforeAndAfterWithContext {

  import Dataset._

  "FilterColumns" should
    "handle included string columns" in {
    @transient val uberContext = context
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("id", IntegerType, false), StructField("int", IntegerType, false),
      StructField("string2", StringType, false), StructField("double", DoubleType, false)))
    val data = List(
      Row(1, 5, "vlr1", 10.5),
      Row(2, 1, "vl3", 0.1),
      Row(3, 8, "vl3", 10.0))
    val rdd = sc.parallelize(data)
    val schema = sqlContext.applySchema(rdd, structType)
    val result = schema.sliceByName(Seq("id", "string2"),Seq.empty)

    assert(result.collect.deep == Array(new GenericRow(Array(1, "vlr1")), new GenericRow(Array(2, "vl3")),new GenericRow( Array(3, "vl3"))).deep)
  }

  it should "correct handle excluded string columns" in {
    @transient val uberContext = context
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("id", IntegerType, false), StructField("int", IntegerType, false),
      StructField("string2", StringType, false), StructField("double", DoubleType, false)))

    val data = List(
      Row(1, 5, "vlr1", 10.5),
      Row(2, 1, "vl3", 0.1),
      Row(3, 8, "vl3", 10.0))
    val rdd = sc.parallelize(data)
    val schema = sqlContext.applySchema(rdd, structType)
    val result = schema.sliceByName(excludes =  Seq("id", "double"))
    assert(result.collect.deep == Array(new GenericRow(Array(5, "vlr1")),new GenericRow(Array( 1, "vl3")),new GenericRow(Array( 8, "vl3"))).deep)
  }

  it should "handle included int columns" in {
    @transient val uberContext = context
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("id", IntegerType, false), StructField("int", IntegerType, false),
      StructField("string2", StringType, false), StructField("double", DoubleType, false)))
    val data = List(
      Row(1, 5, "vlr1", 10.5),
      Row(2, 1, "vl3", 0.1),
      Row(3, 8, "vl3", 10.0))
    val rdd = sc.parallelize(data)
    val schema = sqlContext.applySchema(rdd, structType)
    val result = schema.slice(Seq(2, 3))
    assert(result.collect.deep == Array(new GenericRow(Array("vlr1", 10.5)), new GenericRow(Array("vl3", 0.1)),new GenericRow( Array("vl3", 10.0))).deep)
  }

  it should "correct handle excluded int columns" in {
    @transient val uberContext = context
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("id", IntegerType, false), StructField("int", IntegerType, false),
      StructField("string2", StringType, false), StructField("double", DoubleType, false)))

    val data = List(
      Row(1, 5, "vlr1", 10.5),
      Row(2, 1, "vl3", 0.1),
      Row(3, 8, "vl3", 10.0))
    val rdd = sc.parallelize(data)
    val schema = sqlContext.applySchema(rdd, structType)
    val result = schema.slice(excludes = Seq(0, 3))
    assert(result.collect.deep == Array(new GenericRow(Array(5, "vlr1")),new GenericRow(Array( 1, "vl3")),new GenericRow(Array( 8, "vl3"))).deep)
  }
}

