package org.example.transformers

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, first, lit, reduce, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.example.Schemas.covidDataSchemas
import org.example.Util.Path

case class data(id: Int, name: String)

class DataManipulation(spark: SparkSession) {
  def pivotUnpivot(): Unit={
    val path = Path()
    val sourceDF = spark.read.format("csv").option("header", true).
      schema(covidDataSchemas.countrySchema).
      load(path.dataManipulationFilePath)
    val  df = sourceDF.select(col("`Country/Region`"), col("`WHO Region`"), col("`Confirmed`"), col("`Deaths`"), col("`Active`"))

//    val runningDF = df.groupBy(col("`WHO Region`")).pivot(col("`Country/Region`")).agg(first(col("Confirmed")))
    val res = runningSum(df)
    res.show(false)

    val schema = new StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    ))
    import spark.implicits._
    val df1 = spark.sparkContext.parallelize(Seq(Row((1,"aa"), Row(2, "ab"), Row(3, "ac"), Row(4, "ad"), Row(1, "aa"), Row(1, "aa"), Row(3, "ac"), Row(3, "ad"), Row(3, "am"))))
    val df2 = spark.sparkContext.parallelize(Seq.empty[{}])

    val temp = spark.sparkContext.parallelize(Array((1,"aa"), (2, "ab"), (3, "ac"), (4, "ad"), (1, "aa"), (1, "aa"), (3, "ac"), (3, "ad"), (3, "am")))

    val ds = spark.createDataFrame(df1, schema)
//    val d = ds.filter(x => x.id != 2).map(x => x)
//    ds.map(x => (1,x))
//    ds.groupByKey(x => x.id!=2)
//    ds.reduce((x,y) => (x+y))
//
//    val rdd1 = ds.rdd.keyBy(x => (x.id, x))
//    rdd1.groupByKey()
//    rdd1.groupBy(x => x._1).reduce((x,y) => x+y)

    val df11 = spark.range(5).toDF.withColumn("country", lit("USA"))
    val df22 = spark.range(10).toDF.withColumn("country", lit("India"))
    df11.join(df22, df11.col("id1")===df22.col("id"), "left")

  }

  def runningSum(df: DataFrame): DataFrame={
    val winSpec = Window.partitionBy(col("`WHO Region`")).orderBy(col("Confirmed"))
    val sol = df.withColumn("running_sum", sum(col("Confirmed")).over(winSpec))
    sol
  }
}
