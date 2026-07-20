package org.example.transformers

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.example.Schemas.covidDataSchemas.countrySchema
import org.example.Util.{Path, Util}

class UDF(spark: SparkSession) extends Serializable {
  val filepath: Path = Path()
  val Util = new Util()
  def createUDF(): Unit={
    val df = spark.
      read.
      format("csv").
      option("header", true).
      schema(countrySchema).
      load(filepath.countryFilePath)

    val selectedDF: Dataset[Row] = df.select(col("Country/Region"), col("Confirmed"), col("Deaths"))

    val calculateRisk = spark.udf.register("calculateCountryAtRisk", Util.calculateCountryAtRisk(_: Int): String)
    val counter = spark.udf.register("counter", Util.counter)
    val finalDF = selectedDF.
      withColumn("isAtRisk", calculateRisk(col("Deaths"))).
      withColumn("counter", counter())
    finalDF.show
  }
}
