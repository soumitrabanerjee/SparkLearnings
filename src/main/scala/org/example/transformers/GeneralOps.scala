package org.example

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, corr, count, desc, explode, expr, from_json, lit, monotonically_increasing_id, split, struct, sum, to_json}
import org.apache.spark.sql.types.StringType
import org.example.Schemas.jsonSchema.myStructSchema
import org.example.supportiveClasses.SparkUDF

class GeneralOps(spark: SparkSession) {
  def covidInIndia(indiaDF: DataFrame): Unit={
    indiaDF.printSchema()
    val filterDeaths = col("Deaths") > 100 && col("Deaths") < 1000
    val sortExpr = desc("Deaths")
    val exp = expr("`Country/Region` = 200")

    val deathsDF = indiaDF.
      select("1 week change", "Deaths").
      filter(filterDeaths).
      sort(sortExpr).
      filter(exp)

    deathsDF.withColumn("temp", sum("`1 week change`").over(Window.partitionBy("Country/Region")))
    deathsDF.sample(false, 0.5, 5)

    val complexStruct = indiaDF.select(col("`Country/Region`"), col("Deaths"), struct(col("Deaths"), col("Country/Region")).alias("complexCovid"))
    complexStruct.select(col("Country/Region"), struct(col("Deaths"), col("complexCovid").getField("Deaths"))).show

    val complexArray = indiaDF.select(split(col("`WHO Region`"), " "))
    complexArray.show

//    val complexMap = indiaDF.select(map(col("`WHO Region`"), col("`Country/Region`")))

    indiaDF.select(corr(col("`Recovered / 100 Cases`"), col("`Deaths / 100 Cases`"))).show()

    val complexDF = indiaDF.select(split(col("`WHO Region`").
      cast(StringType), " ").
      alias("complex_array")).
      withColumn("exploded", explode(col("complex_array"))).
      show(false)

    val jsonParse: Dataset[Row] = indiaDF.
      select(col("`Country/Region`"), col("Deaths"), col("`WHO Region`"), split(col("`WHO Region`"), " ").alias("complex_arr")).
      selectExpr("(`Country/Region`, Deaths, `WHO Region`, `complex_arr`)  as myStruct").
      select(to_json(col("`myStruct`")).alias("newJSON")).
      select(from_json(col("newJSON"), myStructSchema).alias("parseJson"), col("newJSON"))

    jsonParse.printSchema
    jsonParse.show(false)

    val getSummation = spark.udf.register("getSummation", SparkUDF.getSummation(_:Double, _:Double):Double)
//    val getNumbers = spark.udf.register("getNumbers", SparkUDF.getNumbers)

    indiaDF.
      select(
        col("`Recovered / 100 Cases`"),
        col("`Deaths / 100 Cases`"),
        getSummation(col("`Recovered / 100 Cases`"), col("`Deaths / 100 Cases`")).alias("summation"),
        monotonically_increasing_id().alias("id")).
      show(false)
  }
}
