package org.example.transformers

import breeze.linalg.roll
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{approx_count_distinct, asc_nulls_first, avg, col, corr, count, countDistinct, count_distinct, covar_pop, covar_samp, dense_rank, desc, first, kurtosis, last, log, max, mean, min, rank, round, skewness, stddev_pop, stddev_samp, sum, sum_distinct, var_pop, var_samp}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class Aggregations(spark: SparkSession) {
  def aggregationFunc(covidDF: DataFrame): Unit={
    val df: Dataset[Row] = covidDF.
      select(col("`WHO Region`"),
        col("`Confirmed`"),
        col("`Deaths`"),
        col("`Recovered`"),
        col("`Recovered / 100 Cases`"),
        col("`Deaths / 100 Cases`"))

    generalAggregation(df)
    mappedAgg(df)
    windowFunc(df)
    rollUp(df)
    cubeFunc(df)
    df.dropDuplicates("col_name")
  }


  def cubeFunc(df: Dataset[Row]): Unit={
    println("<<< CUBE FUNC >>>")
    df.write.bucketBy(10, "id").sortBy("id").format("parquet").save("")
    val newDF = df.
      cube(col("`WHO Region`"), col("`Recovered / 100 Cases`")).
      agg(sum("`Recovered`")).
      orderBy(asc_nulls_first("`WHO Region`"),
        asc_nulls_first("`Recovered / 100 Cases`"))

    newDF.select(col("`WHO Region`")).distinct().show()

    newDF.
      selectExpr("*").
      filter(col("`WHO Region`").isNull || col("`Recovered / 100 Cases`").isNull).
      show(false)

    newDF.
      groupBy(col("`WHO Region`"), col("`Recovered / 100 Cases`")).
      agg(count(col("`Recovered / 100 Cases`"))).
      filter(col("`WHO Region`").isNull || col("`Recovered / 100 Cases`").isNull).
      show(false)
  }

  def rollUp(df: DataFrame): Unit={
    val rollDF: Dataset[Row] = df.
      rollup(col("`WHO Region`"), col("`Recovered / 100 Cases`")).
      agg(count(col("`Recovered`"))).
      orderBy("`WHO Region`")

    rollDF.
      selectExpr("*").
      filter(col("`WHO Region`").isNull || col("`Recovered / 100 Cases`").isNull || col("`count(Recovered)`").isNull).
      show(false)

    rollDF.
      orderBy(asc_nulls_first("`WHO Region`"),
        asc_nulls_first("`Recovered / 100 Cases`")).
      show(100, false)
  }

  def windowFunc(df: Dataset[Row]): Unit ={
    val expr = Window.partitionBy(col("`WHO Region`")).orderBy("`Recovered`")
    val exprRank = dense_rank().over(expr)
    df.
      select(
        col("`WHO Region`"),
        col("Recovered"),
        col("Deaths"),
        round(avg(col("Deaths")).over(expr), 2).alias("windowAvgDeathsRegionWise"),
        exprRank.alias("rank")
      ).
      show(false)
  }

  def mappedAgg(df: Dataset[Row]): Unit ={
    df.
      groupBy("`WHO Region`").
      agg("Confirmed" -> "avg", "Confirmed" -> "count").
      show(false)
  }

  def generalAggregation(df: Dataset[Row]): Unit ={
    val df_all = df.
      select(
        min(col("`Confirmed`")).alias("MIN"),
        max(col("`Confirmed`")).alias("MAX"),
        avg(col("`Confirmed`")).alias("AVG"),
        mean(col("`Confirmed`")).alias("MEAN"),
        sum(col("`Confirmed`")).alias("SUM"),
        count("`Confirmed`").alias("COUNT"), // this count performs lazy evaluation unlike df.count
        first(col("`Confirmed`")).alias("FIRST"),
        last(col("`Confirmed`")).alias("LAST"),
        sum_distinct(col("`Confirmed`")).alias("SUM DISTINCT"),
        count_distinct(col("`Confirmed`")).alias("COUNT DISTINCT"),
        approx_count_distinct(col("`Confirmed`")).alias("APPROX COUNT DISTINCT"),
        skewness(col("`Confirmed`")).alias("SKEWNESS"),
        kurtosis(col("`Confirmed`")).alias("KURTOSIS"),
        stddev_samp(col("`Confirmed`")).alias("STD DEV SAMP"),
        stddev_pop(col("`Confirmed`")).alias("STD DEV POPU"),
        var_pop(col("`Confirmed`")).alias("VARIANCE POPU"),
        var_samp(col("`Confirmed`")).alias("VARIANCE SAMP"),
        corr(col("`Confirmed`"), col("`Deaths`")).alias("CORR"),
        covar_pop(col("`Confirmed`"), col("`Deaths`")).alias("COVAR POPU"),
        covar_samp(col("`Confirmed`"), col("`Deaths`")).alias("COVAR SAMP")
      )
    val totalColumns: Int = df_all.columns.size
    val numFirstHalfColumns = totalColumns/2
    val numSecondHalfColumns = totalColumns - numFirstHalfColumns
    val allColumns = df_all.columns
    val firstHalfColumns = allColumns.take(numFirstHalfColumns).map(c => col(c))
    val secondHalfColumns = allColumns.takeRight(numSecondHalfColumns).map(c => col(c))

    println("<<< First DF >>>")
    df_all.select(firstHalfColumns: _*).show(false)
    df_all.select("").collect.map(x => x)

    println("<<< Right DF >>>")
    df_all.select(secondHalfColumns: _*).show(false)
  }
}
