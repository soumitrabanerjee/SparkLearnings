package org.example.App

import org.example.transformers.FlinkWithSpark

object FlinkApp {
  def main(args: Array[String]): Unit = {
    val flinkJob = new FlinkWithSpark()
    flinkJob.runFlinkJob()

//    Here's a sample Spark Scala DataFrame with 10 student rows:
//      scalaimport org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.Row

    val spark = SparkSession.builder()
      .appName("StudentDataFrame")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // -------------------------------------------------------------------
    // Option 1: Using case class + toDF() — simplest approach
    // -------------------------------------------------------------------
    case class Student(
                        student_id : Int,
                        name       : String,
                        age        : Int,
                        gender     : String,
                        course     : String,
                        grade      : String,
                        score      : Double,
                        city       : String,
                        is_active  : Boolean,
                        year       : Int
                      )

    val students = Seq(
      Student(1,  "Alice Johnson",  20, "F", "Data Engineering", "A",  92.5, "New York",    true,  2024),
      Student(2,  "Bob Smith",      22, "M", "Computer Science", "B+", 85.0, "San Francisco",true, 2023),
      Student(3,  "Clara Davis",    21, "F", "Machine Learning", "A+", 97.3, "Chicago",      true,  2024),
      Student(4,  "David Lee",      23, "M", "Data Science",     "B",  78.4, "Austin",       false, 2022),
      Student(5,  "Eva Martinez",   20, "F", "Statistics",       "A",  91.0, "Seattle",      true,  2024),
      Student(6,  "Frank Wilson",   24, "M", "Data Engineering", "C+", 65.2, "Boston",       true,  2021),
      Student(7,  "Grace Kim",      22, "F", "Computer Science", "A-", 88.7, "Los Angeles",  true,  2023),
      Student(8,  "Henry Brown",    21, "M", "Machine Learning", "B-", 72.1, "Denver",       false, 2023),
      Student(9,  "Isla Thompson",  20, "F", "Data Science",     "A",  94.6, "Portland",     true,  2024),
      Student(10, "James Garcia",   23, "M", "Statistics",       "B+", 83.9, "Houston",      true,  2022)
    )


    // -------------------------------------------------------------------
    // Option 2: Using explicit schema + Row — useful for dynamic schemas
    // -------------------------------------------------------------------
    val schema = StructType(Seq(
      StructField("student_id", IntegerType,  nullable = false),
      StructField("name",       StringType,   nullable = false),
      StructField("age",        IntegerType,  nullable = true),
      StructField("gender",     StringType,   nullable = true),
      StructField("course",     StringType,   nullable = true),
      StructField("grade",      StringType,   nullable = true),
      StructField("score",      DoubleType,   nullable = true),
      StructField("city",       StringType,   nullable = true),
      StructField("is_active",  BooleanType,  nullable = true),
      StructField("year",       IntegerType,  nullable = true)
    ))

    val rows = Seq(
      Row(1,  "Alice Johnson",   20, "F", "Data Engineering", "A",  92.5, "New York",     true,  2024),
      Row(2,  "Bob Smith",       22, "M", "Computer Science", "B+", 85.0, "San Francisco",true,  2023),
      Row(3,  "Clara Davis",     21, "F", "Machine Learning", "A+", 97.3, "Chicago",      true,  2024),
      Row(4,  "David Lee",       23, "M", "Data Science",     "B",  78.4, "Austin",       false, 2022),
      Row(5,  "Eva Martinez",    20, "F", "Statistics",       "A",  91.0, "Seattle",      true,  2024),
      Row(6,  "Frank Wilson",    24, "M", "Data Engineering", "C+", 65.2, "Boston",       true,  2021),
      Row(7,  "Grace Kim",       22, "F", "Computer Science", "A-", 88.7, "Los Angeles",  true,  2023),
      Row(8,  "Henry Brown",     21, "M", "Machine Learning", "B-", 72.1, "Denver",       false, 2023),
      Row(9,  "Isla Thompson",   20, "F", "Data Science",     "A",  94.6, "Portland",     true,  2024),
      Row(10, "James Garcia",    23, "M", "Statistics",       "B+", 83.9, "Houston",      true,  2022),
      Row(11,  "Grace Kim",       22, "F", "Computer Science", "A-", 88.7, "Los Angeles",  true,  2023),
      Row(12,  "Bob Smith",       22, "M", "Computer Science", "B+", 85.0, "San Francisco",true,  2023),
      Row(13,  "Frank Wilson",    24, "M", "Data Engineering", "C+", 65.2, "Boston",       true,  2021),
      Row(4,  "Frank Wilson",    24, "M", "Data Engineering", "C+", 65.2, "Boston",       true,  2021)
    )

  }
}
