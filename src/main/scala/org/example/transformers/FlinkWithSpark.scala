package org.example.transformers

import org.apache.flink.streaming.api.scala._

class FlinkWithSpark {

  def runFlinkJob(): Unit = {
    // 1. Set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. Read data (Mocking a source here using fromElements)
    // In a real scenario, this could be Kafka, a file, etc.
    val sourceData: DataStream[String] = env.fromElements(
      "Apache Flink is a stream processing framework",
      "Apache Spark is a unified analytics engine",
      "Flink excels at stateful stream processing",
      "Spark is great for batch and micro-batch"
    )

    // 3. Transform data (e.g., Word Count)
    val transformedData: DataStream[(String, Int)] = sourceData
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    // 4. Load/Sink data (Printing to standard output here)
    // In a real scenario, this could be writing back to Kafka, a database, or a file.
    transformedData.print()

    // 5. Execute the Flink job
    env.execute("Flink Word Count Example")
  }
}
