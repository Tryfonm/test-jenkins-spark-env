import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession
      .builder()
      .appName("SyntheticData")
      .master("local[*]")
      .getOrCreate()

    val numRows = 100
    val syntheticData: DataFrame = generateSyntheticData(spark, numRows)

    syntheticData.show()

    // Add a rank column based on salary
    val orderedData = syntheticData
      .orderBy(desc("Salary"))
      .withColumn("HighSalary", col("Salary") >= 50000)

    val groupedByAgeOrdered = orderedData
      .groupBy("age")
      .agg(
        avg("Salary")
          .cast("bigint")
          .as("AverageSalary")
      )
      .orderBy(desc("AverageSalary"))

    groupedByAgeOrdered.show()
    groupedByAgeOrdered.write
      .format("parquet")
      .mode("overwrite")
      .save("./src/main/output/")
    spark.stop()

  }

  def generateSyntheticData(spark: SparkSession, numRows: Int): DataFrame = {
    import spark.implicits._

    (1 to numRows)
      .toDF("ID")
      .withColumn("Name", concat(lit("User_"), col("ID")))
      .withColumn("Age", (rand() * 50 + 18).cast("int"))
      .withColumn("Salary", (rand() * 50000 + 30000).cast("bigint"))

  }
}
