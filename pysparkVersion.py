from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg, concat, lit, rand

def generate_synthetic_data(spark, num_rows):
    return spark.range(1, num_rows + 1)\
        .withColumn("Name", concat(lit("User_"), col("id")))\
        .withColumn("Age", (rand() * 50 + 18).cast("int"))\
        .withColumn("Salary", (rand() * 50000 + 30000).cast("bigint"))

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("SyntheticData").getOrCreate()

    num_rows = 100
    synthetic_data = generate_synthetic_data(spark, num_rows)

    print("\n---------------\nOriginalDataset")
    synthetic_data.show()

    # Add a rank column based on salary
    ordered_data = synthetic_data\
        .orderBy(desc("Salary"))\
        .withColumn("HighSalary", col("Salary") >= 50000)

    grouped_by_age_ordered = ordered_data\
        .groupBy("Age")\
        .agg(avg("Salary").cast("bigint").alias("AverageSalary"))\
        .orderBy(desc("AverageSalary"))

    print("\n----------------\nProcessedDataset")
    grouped_by_age_ordered.show()

    grouped_by_age_ordered.write\
        .format("parquet")\
        .mode("overwrite")\
        .save("./output/")

    spark.stop()
