// Databricks notebook source
// MAGIC %md
// MAGIC # Read Customers CSV from DBFS

// COMMAND ----------



// Import necessary libraries
import org.apache.spark.sql.SparkSession

// Initialize Spark session (In Databricks, it's already initialized)
val spark = SparkSession.builder().appName("CustomerDataFrame").getOrCreate()

// Path to the uploaded CSV file in DBFS
val csvFilePath = "dbfs:/FileStore/tables/customers.csv"

// Read CSV file into DataFrame
val customerDF = spark.read
  .format("csv")
  .option("header", "true")   // CSV file has a header row
  .option("inferSchema", "true")  // Automatically infer data types
  .load(csvFilePath)

// Show the DataFrame content
customerDF.show()


// COMMAND ----------

// MAGIC %md
// MAGIC # Doing minor transformation using Scala and creating new Dataframe

// COMMAND ----------

import org.apache.spark.sql.functions.{col, when}

// Step 3: Transform - Apply Transformations
val transformedDf = customerDF
  .filter(col("age") > 18)  // Filter rows where age > 18
  .withColumn("age_group", when(col("age") < 30, "Young").otherwise("Old"))  // Add a new column based on age
  .select("customer_name", "age", "age_group")  // Select only specific columns

  transformedDf.show()

// COMMAND ----------

// MAGIC %md
// MAGIC # Transformed dataframe is converted to csv and store in DBFS

// COMMAND ----------

// Step 4: Load - Write the Transformed Data to CSV
val outputFilePath = "dbfs:/FileStore/tables/customers_transformed.csv"
transformedDf.write
  .format("csv")
  .option("header", "true")  // Write header to the CSV
  .mode("overwrite")  // Overwrite existing file if it exists
  .save(outputFilePath)


