package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .getOrCreate()

  // CREATING SCHEMAS / STRUCT TYPE
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")


  /*
  READING ANY DF FROM ANY DATA SOURCE:
  - format:
  - such as json file formats

  - schema:
  - optional but can be enforced with the schema() function which takes a schema Struct type as arg)
  - OR if no schema: use option() method with args: inferSchema true (if not schema)

  - option:
  - if schema: use option() with other args:
    - option(mode, arg): decides what spark should do if enoucntering malformed record
                         eg: json does not conform to the schema / malformed json
    - args for mode include:
      - failFast: throws an exception for spark if we encounter malinformed record
      - dropMalformed: ignores faulty rows
      - permissive: default action if no arg specified

  - load:
  - can be a file locally or even S3 bucket in Amazon
   */

  // instead of doing it this way we can use optionMap
  // advantage: allows computing options dynamically at runtime
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferScheme" -> "true"
    ))
    .load()

  // WRITING DATAFRAMES
  /*
  READING ANY DF FROM ANY DATA SOURCE:
  - format:
  - such as json file formats

  - save mode:
  - what to do if a file already exists on the file we are writing to
  - options include: overwrite, append, ignore, errorIfExists

  - path:
  - file we are writing to

  - option:
  - zero or more options
  - advanced options for sorting, bucketing etc
   */
  // Clear any potential cached metadata
  spark.catalog.clearCache()

  // Write to a clean path
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe_clean.json")  // Ensure this path is new and clean

  spark.stop()







}
