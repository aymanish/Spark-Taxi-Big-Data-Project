package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

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
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  // builder creational pattern
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
  //carsDF.write
  //  .format("json")
  //  .mode(SaveMode.Overwrite)
  //  .save("src/main/resources/data/cars_dupe_clean.json")  // Ensure this path is new and clean


  // DATA SOURCES P2:

  // JSON FLAGS:
  // if we have date types spark can format it
  // BUT we need to specify a schema
  // - if we specify a schema with a date type
  // - we need to specify the date format under option
  // also applicable to time stamp formats
  // SPARK DATE SENSITIVE:
  // - if either the format specified within option()
  // - or the actual date within the date is in the wrong format
  // - spark creates a null value instead in the dataframe when parsin
  spark.read
    .format("json")
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd")
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") //bzip2 //gzip //lz4 //snappy //deflate
    .load("src/main/resources/data/cars.json")

  // instead of adding .format and .load at the start and end
  // - use .json("path") at the end -> first class json method




  // CSV FLAGS:
  // has the most possible flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))
  /*
    spark.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true") // ignores first row of column names
    .option("sep", ",") // seperator for the values
    .option("nullValue", "") // parse target value as null since csv has no null
    .load("src/main/resources/data/cars.csv")

   */

  // instead of adding .format and .load at the start and end
  // - use .csv("path") at the end -> first class json method

  // Parquet
  // - open source compressed binary storage format
  // - optimised for fast reading of columns
  // -  default storage format for dataframes

  // - advantage: very predictable so dont need many options
  //carsDF.write
    //.mode(SaveMode.Overwrite)
    //.parquet("src/main/resources/data/cars.parquet")
  //.save("src/main/resources/data/cars.parquet")
  // save and parquet() are the same as its the default format so also dont need format()


  // Text files
  // every single line is considered a value in a single column
  //spark.read.text("src/main/resources/data/cars.parquet")

  // READING FROM A REMOTE DB:
  // docker compose up -> spin up docker container to accept DB connections
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load() // no path since remote

  employeesDF.show()


  // Exercise 1:
  // Read the movies DF | DONE
  // write it as:
  // - tab seperated values file / csv
  // - snappy parquet
  // - table public.movies in the postgres DB
  val newMoviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") //bzip2 //gzip //lz4 //snappy //deflate
    .load("src/main/resources/data/movies.json")

  newMoviesDF.show()
  newMoviesDF.printSchema()

  // WRITE AS CSV
  //val newMoviesDFCSV = newMoviesDF.write
  //  .format("csv")
  //  .mode(SaveMode.Overwrite)
  //  .option("header", "true")
  //  .option("sep", " \t") // separator for the values in csv
  //  .save("src/main/resources/data/moviesNew.csv")

  val newMoviesDFRemote = newMoviesDF.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save() // no path since remote



  // WRITE AS SNAPPY
  //val newMoviesDFParquet = newMoviesDF.write
  //  .parquet("src/main/resources/data/cars.parquet")

  spark.stop()


}
