package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, current_date, current_timestamp, datediff, expr, size, split, struct, to_date}

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


  // DATES
  // - how to operate on dates when its already in String format due to inferSchema
  // string column to date column
  val moviesWithReleaseDates = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))

  moviesWithReleaseDates
    .withColumn("Today", current_date()) // current_date() returns todays column
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)
    .show() // we also have date_add, date_sub -> add or subtract days from dates

  // DATE CONVERSION ERROR NOTES:
  // - date conversion to_date() -> 20 can be interpreted as 1920 or 2020
  // - some dates in the data could be in different format
  //   this can result in to_date() conversion to result in null value in the col


  // above adds new col
  // that converts release data and its format to actual date format

  // EXERCISES:
  // 1) how do we deal with multiple date formats?
  // - parse the df multiple times with with all the formats identified
  // - then union the small DFs

  // - not really feasible with large datasets as we would need multiple parses
  // - instead if its only a small portion just remove that data

  // 2) Read the stocks df and parse the dates
  val stocksDF = spark.read
    .option("inferSchema", "true")
    //.option("dateFormat", "MMM dd YYYY")
    .option("header", "true") // ignores first row of column names
    //.option("sep", ",") // separator for the values
    .csv("src/main/resources/data/stocks.csv")

  val stocksParsedDatesDF = stocksDF
    .withColumn("Actual_Date", to_date(col("date"), "MMM dd yyyy")) // add parse dates

  stocksParsedDatesDF.show()


  // STRUCTURES:

  // 1 - col operators
  // - MULTIPLE COLUMNS IN ONE COLUMN IN THE FORM OF AN ARRAY
  moviesDF
    // struct column profit = us cross and worldwide gross
    .select(col("Title"), struct(col("Worldwide_Gross"), col("US_Gross")).as("Profit"))
    // we can get cols from the struct as well
    // struct col.getField(sub col name).as(new name)
    .select(col("Title"), col("Profit").getField("US_Profit").as("US_profit"))
    .show()

    // 2 - expressions
  moviesDF
    // struct column profit = us cross and worldwide gross
    .selectExpr("Title", "(Worldwide_Gross, US_Gross) as Profit")
    // we can get cols from the struct as well
    // struct col.getField(sub col name).as(new name)
    .selectExpr("Title", "Profit.US_Gross")
    .show()

  // ARRAYS:
  val moviesWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words"))
  // title now has a column with title words separated in an array

  // array operations
  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"), // get array value at index -> col expr = array[index] from the title words array column
    size(col("Title_Words")), // get array sizes
    array_contains(col("Title_Words"), "Love") // check if element in array
  ).show()




}

