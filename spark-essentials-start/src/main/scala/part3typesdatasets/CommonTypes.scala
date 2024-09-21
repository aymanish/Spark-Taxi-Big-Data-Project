package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")



  // ADDING A PLAIN VALUE TO A DF
  // lit() works with all types
  moviesDF.select(col("Title"), lit(47).as("plain_value")).show

  // BOOLEANS
  val dramaFilter = col("Major_Genre") === "Drama" // also use equalTo
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter // booleans can act as columns as well

  // use filter normally
  moviesDF.select("Title").where(dramaFilter)

  // we can also use the filters as booleans
  // good_movie appears as column of true / false
  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  moviesWithGoodnessFlagsDF.where("good_movie").show()
  // we can filter using the column name if it contains boolean values
  // where("good_move") === "true"

  // also works with negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movies"))).show()

  // NUMBERS
  // we can use numeric operators on numeric columns
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // correlation
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) // corr is an action


  // STRINGS
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalisation of first letter of words: initcap(column)
  // - lower, upper methods as well
  carsDF.select(initcap(col("Name"))).show()

  // contains - simple
  carsDF.select("*").where(
    col("Name").contains("volkswagen")
    or
    col("Name").contains("vw")
  )

  // contained - better
  // REGEX
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract"))
    .where(col("regex_extract") =!= "").drop("regex_extract")

  // define a regString condition
  // df.select( column,
  // regexp_extract(col, regString, 0).as(name col) -> extract regex from col using regString and create new col
  // where(new regex col not equal to empty)

  // REPLACING String
  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace"))
    .show()

  // EXERCISE:
  // Filter the carDF by a list of car names obtained by an API call
  // - contains
  // - regex

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|")

  // regex
  val filterCarDF1 = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract"))
    .where(col("regex_extract") =!= "").drop("regex_extract")
  filterCarDF1.show()

  // contains
  val filters = getCarNames
    .map(_.toLowerCase()) // [list of lower case names]
    .map(name => col("Name").contains(name))
  // for every car name
  // i create a new filter
  // such that the col name in the df contains the car name
  // i have a list of columns of filters

  val bigFilter = filters
    .fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)

  carsDF.filter(bigFilter).show()






}
