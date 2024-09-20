package part2dataframes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, countDistinct, min, stddev, sum, mean}


object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // DATA SCIENCE & COUNTING:
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))).show()
  moviesDF.selectExpr("count(Major_Genre)").show()

  //counting all
  moviesDF.select(count("*")).show()

  // count distinct
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // approximate count
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg(col("US_Gross")))
  moviesDF.selectExpr("avg(US_Gross)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // GROUPING
  // not just count the distinct genres in the df
  // but rather how many movies for each of those genres
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .count()
    .show()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")
    .show()

  // agg contruct
  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("Num_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    ).orderBy("Avg_Rating")

  aggregationsByGenreDF.show()


  // EXERCISES
  // 1) Sum up all the profits of all the movies in the DF
  moviesDF.select(sum(col("US_Gross") + col("Worldwide_Gross"))).show()

  moviesDF.select((col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross"))
    .select(sum(col("Total_Gross")))
    .show()

  // 2) count how many distinct directors we have
  moviesDF.select(countDistinct(col("Director"))).show()

  // 3) show the mean and stddev of us gross for movies
  moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  ).show()

  // 4) get avg IMDB_Rating and the avg US_Gross rev per director
  moviesDF.groupBy(col("Director"))
    .agg(
      avg(col("IMDB_Rating")).as("Average_Rating"),
      avg(col("US_Gross")).as("Average_US_Gross")
    ).orderBy(col("Average_Rating").desc_nulls_last)
    .show()

  // ALL ABOVE ARE WIDE TRANSFORMATIONS - expensive read computation







}

