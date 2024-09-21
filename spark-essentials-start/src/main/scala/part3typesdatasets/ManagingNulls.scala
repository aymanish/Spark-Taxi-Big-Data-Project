package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

object ManagingNulls extends App {

  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // NULLS:
  // selecting the first non null across 2 cols
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    // coalese() returns first null of the 2 cols, if both a null furst col arg takes priority
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  )
  // checking for nulls
  // - filter nulls with isNull
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // nulls when ordering - at the top or bottom of order
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)
  // desc_nulls_first, desc

  // removing nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop()
  // removing rows containing columns

  // replacing nulls
  // replace nulls in listed cols with 0
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
  // but better alt
  moviesDF.na.fill(Map(
    "Rotten_Tomatoes_Rating" -> 10, // map cols with null replacements
    "IMDB_Rating" -> 0,
    "Director" -> "Unknown"
  ))

  // complex operations with selectEXPR:
  // ifnull() // takes col names -> same as coalese
  // nvl( col names ) -> same
  // nullif( col names ) -> returns null if the two values are EQUAL, else first value returned
  // nvl2 (more than 2 col names?) -> if (first != null) second else third
}
