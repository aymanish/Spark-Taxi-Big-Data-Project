package part3typesdatasets

import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import java.sql.Date

object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/numbers.csv")

  // convert a df to a dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  // df: type_dataset = df.as[Int]
  // above works with single column dfs

  // dataset of a complex type
  // define car object like schema
  //DEFINE TYPE
  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Double,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Year: Date,
                Origin: String
                )

  def readDF(fileName: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$fileName")

  val carsDF = readDF("cars.json")

  // we need an encoder for a car
  import spark.implicits._
  //implicit val carEncoder = Encoders.product[Car]
  // by importing all implicits we can avoid above and simply write:

  val carsDS = carsDF.as[Car]

  // dataset of a complex type
  // 1 - DEFINE TYPE AS CASE CLASS
  // 2 - READ DF FROM FILE
  // 3 - DEFINE ENCODER OR IMPORT IMPLICITS
  // 4 - CONVERT DF TO DS

  // DF COLLECTIONS functions
  numbersDS.filter(_ < 100).show()

  // map, flatmap, fold, reduce, for comprehensions ...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  // DATASETS:
  /*
  USEFUL when:
  - we want to maintain type info
  - we want clean concise code
  - our filters / transformations are hard to express in DF or SQL

  AVOID when
  - performance is critical: Spark can't cptimise as we no longer have transofrmation and action separation
   */

  // EXERCISES
  // 1) count how many cars
  println(carsDS.count)
  // 2) count how many cars hp > 140
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)

  // 3) avg hp for the entire dataset
  println()
  carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsDS.count

  // we can also us df functions with ds



  // JOINS:
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayerDS = readDF("guitarPlayer.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  // for joins we use joinWith as it preserves the dataset typing
  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayerDS.joinWith(
    bandsDS,
    guitarPlayerDS.col("band") === bandsDS.col("id"),
    "inner"
  )

  guitarPlayerBandsDS.show()


  // Exercise
  // join the guitarDS and guitarPlayerDS in an outer join
  // use array_contains

  guitarPlayerDS.joinWith(guitarsDS, array_contains(guitarPlayerDS.col("guitars"), guitarsDS.col("id")), "outer")
    .show()

  // Grouping DS
  carsDS.groupByKey(_.Origin)
    .count().show()

}
