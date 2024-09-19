package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // COLUMNS:
  // - special objects that allow you to obtain new dataframes from some source data
  val firstColumn = carsDF.col("Name") // working as a filter object?

  // selecting
  val carNamesDF = carsDF.select(firstColumn)
  // - we are projecting the carsDF into a new df which contains less data
  // - since there are fewer columns

  carNamesDF.show()

  // VARIOUS SELECT METHODS: for selecting columns in a df
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    carsDF.col("Acceleration"),
    // but this is tedious so we use just col
    col("Acceleration"),
    // and another more explicit method
    column("Weight_in_lbs"),
    // use scala symbols
    'Year, // scala symbol - autoconverted to column
    // or dollar
    $"Horsepower", // fancier interpolated string
    // lastly we have expr
    expr("Origin") // expression returns the origin column
  )

  // You can either pass column objects as expressions using col
  // Or column names as strings below:

  // another popular way is to just pass in a binch of strings
  carsDF.select("Name", "Year")

  // EXPRESSIONS:
  val simpleExpression = carsDF.col("Weight_in_lbs")
  // simple expression can be chained further
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  // this can now be used as a column expression
  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression,
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2") // new derived column which we rename
  )

  carsWithWeightsDF.show()

  // SELECT EXPR():
  // but to deal with boilerplate code we have the select expr() method
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF PROCESSING:

  // Adding new column
  // to a df - new df
  // new df = df.withColumn("new column name", expression "df column" + some expression
  val carsWithKg3DF =carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // Renaming a column
  val carsWithColRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names -> above we renames without _ for better parsing
  // so when we use that col name use back tick (`) as shown below
  carsWithColRenamed.selectExpr(
    "Weight in pounds`"
  )

  // Removing a column:
  carsWithColRenamed.drop("Cylinders", "Displacement")

  // Filtering: use filter() or where()
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA") // filter by column origin not equal to USA
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")

  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = USA")

  //chaining filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  // OR
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  // OR
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCarsDF) // DFs need to have the same schema

  // disting values: shows all unique countries
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

}
