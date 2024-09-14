package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFrameBasics extends App {

  //CREATING A SPARK SESSION:
  // we need a spark session to run an application on top of spark
  // we create a spark session object and
  // call the builder constructor to construct a session as spark
  // we define attributes with.attributes()
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()
  // we have created a spark session - entry point for reading and writing dataframe

  // READING A DATAFRAME:
  // weve instructed the spark dataframe reader firstDF to
  // - read a json file with infer schema true
  // - meaning all the columns for the df are figured out from the json structure
  // - and the path of the file is here
  // this resutls int he ddataframe -> firstDF
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // HANDLING DATAFRAMES:
  // What is a DataFrame:
  // It is the attributes of the data and a distributed collection of rows
  // that are conforming to that schema / attribute structure

  firstDF.show() // we can print out the dataframe
  firstDF.printSchema() // get info on columns such as types

  //firstDF.take(10) // returns the first n rows of dataset
  firstDF.take(10).foreach(println) // print first n rows

  //SPARK TYPES:

  // Spark types describing the data as in printSchema
  // are explained by case objects spark uses internally
  // not known at compile time but at run time
  // as a case object longtype is a singleton that spark uses internally to describe the type of data inside
  val longType = LongType

  // complicated types: data with multiple columns
  // Structtypes contain arrays of struct field defining complex data / multi columns
  // Structfield arguments include data followed by spark datatype
  // we can use this to create our own schemas as best practice instead of inferschema

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

  /*
  As a best practice, you should define your own schema using StructType when:
  - You need precision in data types.
  - You want to avoid errors in automatic type inference.
    - Accidentally parsing dates incorrectly due to inconsistent data formats compared to standards
  - You have large datasets where inferring the schema can be costly.

  Use inferSchema when:
  - You're dealing with small datasets or prototyping.
  - Speed of development is a priority over precision.

  So create your own schema and force your dataframe to conform to that
  - Any exceptions when parsing -> we then make our data frame conform to that
   */

  // OBTAIN EXISTING SCHEMA
  val carsDFSchema = firstDF.schema // schema is struct type

  // READING DATAFRAMES WITH CUSTOM SCHEMA:
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema) // we pass a schema struct type
    .load("src/main/resources/data/cars.json")

  // CREATE ROWS MANUALLY:
  // - rarely used but useful when testing stuff
  // we use the row.apply() factory method
  val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA") // can take anything it wants

  // CREATE DF FROM TUPLES OF ROWS
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )
  val manualCarsDF = spark.createDataFrame(cars) // schema auto created from sequence

  // note: dataframes have schemas, rows do not

  // CREATE DFs WITH IMPLICITS
  import spark.implicits._ // import implicits form spark session object
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG","Cylinders","Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")
  // in tuples manualCarsDF the schema was infered from tuples but no column names were provided
  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // EXERCISES:
  // 1) Create a manual DF describing smartphones:
  // - make
  // - model
  // - screen dimension
  // - camera megapixel
  // Print it out using show and print schema


  // create a sequence of tuples
  // - each tuple is a row containing details
  // use spark.createDataFrames() to create schema + df
  val smartPhones = Seq(
    ("Samsung", "Flip 2", 10, 20, 500),
    ("Xiaomi", "Redmi 5G", 12, 18, 400),
    ("Iphone", "16", 10, 15, 700)
  )

  val manualSmartPhoneDF = spark.createDataFrame(smartPhones)
  val manualSmartPhoneDFImp = smartPhones.toDF("Make", "Model", "Dim_X", "Dim_Y", "Megapixels")

  manualSmartPhoneDF.show()
  manualSmartPhoneDF.printSchema()

  // better labels BUT auto schema not ideal
  println("Show smartphone data frame:")
  manualSmartPhoneDFImp.show()
  println("Print smartphone DF schema:")
  manualSmartPhoneDFImp.printSchema()

  // 2) Read another file eg: movies.json
  // - print its schema
  //  - read json file and convert to df and use print schema
  // - count the number of rows using count

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  println("Show movies data frame:")
  moviesDF.show()
  println("Print movies df schema")
  moviesDF.printSchema()
  println("Movies rows")
  println(moviesDF.count()) //3201


  // read/write df in various formats
  // use files and databased as data sources













}
