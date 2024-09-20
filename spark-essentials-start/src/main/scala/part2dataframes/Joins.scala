package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  /*
  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")


  // JOINS
  // - combine data form multiple DFs based on some condition such as matching id
  // - if the condition passes rows are combines else discarded

  // new df = df1.join(df2, condition, jointype) // inner jointype - discard ones that dont meed condition
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(
    bandsDF,
    joinCondition,
    "inner")

  guitaristsBandsDF.show()

  // OUTER JOINS
  // left outer = everything in inner join + remaining rows in left table (null for missing data)
  guitaristsDF.join(bandsDF, joinCondition, "left_outer").show()

  // left outer = everything in inner join + remaining rows in right table (null for missing data)
  guitaristsDF.join(bandsDF, joinCondition, "right_outer").show()

  // left outer = everything in inner join + remaining rows in BOTH tables (null for missing data)
  guitaristsDF.join(bandsDF, joinCondition, "full_outer").show()


  // SEMI JOINS - everything in the left df for which there is a new on the right df satisfying condition
  // left semi = just data in the first df for which there is a row in the  bands df given condition
  // filtering left df based on the join
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()

  // ANTI JOINS - opposite of semi joins
  // only data in left df with no row in the right df
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show()

  // notes:
  // NEW JOINED DF HAS 2 COLUMNS BOTH CALLED ID WHICH WHEN REFERRED TO SPARK CRASHES
  // guitaristsBandDF.select("id", "band").show() // crashes due to id vague reference

  // OPTION 1 FIX - rename the column on which we are joining
  //guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band").show()

  // OPTION 2 -drop one of the duplicate id column
  //guitaristsDF.drop(bandsDF.col("id")) // spark knows which id col im referring to due to  band df reference

  // OPTION 3 - rename offending col and keep the data
  //val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  //guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("id"), "inner").show()


  //println("comlex types: ")
  // USING COMPLEX TYPES
  // such as using expressions for the conditions of arrays
  //guitaristsDF.join(guitaristsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId")).show()

   */


  // EXERCISES - docker spin up required
  // - show all employee and their max salary
  // - show all employees who were never managers
  // - show top 10 paid job titles

  // read db to df
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user )
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load() // no path since remote

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val titlesDF = readTable("titles")
  val deptManagersDF = readTable("dept_manager")

  employeesDF.show()
  salariesDF.show()
  titlesDF.show()
  deptManagersDF.show()


  // - show all employee and their max salary

  // tutorial code
  val maxsalperemp = salariesDF.groupBy("emp_no").max("salary")
  val empsaldf = employeesDF.join(maxsalperemp, "emp_no")

  empsaldf.show()


  // my code
  val salariesModDF = salariesDF.withColumnRenamed("emp_no", "emp_no_salary")
    .groupBy(col("emp_no_salary"))
    .agg(
      max("salary").as("Max_Salary")
    )
  salariesModDF.show()

  val employeeSalaryCondition = employeesDF.col("emp_no") === salariesModDF.col("emp_no_salary")
  val employeeMaxSalaryDF = employeesDF
    .join(salariesModDF, employeeSalaryCondition, "inner")
    .select("emp_no", "first_name", "last_name", "Max_Salary")

  employeeMaxSalaryDF.show()


  // - show all employees who were never managers
  val deptManagersModDF = deptManagersDF.withColumnRenamed("emp_no", "emp_no_manager")


  val employeesManagerCondition = employeesDF.col("emp_no") === deptManagersModDF.col("emp_no_manager")
  val employeesNeverManagerDF = employeesDF
    .join(deptManagersModDF, employeesManagerCondition, "anti")
    .select("emp_no", "first_name", "last_name")

  employeesNeverManagerDF.show()

  // alternative
  //println("riubgrueigberiugbwer")
  //employeesDF.join(deptManagersDF, employeesDF.col("emp_no") === deptManagersModDF.col("emp_no"), "left_anti")
  //  .show()


  // employees    = emp_no, birth_date, first_name, last_name, gender, hire_date
  // salaries     = emp_no, salary, from_date, to_date
  // titles       = emp_no, title, from_date, to_date // run max on date to find most recent
  // dept_manager = emp_no, dept_no, from_date, to_date

  // - show top 10 paid job titles
  val latestJobPerEmp = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  latestJobPerEmp.show()

  val bestPaidEmp = employeeMaxSalaryDF.orderBy(col("Max_Salary").desc).limit(10)

  bestPaidEmp.join(latestJobPerEmp, "emp_no").show()


}
