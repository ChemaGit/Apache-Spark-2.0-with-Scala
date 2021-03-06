package spark_sql

import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}
import org.apache.spark.sql.types.{StringType, StructField}

object SparkSQL {

  lazy val spark = SparkSession
    .builder()
    .appName("Spark SQL")
    .master("local[*]")
    .getOrCreate()

  lazy val sc = spark.sparkContext

  import spark.implicits._

  //Let's assume we have a DataFrame representing a data set of employees:
  case class Employee(id: Int, fname: String, lname: String, age: Int, city: String)
  case class Person(id: Int, name: String, country: String, city: String)

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    /**
      * Spark SQL
      * 1. Support relational processing both within Spark programs and
      *    on external data sources with a friendly API.
      * 2. High performance
      * 3. Easily support new data sources such as semi-structured data and
      *    external databases.
      *
      * Three main APIs
      * - SQL literal syntax
      * - DataFrames
      * - Datasets
      */

    /**
      * DataFrame is Spark SQL's core abstraction:
      * Conceptually equivalent to a table in relational database.
      * Are RDDs full of records with a known schema. So require
      * some kind of schema info!
      * DataFrames are untyped!!!
      *
      * DataFrames can be created in two ways:
      * 1. From an existing RDD.
      * 2. Reading a specific data source from file.
      */

    //Create DataFrame from RDD, schema reflectively inferred
    val tupleRDD = sc
      .textFile("hdfs://quickstart.cloudera/user/cloudera/files/data.csv")
      .map(line => line.split(","))
      .map(f => (f(0).toInt, f(1), f(2), f(3)))
    val tupleDF = tupleRDD.toDF("id","name","country","city")

    val tuplePerson = sc
      .textFile("hdfs://quickstart.cloudera/user/cloudera/files/persons.csv")
      .map(line => line.split(","))
      .map(f => Person(f(0).toInt, f(1), f(2), f(3)))

    val personDF = tuplePerson.toDF("id", "name", "country", "city") //infer the attributes from the case class's fields.

    //Create DataFrame from existing RDD, schema explicity specified
    //1. Create an RDD of Rows from the original RDD.
    //2. Create the schema represented by a StructType matching the structure of Rows in the RDD create in step 1.
    //3. Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession

    //The schema is encoded in a string
    val schemaString = "id name country city"
    //Generate the schema based on the string of schema
    val fields = schemaString
      .split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = types.StructType(fields)
    //Convert records of the RDD (people) to Rows
    val rowRDD = sc
      .textFile("hdfs://quickstart.cloudera/user/cloudera/files/data.csv")
      .map(line => line.split(","))
      .map(attributes => Row(attributes(0),attributes(1),attributes(2), attributes(3)))
    //Apply the schema to the RDD
    val perDF = spark.createDataFrame(rowRDD, schema)

    //Crate DataFrame by reading in a data source from file.
    //Using the SparkSession object, you can read in semi-structured/structured data by using the read method,
    //for example, to read in data and infer a schema from a JSON file
    val df = spark
      .read
      .json("hdfs://quickstart.cloudera/user/cloudera/files/employees.json")
    df.show()
    /**
      * Semi-structured/structured data sources Spark SQL can directly create DataFrames from: JSON, CSV, Parquet, JDBC
      * API docs for DataFrameReader: http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader
      */


    /**
      * SQL Literals
      * Now we have a DataFrame to operate on and write SQL sintax.
      * The SQL statements available are largely what's available in
      * HiveQL. This includes standard SQL statements such as: SELECT, FROM WHERE,
      * COUNT, HAVING, GROUP BY, ORDER BY, SORT BY, DISTINCT, JOIN, LEFT|RIGHT|FULL OUTER JOIN
      * Subqueries: SELECT col FROM(SELECT a + b AS col FROM t1) t2
      */
    //Register the DataFrame as a SQL temporary view
    tupleDF.createOrReplaceTempView("people")
    //SQL literals can be passed to Spark SQL's sql method
    val adultsDF = spark.sql("SELECT * FROM people WHERE age > 17")
    adultsDF.show()

    //DataFrame with schema defined in Emplyee case class
    val employeeDF = sc
      .textFile("hdfs://quickstart.cloudera/user/cloudera/files/employees.csv")
      .map(line => line.split(","))
      .map(f => Employee(f(0).toInt, f(1), f(2), f(3).toInt,f(4)))
      .toDF

    //Register the DataFrame as a SQL temporary view
    employeeDF.createOrReplaceTempView("employee")
    //Obtain just the IDs and last names of employees working in Sydney, sort the result
    //in order of increasing employee ID
    val query = spark.sql(""""SELECT id,lname from employee WHERE city = "Sydney" ORDER BY id"""")
    query.show()

    sc.stop()
    spark.stop()
  }

}
