package spark_mlib.basic_statistics_and_data_types

import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{Matrices, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object LocalAndDistributedMatrices {

  val spark = SparkSession
    .builder()
    .appName("LocalAndDistributedMatrices")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","LocalAndDistributedMatrices") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    try {
      /*
       A Simple Dense Matrix
       # Sparse Matrices in Spark
       # Compressed Sparse Column(CSC) format

       0	1	  2	  3
       0
       1	34.0
       2
       3		  55.0
       4

       - Rows: 5
       - Columns: 4
       - Column pointers: (0,0,1,2,2)
       - Row indices:(1,3)
       - Non-zero values: (34.0,55.0)
        */
      val simpleDenseMatrix = Matrices.dense(3, 2, Array(1,3,5,2,4,6))

      // Sparse Matrix Example
      val m = Matrices.sparse(5,4,
        Array(0,0,1,2,2),
        Array(1,3),
        Array(34,55))
      /*
      # RowMatrix
      - The most basic type of distributed matrix
        - It has no meaningful row indices, being only a collection fo feature vectors
      - Backed by an RDD of its rows, where each row is a local vector
      */
      val rows: RDD[Vector] = sc.parallelize(Array(
        Vectors.dense(1.0,2.0),
        Vectors.dense(4.0,5.0),
        Vectors.dense(7.0,8.0)))

      val mat: RowMatrix = new RowMatrix(rows)
      val m1 = mat.numRows()
      val n = mat.numCols()

      /*
      # IndexedRowMatrix
      - Similar to a RowMatrix
      - But it has meaningful row indices, which can be used for identifying rows and executing joins
      - Backed by an RDD of indexed rows, where each row is a tuple containing an index(long-typed) and a local vector
      - Easily created from an instance of RDD[IndexedRow]
      - Can be converted to a RowMatrix by calling toRowMatrix()
       */
      val rowsI: RDD[IndexedRow] = sc.parallelize(Array(
        IndexedRow(0,Vectors.dense(1.0,2.0)),
        IndexedRow(0,Vectors.dense(4.0,5.0)),
        IndexedRow(0,Vectors.dense(7.0,8.0))))
      val idxMat: IndexedRowMatrix = new IndexedRowMatrix(rowsI)

      /*
      # CoordinateMatrix
      - Should be used only when both dimensions are huge and the matrix is very sparse
      - Backed by an RDD of matrix entries, where each entry is a tuple(i: Long, j: Long, value: Double)
        where:
        - i is the row index
        - j is the column index
        - value is the entry value
      - Can be easily created from an instance of RDD[MatrixEntry]
      - Can be converted to an IndexedRowMatrix with sparse rows by calling toIndexedRowMatrix()
       */
      val entries: RDD[MatrixEntry] = sc.parallelize(Array(
        MatrixEntry(0,0,9.0),
        MatrixEntry(1,1,8.0),
        MatrixEntry(2,1,6.0)))
      val coordMat: CoordinateMatrix = new CoordinateMatrix(entries)

      // To have the opportunity to view the web console of Spark: http://localhost:4041/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped")
      spark.stop()
      println("SparkSession stopped")
    }
  }
}
