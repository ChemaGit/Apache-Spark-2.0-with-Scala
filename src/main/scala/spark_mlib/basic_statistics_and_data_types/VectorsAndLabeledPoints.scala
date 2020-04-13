package spark_mlib.basic_statistics_and_data_types

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object VectorsAndLabeledPoints {

  // Dense Vector example
  val vectorDense = Vectors.dense(44.0,0.0,55.0)

  // Sparse Vector example
  val sparseVector = Vectors.sparse(3, Array(0,2), Array(44.0,55.0))
  val sparseVector1 = Vectors.sparse(3, Seq((0,44.0),(2,55.0)))

  // Labeled Points Example
  val labeledPoint = LabeledPoint(1.0, Vectors.dense(44.0,0.0,55.0))
  val labeledPoint1 = LabeledPoint(0.0, Vectors.sparse(3, Array(0,2), Array(44.0,55.0)))
}
