package spark_mlib

import org.apache.spark.mllib.linalg.{Vector,Vectors}

object DataTypesOnMlib {
	def main(args: Array[String]): Unit = {
		val dv: Vector = Vectors.dense(1.0,0.0,3.0)

		val sv1: Vector = Vectors.sparse(3, Array(0,2),Array(1.0,3.0))
		val sv2: Vector = Vectors.sparse(3, Seq( (0, 1.0), (2,3.0)))
	}
}
