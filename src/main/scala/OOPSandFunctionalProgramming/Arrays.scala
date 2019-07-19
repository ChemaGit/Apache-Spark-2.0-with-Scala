package OOPSandFunctionalProgramming

import Array._

object ArrayDemo {
	def main(args: Array[String]): Unit = {

		println(z.mkString(","))
		println("***********************")
		range.foreach( x => range2.foreach(y => myArray(x)(y) = x + y))
		myArray.foreach(x => x.foreach(y => println(y)))
		println("***************************")
		myList3.foreach(x => println(x))

	}

	val z: Array[String] = new Array[String](3)
	val x = new Array[String](3)
	z(0) = "Maria"
	z(1) = "Lucia"
	z(2) = "Marcia"

	// Two dimensions Array
	val myArray = ofDim[Int](3,3)
	val range = 0 until 3
	val range2 = 0 until 3

	val myList = Array(1, 3, 5, 4,6,7)
	val myList2 = Array(9, 4, 43,65,21)
	val myList3 = concat(myList, myList2)

}
