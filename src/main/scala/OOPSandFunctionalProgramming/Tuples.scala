package OOPSandFunctionalProgramming

object DemoTuples {
	def main(args: Array[String]){
		val (num,str,str2) = t
		println(s"num = $num, str: $str, str2 = $str2")
		t1.productIterator.foreach(v => println(s"v: $v"))
		val sum = t2._1 + t2._2 + t2._3 + t2._4
		println(s"sum = $sum")
		println(t3.swap)

	}
	val t = (1,"Hello","Console")
	val t1 = (2, "World", "Scala")
	val t2 = new Tuple4[Int, Int, Int, Int](1,2,3,4)
	val t3 = new Tuple2("World","Hello")
}
