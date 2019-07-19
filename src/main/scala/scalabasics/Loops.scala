package scalabasics

/**Loops in Scala**/
object Test {
	def main(args: Array[String]):Unit = {
		var a = 10
		//while loop execution
		while(a < 20) {
			println(s"Value of a: $a")
			a = a + 1
		}
		
		// do loop execution
		var b = 10
		do {
			println(s"Value of b: $b")
			b += 1
		}while(b < 20)

		// for loop execution
		val numList = List(1,2,3,4,5,6,7,8,9)
		for(c <- numList) {
			println(s"Value of c: $c")
		}

		for(d <- 0 until 10) {
			println(s"Value of d: $d")
		}
		for(e <- 0 to 10) {
			println(s"Value of e: $e")
		}
		println("***********************")
		for(f <- 0 to 3; g <- 0 to 3) {
			println(s"Value of f: $f")
			println(s"Value of g: $g")
		}
		println("*************************")
		for(h <- numList if(h %2 == 0)) {
			println(s"Value of h: $h")
		}
		println("***********************")
		val retVal = for(a <- numList if(a % 2 != 0))yield a
		for(i <- retVal){
			println(s"Value of i: $i")
		}
	}
}
