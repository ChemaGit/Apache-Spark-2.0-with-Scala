package OOPSandFunctionalProgramming

/**Functions and Procedures**/
object Demo {
	def main(args: Array[String]): Unit = {
		val result = addInt(5, 10)
		sayHiTo("Scala")
		println(s"And addInt(5,10) is: $result")
		println("****************************")
		delayed(time())
		println("*****************************")
		printInt(b = 5, a = 7)
		println("****************************")
		printStrings("Hello ","World.","How ", "are", "you?")		
		println("****************************")
		for(i <- 1 to 10) println(s"Factorial $i = " + factorial(i))
		println("****************************")
		println("addInt_b() = " + addInt_b())
		println("addInt_b(10,12) = " + addInt_b(10,12))

		println("****************************")
		println("Factorial 0: " + factorial_b(0))
		println("Factorial 1: " + factorial_b(1))
		println("Factorial 2: " + factorial_b(2))
		println("Factorial 3: " + factorial_b(3))

		println("***********************************")
		val x = inc(8)
		println(s"Inc x = $x")
		val z = mul(7,5)
		println(s"Mul z = $z")
		println(s"userDir = $userDir")
		

	}
	/**Function: Return some value**/
	def addInt(a: Int,b: Int):Int = {
		a + b
	}

	/**Procedure: doesn't return anything**/
	def sayHiTo(name: String): Unit = {
		println(s"Hi $name")
	}

	/**Call by Name Parameter**/
	def time():Long = {
		println("Getting time in nano seconds")
		System.nanoTime
	}
	def delayed(t: => Long)= {
		println("In delayed method")
		println(s"Param: $t")
	}

	/**Functions with Named Arguments**/
	def printInt(a: Int,b: Int) = {
		println(s"Value of a: $a")
		println(s"Value of b: $b")
	}

	/**Functions with variable arguments**/
	def printStrings(args: String*) = {
		var i: Int = 0
		for(arg <- args){
			println("Arg value[" + i + "] =" + arg)
			i += 1
		}
	}

	/**Recursion Functions**/	
	def factorial(n: BigInt): BigInt = {
		if(n <= 1) 1 else factorial(n - 1) * n
	}

	/**Default parameters for a function**/
	def addInt_b(a: Int = 5,b: Int = 7): Int = {
		a + b
	}

	/**Nested Functions**/
	def factorial_b(n: Int): Int = {
		def fact(l: Int, accumulator: Int): Int = {
			if(l <= 1) accumulator
			else fact(l - 1, l * accumulator)
		}
		fact(n,1)
	}

	/**Anonymous Functions**/
	val inc = (x: Int) => x + 1
	val mul = (x: Int,y: Int) => x * y
	val userDir = () => System.getProperty("user.dir")
}
