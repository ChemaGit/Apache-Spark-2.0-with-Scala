package OOPSandFunctionalProgramming

object DemoString {
	def main(args: Array[String]): Unit = {
		println(greeting)
		println(s"len palindrome = $len")
		println(s"str1 + str2 = $str1$str2")

		printf("The value of the float variable es %f, while the value of the integer is %d, and the string is %s \n",floatVal,intVal,stringVal)
		println("The value of the float variable es %f, while the value of the integer is %d, and the string is %s".format(floatVal,intVal,stringVal))
	}
	val greeting = "Hello World"
	val palindrome = "dot saw I vas Tod"
	val len = palindrome.length

	val str1 = "Dot saw I was "
	val str2 = "Tod"

	val floatVal = 12.456
	val intVal = 2000
	val stringVal = "Hello, Scala"

}
