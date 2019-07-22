package OOPSandFunctionalProgramming

import java.io.FileReader
import java.io.FileNotFoundException
import java.io.IOException

object DemoExceptionHandling {
	def main(args: Array[String]): Unit = {
		try {
			val f = new FileReader("input.txt")
		} catch {
			case ex: FileNotFoundException => println("Missing file exception")
			case ex: IOException => println("IOException")
		} finally {
			println("See you later pal")
		}		
	}
}
