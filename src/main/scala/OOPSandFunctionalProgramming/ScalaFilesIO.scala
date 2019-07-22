package OOPSandFunctionalProgramming

import java.io._
import scala.io.Source
import scala.io.StdIn

object DemoFiles {
	def main(args: Array[String]): Unit = {
		//Writing a file
		val writer = new PrintWriter(new File("/home/cloudera/files/test.txt"))
		writer.write("Hello Scala")
		writer.close()
	
		// Read Line from Console
		val user_input = readLine()
		println(s"Thanks, you just typed: $user_input")

		//Read File Content
		println("Following is the content read: ")
		Source.fromFile("/home/cloudera/files/test.txt").foreach {
			print(_)
		}		
	}
}
