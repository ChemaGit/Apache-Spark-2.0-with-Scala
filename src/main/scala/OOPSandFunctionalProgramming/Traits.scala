package OOPSandFunctionalProgramming

object DemoTraits {
	def main(args: Array[String]): Unit = {
		val emp = new Employee
		emp.sendEmail("Hi", "Hello World Scala")
	}

	trait Person {
		def sendEmail(subject: String, body: String)
	}

	class Employee extends Person {
		def sendEmail(subject: String, body: String): Unit = {
			println(s"$subject $body")
		}
	}
}
