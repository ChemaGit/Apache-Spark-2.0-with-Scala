package OOPSandFunctionalProgramming

object DemoPatternMatching {
	def main(args: Array[String]): Unit = {
		println("matchTest(2): " + matchTest(2))
		println("matchTest(3): " + matchTest(3))
		println("""matchTestAny("one"): """ + matchTestAny("one"))
		println("matchTestAny(3): " + matchTestAny(3))
		println("""matchTestAny("two"): """ + matchTestAny("two"))
		println("""matchTestAny("three"): """ + matchTestAny("three"))

		val alice = new Person("Alice", 25)
		val bob = new Person("Bob", 32)
		val charlie = new Person("Charlie", 35)
		val person: List[Person] = List(alice,bob,charlie)

		person.foreach{ p => p match{
			case Person("Alice",25) => println("Hi Alice")
			case Person("Bob",32) => println("Hi Bob")
			case Person(name,age) => println(s"Age: $age  name: $name")
		}
		}
	}

	def matchTest(x: Int): String = x match {
		case 1 => "one"
		case 2 => "two"
		case _ => "many"
	}
	
	def matchTestAny(x: Any): Any = x match {
		case 1 => "one"
		case "two" => 2
		case y: Int => "scala.Int"
		case _ => "many"
	}
	
	case class Person(name: String, age: Int)

}
