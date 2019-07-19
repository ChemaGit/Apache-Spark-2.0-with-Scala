package OOPSandFunctionalProgramming

object DemoOptions {
	def main(args: Array[String]): Unit = {
		println("""capitals.get("France"): """ + capitals.get("France"))
		println("""capitals.get("India"): """ + capitals.get("India"))
		println("""show(capitals.get("Japan")): """ + show(capitals.get("Japan")))
		println("""show(capitals.get("India")): """ + show(capitals.get("India")))

		println("a.getOrElse(0): " + a.getOrElse(0))
		println("b.getOrElse(10): " + b.getOrElse(10))

		println("a.isEmpty: " + a.isEmpty)
		println("b.isEmpty: " + b.isEmpty)
	}

	val capitals = Map("France" -> "Paris", "Japan" -> "Tokyo")
	val a: Option[Int] = Some(5)
	val b: Option[Int] = None

	def show(opt: Option[String]): String = {
		opt match {
			case Some(s) => s
			case None => "What the hell?"
		}
	}
}
