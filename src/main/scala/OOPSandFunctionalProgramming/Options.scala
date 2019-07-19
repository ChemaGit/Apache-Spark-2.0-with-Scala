package OOPSandFunctionalProgramming

object DemoOptions {
	def main(args: Array[String]): Unit = {
		println("""capitals.get("France"): """ + capitals.get("France"))
	}

	val capitals = Map("France" -> "Paris", "Japan" -> "Tokyo")
}
