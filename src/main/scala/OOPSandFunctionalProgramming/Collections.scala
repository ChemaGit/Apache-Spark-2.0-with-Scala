package OOPSandFunctionalProgramming

object DemoCollections{

	def main(args: Array[String]): Unit = {
		/**Lists in Scala**/
		println("*********List in Scala***************")
		println(fruit.mkString("List(",",",")"))
		println(nums.mkString("List(",",",")"))
		println(fruit1.mkString("List(",",",")"))
		println(nums1.mkString("List(",",",")"))

		//head, tail, isEmpty of a List
		println(fruit.head)
		println(fruit.tail.mkString(","))
		val res = if(empty.isEmpty) "Empty List" else println(empty.mkString(","))
		println(res)


		println(fruit2.mkString(","))
		println(fruit3.mkString(","))

		println(fruit4.mkString(","))
		println(nums2.mkString(","))
		println(square.mkString(","))
		println(mult.mkString(","))
		println(fruit5.mkString(","))
		println("*********Sets in Scala***************")
		println(s)
		println(s1)
		println(sFruit.mkString(","))
		println("Head of sFruit: " + sFruit.head)
		println("Tail of sFruit: " + sFruit.tail)
		println("Check if sFruit is empty: " + sFruit.isEmpty)
		println("sFruit3: " + sFruit3)
		println("sFruit4: " + sFruit4)
		println("Min: " + s1.min)
		println("Max: " + s1.max)
		println("intersect: " + sFruit.&(sFruit3))
		println("intersect: " + sFruit.intersect(sFruit3))
		println("*******Maps in Scala***********")
		println("Keys in colors: " + colors.keys)
		println("Values in colors: " + colors.values)
		println("Check if colors is empty: " + colors.isEmpty)
		println("Check if m is empty: " + m.isEmpty)
		println("Keys in colors2: " + colors2.keys)
		println("Keys in colors3: " + colors3.keys)
		println("Values in colors2: " + colors3.values)
		println("Values in colors3: " + colors3.values)
		colors3.foreach(i =>{println("Key: %s -> Value: %s".format(i._1, i._2))})
		if(colors.contains("red")) println("red key exists with value: " + colors("red"))
		else println("red key does not exists")
		if(colors.contains("maroon")) println("red key exists with value: " + colors("maroon"))
		else println("maroon key does not exists")
	}

	/**Lists in Scala**/
	val fruit: List[String] = List("apples","oranges", "pears")
	val fruit1 = "mango" :: ("banana"::("tangerine"::Nil))
	val nums: List[Int] = List(1,2,3,4)
	val nums1 = 1::(2::(3::(4::Nil)))
	val empty: List[Nothing] = List()
	val empty1 = Nil
	//Concatenate Lists
	val fruit2 = fruit ::: fruit1
	val fruit3 = List.concat(fruit,fruit2)
	val fruit4 = List.fill(3)("apples")
	val nums2 = List.fill(10)(2)
	val square = List.tabulate(6)(n => n * n)
	val mult = List.tabulate(4,5)( (x,y) => x * y)
	val fruit5 = fruit3.reverse

	/**Sets in Scala**/
	val s: Set[Int] = Set()
	val s1 = Set(1,3,5,7)
	val sFruit = Set("apples","oranges","pears")
	println("Is empty set: " + sFruit.isEmpty)
	println("Head: " + sFruit.head)
	println("Tail: " + sFruit.tail)
	val sFruit2 = Set("apples","oranges","Mango","Tangerine","Banana")
	val sFruit3 = sFruit.++(sFruit2)
	val sFruit4 = sFruit ++ sFruit2

	/**Maps in Scala**/
	val m: Map[Char, Int] = Map()
	val colors = Map("red" -> "#FF0000", "azure" -> "#F0FFFF", "peru" -> "#CD853F")
	val colors1 = Map("blue" -> "#0033FF", "yellow" -> "#FFFF00", "red" -> "#FF0000")
	val colors2 = colors ++ colors1
	val colors3 = colors.++(colors1) 


}
