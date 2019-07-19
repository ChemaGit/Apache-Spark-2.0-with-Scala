/**
  * If else Statements in Scala
  */

object Demo {
  def main(args: Array[String]): Unit = {
    var x = 10
    if(x < 20) {
      println(s"this is If statement")
    }
  }
}
Demo.main(Array("Hello"))

object Demo1 {
  def main(args: Array[String]): Unit = {
    var x = 30
    if(x < 20) {
      println(s"this is If statement")
    } else {
      println(s"this is Else statement")
    }
  }
}
Demo1.main(Array("Hello"))

object Demo2 {
  def main(args: Array[String]): Unit = {
    var x = 30
    if(x == 10) {
      println(s"value of x is $x")
    } else if(x == 20) {
      println(s"value of x is $x")
    } else if(x == 30) {
      println(s"value of x is $x")
    } else {
      println(s"this is Else statement")
    }
  }
}
Demo2.main(Array("Hello"))

object Demo3 {
  def main(args: Array[String]): Unit = {
    var x = 30
    val y = 10
    if(x == 30) {
      if(y == 10) {
        println(s"Value of X is $x and Value of Y is $y")
      }
    }
  }
}
Demo3.main(Array("Hello"))