/**
  * Access Modifiers in Scala
  */

class Outer {
  class Inner{
    private def f(){println("hello")}
    class InnerMost {
      f()
    }
  }
  //(new Inner).f()
}

object Hello {
  def main(args: Array[String]): Unit = {
  }
}

class Super {
  protected def f(){println("hello")}
  class Sub extends Super{
    f()
  }
}
class Other {
  //(new Super).f()
}
object Hello2{
  def main(args: Array[String]): Unit = {
  }
}


class Outer2 {
  class Inner {
    def f() {println("hello")}
    class InnerMost{
      f()
    }
  }
  (new Inner).f()
}

object Hello3{
  def main(args: Array[String]): Unit = {
    val a = new Outer2()
    val b = new a.Inner()
    val c = new b.InnerMost()
  }
}
Hello3.main(Array("Hello"))