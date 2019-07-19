/**
  * Classes and Objects
  * @param xc
  * @param yc
  */
class Point(xc: Int, yc: Int) {
  var x: Int = xc
  var y: Int = yc

  def move(dx:Int,dy:Int) {
    x = x + dx
    y = y + dy
    println(s"Point x location: $x")
    println(s"Point y location: $y")
  }
}
object Test {
  def main(args: Array[String]): Unit = {
    val pt = new Point(10, 20)
    pt.move(10,10)
    val arg = args(0)
    println(s"$arg")
  }
}
Test.main(Array("Hello"))

class Location(xc: Int,yc: Int, val zc: Int) extends Point(xc,yc) {
  var z: Int = zc

  def move(dx:Int,dy:Int,dz:Int) {
    x = x + dx
    y = y + dy
    z = z + dz
    println(s"Point x location: $x")
    println(s"Point y location: $y")
    println(s"Point z location: $z")
  }
}

object Demo {
  def main(args: Array[String]): Unit = {
    val loc = new Location(10, 20, 15)
    loc.move(10,20,15)
    val arg = args(0)
    println(s"$arg")
    loc.move(10,20)
    println(s"$arg")
    val pt = loc.asInstanceOf[Point]
    pt.move(30,40)
    println(s"$arg")
  }
}
Demo.main(Array("World"))

