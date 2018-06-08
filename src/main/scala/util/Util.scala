package util

object Util {

  // 201805 : block 실행 시간 (millis sec)
  def watchTime[T](name : String, min : Int = 9000)(block : => T) : T = {
    val start = System.nanoTime()
    val ret = block
    val end = System.nanoTime()

    import scala.concurrent.duration._
    import scala.language.postfixOps

    val elapsed = (end - start ) nanos

    if (elapsed.toMillis > min) {
      println(s"code $name takes ${elapsed.toMillis} millis seconds.")
    }

    ret
  }

}
