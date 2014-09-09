import org.apache.log4j.{Level, Logger}
import org.lda.LatentDirichletAllocation
import scala.io.Source
import java.io._

object Main {
  def main(args: Array[String]) {
    Logger.getLogger("spark").setLevel(Level.WARN)
    val sizes = List(32)//, 64, 128, 256, 512)
    val tasks = List(2)//1, 4, 8, 12, 16, 20, 24, 28, 32)
    var szs = List(0)
    var tsks = List(0)
    val zero : Long = 0
    var time = List(zero)
    var t = 0
    var s = 0
    var start : Long = 0
    var t_elapsed : Long = 0
    for( t <- tasks ){
      for( s <- sizes) {
        val lda = new LatentDirichletAllocation()
        start = System.currentTimeMillis()
        lda.run(t)
        t_elapsed = (System.currentTimeMillis() - start)
        szs = szs :+ s
        tsks = tsks :+ t
        time = time :+ t_elapsed
        var str = ""
        str = str + s + "\t" + t + "\t" + t_elapsed
      }
    }
    println("size \t tasks \t time")
    for (i <- 0 until time.length){
      println(szs(i) + "\t" + tsks(i) + "\t" + time(i))
    }
  }
}
