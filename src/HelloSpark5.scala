import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object HelloSpark5 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN);
    val config = new SparkConf().setAppName("Simple App")
      .setMaster("local[*]")
    val sc = new SparkContext(config)
    val textFile1 = sc.textFile("./data/yarn.cmd")

    val mapResult1 = textFile1.map(l => l.split("\\s+").size).collect
    println(mapResult1.mkString("(", ",", ")\n"))

    val result2 = textFile1.map(l => l.split("\\s+").size)
      .reduce((a, b) => if (a > b) a else b)
    println(s" Max line is $result2")

    var result3 = textFile1.map(l => l.split("\\s+").size)
      .reduce((a, b) => math.max(a, b))
    println("[2] Max line is %d".format(result3))

    sc.stop()
  }
}
