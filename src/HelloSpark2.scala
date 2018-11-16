import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object HelloSpark2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    println("Hello Spark from IntelliJ")

    val config = new SparkConf().setAppName("Simple App").setMaster("local[*]")
    val sc = new SparkContext(config)

    val textFile1 = sc.textFile("./data/yarn.cmd")
    val result = textFile1.count()
    println("Number of lines of yarn.cmd: %d".format(result))

    val numAs = textFile1.filter(l => l.contains("a")).count()
    val numBs = textFile1.filter(l => l.contains("b")).count()
    println(s"Number of lines of a: $numAs, b: $numBs")

    sc.stop()
  }
}
