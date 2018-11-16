import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object HelloSpark4 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN);
    val config = new SparkConf().setAppName("Simple App")
      .setMaster("local[*]")
    val sc = new SparkContext(config)
    val textFile1 = sc.textFile("./data/yarn.cmd")

    val mapResult1 = textFile1.map(l => l.split("\\s+"))
    mapResult1.foreach(
      l => println(l.mkString("[*]", "\n", "\n\n\n"))
    )
    val mapResult2 = textFile1.flatMap(l=>l.split("\\s+"))
    mapResult2.foreach(println)

    sc.stop()
  }
}
