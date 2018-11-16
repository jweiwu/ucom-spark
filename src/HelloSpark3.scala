import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object HelloSpark3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN);
    val config = new SparkConf().setAppName("Simple App")
      .setMaster("local[*]")
    val sc = new SparkContext(config)
    val textFile1 = sc.textFile("./data/yarn.cmd")
    val result1 = textFile1.filter(l => l.contains("b")).collect()
    val result2 = textFile1.filter(l => l.contains("b")).collect()
      .mkString("<", ",\n", ">\n")
    println(result1)
    println(result2)
    sc.stop()
  }
}
