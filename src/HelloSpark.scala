import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object HelloSpark {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    println("Hello Spark from IntelliJ")

    val config = new SparkConf().setAppName("Simple App").setMaster("local[*]")
    val sc = new SparkContext(config)
    sc.stop()
  }
}
