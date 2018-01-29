import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql._

object DataFrames {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder().master("local[*]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
       // .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val jsonFile="D:\\json\\sample.json"
    val jsonRead = spark.sqlContext.read.json(jsonFile)
    val j1 = jsonRead.createGlobalTempView("jsonview")
    val sql = "select action, pageInfo.pageName,entity.checkout.tripID from jsonview limit 100"
    val df = spark.sql(sql)
    println(df.show())






  }

}
