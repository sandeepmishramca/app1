import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
object SoketTextStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SoketStreaming").setMaster("local[*]")
    val batchInterval=Seconds(60)
    val ssc = new StreamingContext(conf,batchInterval)
    val host=args(0)
    val port=args(1).toInt
    val stream:ReceiverInputDStream[String]=ssc.socketTextStream(host,port,StorageLevel.MEMORY_ONLY)
    val message = stream.map(rec=>rec.toString)
    val wc = message.flatMap(_.split(" ")).map(rec=>(rec,1)).reduceByKey(_+_)
    wc.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
