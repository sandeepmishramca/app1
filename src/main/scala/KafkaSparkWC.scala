import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaSparkWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaSparkStreaming").setMaster("local[*]")
    val batchInterval=Seconds(60)
    val ssc = new StreamingContext(conf,batchInterval)
    val host=args(0)
    val port=args(1).toInt
    val kafkaParams=Map[String,String]("broker-list"->"ip-172-31-11-123.ap-south-1.compute.internal:6667")
    val topicset="kafkaTopic".split(",").toSet
    val stream:InputDStream[(String,String)]=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicset)
    val kafkaMessage=stream.map(rec=>rec._2)
    val wc = kafkaMessage.flatMap(_.split(" ")).map(rec=>(rec,1)).reduceByKey(_+_)
    wc.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
