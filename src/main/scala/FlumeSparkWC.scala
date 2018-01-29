import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumeSparkWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("yarn-client").setAppName("FlumeSparkWordCount")
    val ssc = new StreamingContext(conf,Seconds(60))
    val host=args(0)
    val port=args(1).toInt
    val stream:ReceiverInputDStream[SparkFlumeEvent]=FlumeUtils.createPollingStream(ssc,host,port)
    val messageBody=stream.map(rec=>new String(rec.event.getBody.array()))
    val flumeWC=messageBody.flatMap(_.split(" ")).map(rec=>(rec,1)).reduceByKey(_+_)
    flumeWC.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
