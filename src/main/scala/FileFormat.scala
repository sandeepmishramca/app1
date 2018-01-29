
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.io._
object FileFormat {

  def mail(args:Array[String]):Unit={
    val sc=Context.getSparkContext()
    //val read_seq=sc.newAPIHadoopFile("outputs/order/sequence",classOf[SequenceFileInputFormat[IntWritable,Text]],classOf[IntWritable],classOf[Text]).map(rec=>rec.toString)
    val order=sc.textFile("data/retail_db/orders/")
    val orderMap=order.map(rec=>(rec.split(",")(0).toInt,rec))
    orderMap.saveAsNewAPIHadoopFile("outputs/orders/seq21",classOf[IntWritable],classOf[Text],classOf[SequenceFileOutputFormat[IntWritable,Text]])




  }
}
