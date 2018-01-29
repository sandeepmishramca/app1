import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

object Context {



  def getSparkContext():SparkContext={
    val conf = new SparkConf().setAppName("Spark1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc
  }
  def getOrdersStatus(filePath:String):Unit={

    val sc = getSparkContext()
    val inRdd = sc.textFile(filePath)
    val status = inRdd.map(_.split(",")(3)).map((_,1)).reduceByKey(_+_).
      saveAsTextFile("C:\\Users\\564151\\IdeaProjects\\app1\\src\\main\\resource\\orderstatus")
    //val o = status.collect()

    //println(o.foreach(println))

  }
  def getOrderByKey(filePath:String):Unit={
    val sc = getSparkContext()
    val inRdd = sc.textFile(filePath)
    val status = inRdd.map(_.split(",")(3)).map((_,1)).groupByKey().map(t=>(t._1,t._2.sum))
    val o = status.collect()
    println(o.foreach(println))

  }
  def getAggarigateByKey(filePath:String):Unit={
    val sc = getSparkContext()
    val inRdd = sc.textFile(filePath)
    val initialCount=0
    val combine=(k:Int,v:Int)=>k+v
    val sumPatritionsCount=(p1:Int,p2:Int)=>p1+p2
    val status = inRdd.map(_.split(",")(3)).map((_,1)).aggregateByKey(0)(combine,sumPatritionsCount)
    val o = status.collect()
    println(o.foreach(println))

  }



  def main(args:Array[String]): Unit ={
    val filePath="C:\\Users\\564151\\IdeaProjects\\app1\\src\\main\\resource\\part-00000"
   //val filePath=args(0)
    //val r = getOrdersStatus(filePath)
    getOrdersStatus(filePath)




  }

}
