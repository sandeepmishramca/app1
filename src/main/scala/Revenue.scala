import org.apache.spark.rdd.RDD

object Revenue {
  def main(args:Array[String]):Unit={
    getDailyRevenueAndOrdersItems
  }

  def getDailyRevenueAndOrdersItems():Unit={

    val sc = Context.getSparkContext()
    val orders = sc.textFile("D:\\gitrepo\\data\\retail_db\\orders\\part-00000")
    val ordersMap=orders.map(rec=>(rec.split(",")(0).toInt,rec.split(",")(1)))
    val ordersItems = sc.textFile("D:\\gitrepo\\data\\retail_db\\order_items\\part-00000")
    val orderItemsMap=ordersItems.map(rec=>(rec.split(",")(0).toInt,rec.split(",")(4).toDouble))
    val orderJoin=ordersMap.join(orderItemsMap).map(rec=>(rec._2))
    val seqOps=((intAgg:(Double,Int),intValue:Double)=>(intAgg._1+intValue,intAgg._2+1))
    val compOps=((finalAgg:(Double,Int),finalValue:(Double,Int))=>(finalAgg._1+finalValue._1,finalAgg._2+finalValue._2))
    val orderJoinABK=orderJoin.aggregateByKey((0.0,0))(seqOps,compOps)
    //trying to do reduceByKey
   // val orderJoinRBK=orderJoin.map(rec=>(rec._1,(rec._2,1))).reduceByKey((agg,value)=>(agg._1+value._1,agg._2+value._2)).map(rec=>(rec._1,(rec._2._1,rec._2._2)))
    //val dailyAggregate=orderJoinABK.map(rec=>(rec._1,(rec._2._1/rec._2._2)))
     println(orderJoinABK.take(10).foreach(println))

  }

  def getDailyRevenue():Unit={
    val sc = Context.getSparkContext()
    val orders = sc.textFile("D:\\gitrepo\\data\\retail_db\\orders\\part-00000")
    val ordersMap=orders.map(rec=>(rec.split(",")(0).toInt,rec.split(",")(1)))
    val ordersItems = sc.textFile("D:\\gitrepo\\data\\retail_db\\order_items\\part-00000")
    val orderItemsMap=ordersItems.map(rec=>(rec.split(",")(0).toInt,rec.split(",")(4).toDouble))
    val orderJoin=ordersMap.join(orderItemsMap).map(rec=>(rec._2))
    val dailyReve=orderJoin.reduceByKey((agg,value)=>(agg+value))
    println(dailyReve.take(10).foreach(println))
  }
}
