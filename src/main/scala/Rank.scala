object Rank {

  def main(args:Array[String]):Unit={
    getTopNElement
  }

  def getTopNElement():Unit={
    val sc = Context.getSparkContext()
    val products=sc.textFile("D:\\gitrepo\\data\\retail_db\\products\\part-00000")
    val productFilter=products.map(rec=>(rec.split(",")(4).toDouble,rec))
    val takeTop=productFilter.take(5)
    val to = products.takeOrdered(5)(Ordering[Double].on(rec=>rec.split(",")(4).toDouble))
    val sortBK=productFilter.sortByKey(false).take(5)
    val top =productFilter.takeOrdered(5)(Ordering[Double].on(rec=>rec._1))
    val top5=productFilter.top(5)(Ordering[Double].on(rec=>rec._1))
    println(top5.foreach(println))
  }
}
