import Election.reCalculateWithAllianc

object Election {
  def main(args: Array[String]): Unit = {
    val sc = Context.getSparkContext()
    //state	constituency	candidate_name	sex	age	category	partyname	partysymbol	general	postal	total	pct_of_total_votes	pct_of_polled_votes	totalvoters
    val election2014=sc.textFile("D:\\gitrepo\\data\\electionresults\\ls2014.tsv")
    //val removeHeader=election2014.mapPartitionsWithIndex((indx,iter)=> (if(indx==0) iter.drop(0) else iter))
    val eFilter = election2014.filter(rec=>rec.split("\t")(0)=="Bihar")
    val eFilterMap=eFilter.map(rec=>((rec.split("\t")(0),rec.split("\t")(1)),(rec.split("\t")(6),rec.split("\t")(10).toInt)))
    val eFilterMapGBK=eFilterMap.groupByKey.map(rec=>(rec._1._1,reCalculateWithAllianc(rec._2)))
      .map(rec=>(rec._1,rec._2.toList.sortBy(s=> -s._2)))
      .map(rec=>((rec._1,rec._2(0)._1),rec._2(0)._2)).countByKey()
    println(eFilterMapGBK.take(10).foreach(println))
    //val l = Iterable((BSP,12688), (JD(U),95790), (LJP,455652), (INC,230152), (BMF,8957), (BJKVP,3711), (AAAP,6344), (BMUP,3882), (SJP(R),3198), (SP,4185), (AIFB,3216), (IND,8896), (IND,21045), (IND,24399), (IND,7544), (NOTA,15047))
    //val m=Map("JMBP" -> 3217, "BJP" -> 355120, "BSP" -> 15500, "RJD" -> 314172, "JD(U)" -> 107008, "BMUP" -> 2818, "BJPARTY" -> 6765, "BED" -> 9747, "NOTA" -> 19163, "AAAP" -> 5099, "IND"  -> 24645)
    //BJP 358040+355120  RJD 407978+368937
  }
 def reCalculateWithAllianc(rec:Iterable[(String, Int)]):Iterable[(String, Int)]={
  rec.map(r=>{
    if(r._1=="INC" || r._1=="SP")
      ("ALLY",r._2)
    else
      r
  }).groupBy(r=>r._1)
     .map(r=>(r._1,r._2.map((_._2)).sum))
 }


}
