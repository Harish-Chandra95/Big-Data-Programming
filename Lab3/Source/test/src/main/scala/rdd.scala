import org.apache.spark.sql.SparkSession

  object rdd {
    def main(args: Array[String]) {

      System.setProperty("hadoop.home.dir", "C:\\Winutils");

      val sc = SparkSession
        .builder
        .appName("Lab1")
        .master("local[*]")
        .getOrCreate().sparkContext

    //  which team  won most number of worldcups
//    val worldcup = sc.textFile("E:\\Worldcups.csv").map(x => x.split(",")).filter(x => (!x(0).contains("Year"))).map(y => (y(2), 1)).reduceByKey((x, y) => (x + y)).foreach(println)
//

//      which team reached most number of finals
//      val worldcup = sc.textFile("E:\\Worldcups.csv").map(x=>x.split(",")).filter(x=>(!x(0).contains("Year")))
//      val winner = worldcup.map(y=>(y(2),1))
//      val runner = worldcup.map(y=>(y(3),1))
//      val finalists = winner.union(runner).reduceByKey((x,y)=>(x+y)).sortBy(_._2,false).foreach(println)


//      Players with most number of matches
//      val worldcup = sc.textFile("E:\\Players.csv").map(x=>x.split(",")).filter(x=>(!x(0).contains("RoundID")))
//      val views = worldcup.map(x=>(x(6),1)).reduceByKey((x,y)=>(x+y)).coalesce(1).sortBy(_._2,false).take(20).foreach(println)


//      which countries won the world cup as host
//      val  worldcup = sc.textFile("E:\\Worldcups.csv").map(x=>x.split(",")).filter(x=>(!x(0).contains("Year")))
//      val hostwinners = worldcup.filter(x=>(x(1)==x(2))).map(x=>(x(0),x(1))).foreach(println)


//      most goals scored world cup
      val worldcup = sc.textFile("E:\\Worldcups.csv").map(x=>x.split(",")).filter(x=>(!x(0).contains("Year")))
      val views = worldcup.map(x=>(x(0),x(1),x(2),x(3),x(6).toInt)).coalesce(1).sortBy(_._5,false).foreach(println)



  }
}
