import breeze.linalg.*
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._




object dataframe {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\Winutils");

    val spark = SparkSession
      .builder()
      .appName("Lab1")
      .master("local[*]")
      .getOrCreate()


//      created dataframes on fifa datasets
    val df_match = spark.read.format("csv").option("header","true").load("E:\\Matches.csv")
    val df_final = spark.read.format("csv").option("header","true").load("E:\\WorldCups.csv")
    val df_player = spark.read.format("csv").option("header","true").load("E:\\Players.csv")
    val df_match1 =spark.read.format("csv").option("header","true").load("E:\\Matches1.csv")

//    which team scored most number of goals in worldcups
//
//    val df1 = df_match.select(df_match("Home Team Name").alias("team"),df_match("Home Team Goals").alias("goals"))
//    val df2 = df_match.select(df_match("Away Team Name").alias("team"),df_match("Away Team Goals").alias("goals"))
//    val df3 = df1.union(df2)
//
//    val df4 = df3.groupBy(df3("team")).agg(count("*").alias("cnt")).orderBy(desc("cnt")).filter(col("team")isNotNull).show()

//    Which players are part of most number of world cups

//    val df5 = df_player.select(df_player("RoundID"),df_player("MatchID"),df_player("Player Name"))
//    val df6 = df_match.select(df_match("RoundID"),df_match("MatchID"),df_match("Year"))
//    val df7 = df5.join(df6,(df5("RoundID") <=> df6("RoundID") && df5("MatchID") <=> df6("MatchID"))).select(df5("Player Name"),df6("Year"))
//    val df8 = df7.distinct().groupBy(df7("Player Name")).agg(count("*").alias("cnt")).orderBy(desc("cnt")).show()



//    which player won most number of worldcups

//    val df10 = df_match.filter(df_match("Stage").isin("Final")).select(df_match("Year"),when(df_match("Home Team Goals") < df_match("Away Team Goals"),df_match("Away Team Name")).otherwise(df_match("Home Team Name")).alias("team"),when(df_match("Home Team Goals") < df_match("Away Team Goals"),df_match("Away Team Initials")).otherwise(df_match("Home Team Initials")).alias("initials"),df_match("RoundID"),df_match("MatchID"))
//    val df11 = df_player.select(df_player("RoundID"),df_player("MatchID"),df_player("Player Name"),df_player("Team Initials"))
//    val df12 = df10.join(df11,(df10("RoundID") <=> df11("RoundID") && df10("MatchID") <=> df11("MatchID") && df10("initials") <=> df11("Team Initials"))).select(df11("Player Name").alias("player"),df10("Year").alias("year"))
//    val df13 = df12.distinct().groupBy("player").agg(count("*").alias("cnt")).orderBy(desc("cnt")).show()
//
//    Which player has won most matches in world cup

//    val df14 = df_match.filter(df_match("Stage").isin("Final")).select(df_match("Year"),when(df_match("Home Team Goals") < df_match("Away Team Goals"),df_match("Away Team Name")).otherwise(df_match("Home Team Name")).alias("team"),when(df_match("Home Team Goals") < df_match("Away Team Goals"),df_match("Away Team Initials")).otherwise(df_match("Home Team Initials")).alias("initials"),df_match("RoundID"),df_match("MatchID"))
//    val df15 = df_player.select(df_player("RoundID"),df_player("MatchID"),df_player("Coach Name"),df_player("Team Initials"))
//    val df16 = df14.join(df15,(df14("RoundID") <=> df15("RoundID") && df14("MatchID") <=> df15("MatchID") && df14("initials") <=> df15("Team Initials"))).select(df15("Coach Name").alias("coach"),df14("Year"))
//    val df17 = df16.distinct().groupBy("coach").agg(count("*").alias("cnt")).orderBy(desc("cnt")).show()


//    which team scored most number of goals in finals
//    val df20 = df_match.filter(df_match("Stage").isin("Final")).select(df_match("Home Team Name"),df_match("Home Team Goals")).groupBy(df_match("Home Team Name")).agg(sum(df_match("Home Team Goals")).alias("goals")).orderBy(desc("goals")).show()

//    which countries has hosted more number of times
//      val df21 =df_final.groupBy("Country").count().orderBy(desc("count")).show()


    // number of distinct countries who have won the worldcup

//      val df22=df_final.select("Winner").distinct().count()
//      println("Number of distinct countries who have won the worldcup:" +df22);

    df_match.createOrReplaceTempView("table1")
    df_final.createOrReplaceTempView("table2")
    df_player.createOrReplaceTempView("table3")
    df_match1.createOrReplaceTempView("table4")


//    Filtering the Home teams that scored goals >=3 and <=10
//    val Goals = spark.sql("SELECT Stage,Stadium,City,Home_Team_Name FROM table4 WHERE Home_Team_Goals >= 3 AND Home_Team_Goals <= 10")
//    Goals.show()


//    pattern recognition where third poition is germany
//    val Patternreg = spark.sql("SELECT * from table2 WHERE Third LIKE 'Germany'")
//    Patternreg.show()

//  Average number of goals scored by a team in world cups
    val Average_goals = spark.sql("SELECT Country AS Teams, ROUND(AVG(GoalsScored),0) AS average_goals FROM table2 GROUP BY Country")
      Average_goals.show()


  }
}

