import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsByName {
  //objective: count the average numberof friends per each name

  def parseLine(line: String) = {
  
      val fields = line.split(",")
      val name = fields(1)
      val numFriends = fields(3).toInt
  
      (name, numFriends)
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
  
    Logger.getLogger("org").setLevel(Level.ERROR)
        
 
    val sc = new SparkContext("local[*]", "FriendsByAge")
  
   
    val lines = sc.textFile("../fakefriends.csv")
    
   
    val rdd = lines.map(parseLine)
    
  
    val totalFriendsByName = rdd.mapValues(x => (x, 1))
    
    val totalFriendsReduced = totalFriendsByName.reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

    val averageFriend = totalFriendsReduced.mapValues(x => x._1 / x._2)
    
   
    val results = averageFriend.collect()
    
    results.sorted.foreach(println)
  }
    
}
