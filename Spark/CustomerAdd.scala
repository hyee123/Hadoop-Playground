import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object CustomerAdd {
  
  
  
  def parseLine(line: String) = {
  
      val fields = line.split(",")
      val customerID = fields(0)
      val itemID = fields(1)
      val price = fields(2).toFloat
  
      (customerID, itemID, price)
  }
  
  
    def main(args: Array[String]) {
    
          Logger.getLogger("org").setLevel(Level.ERROR)
        
 
          val sc = new SparkContext("local[*]", "CutomerAdd")
          
          val lines = sc.textFile("../customer-orders.csv")   
          val rdd = lines.map(parseLine)
          
          val cleaned = rdd.map( x => (x._1, x._3))
          val reducedUp = cleaned.reduceByKey( (x,y) => x + y)
          val cleaned2 = reducedUp.map( x => (x._2, x._1)).sortByKey()
          
          
          
          val result = cleaned2.map(x => (x._2, x._1)).collect()
          
          for (results <- result)
          {
            val cID = results._1
            val cost = results._2
            println(s"$cID,$cost")
          }
      
      
      
    }
}
