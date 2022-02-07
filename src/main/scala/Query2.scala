import org.apache.spark.sql.SparkSession

object Query2 {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession.builder()
      .appName("Query2")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    //spark.sql("SELECT SUM(count), nameDrinkBran FROM
    // (SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran)
    // WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show(30)
    //spark.sql("SELECT SUM(count), nameDrinkBran FROM
    // (SELECT DISTINCT nameDrinkBran, count FROM BevBranB LEFT JOIN BevConsA ON (BevCons.nameDrinkCon=BevBranA.nameDrinkBran)
    // WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show(30)



    //spark.sql("SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM
    // drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)").show(30)
    //spark.sql("Println(The total number of consumers for Branch1 is: ")
    //spark.sql(
      //"SELECT SUM(result) AS TotalConsumersBranch1 FROM " +
        //"(SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM " +
        //"drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))").show()



  }
}


