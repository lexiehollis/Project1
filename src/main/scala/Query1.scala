import org.apache.spark.sql.SparkSession

  object Query1 {
    def main(args: Array[String]): Unit = {
      // create a spark session
      // for Windows
      System.setProperty("hadoop.home.dir", "C:\\winutils")

      val spark = SparkSession.builder()
        .appName("Query1")
        .config("spark.master", "local")
        .enableHiveSupport()
        .getOrCreate()
      println("created spark session")


      spark.sql(
        "SELECT SUM(result) AS TotalConsumersBranch1 FROM " +
          "(SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))").show()
    }
  }
