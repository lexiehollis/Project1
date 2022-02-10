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


      spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
      "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
      "branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show()

      spark.sql("SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM " +
     "drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)").show()

      spark.sql(
      "SELECT SUM(result) AS TotalConsumersBranch1CA FROM " +
      "(SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM " +
      "drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))").show()
      //___________________________________________________________________________________________
      spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
      "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsB ON (BevConsB.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
      "branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")

      spark.sql("SELECT drinkDistribution3.total/CtDrAppearanceBevA.count AS Result FROM " +
      "drinkDistribution3 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)")

      spark.sql(
      "SELECT SUM(result) AS TotalConsumersBranch1CB FROM " +
      "(SELECT drinkDistribution3.total/CtDrAppearanceBevA.count AS Result FROM " +
      "drinkDistribution3 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))").show()
      //_____________________________________________________________________________________________________________________________________
      spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
      "(SELECT DISTINCT nameDrinkBran, count FROM " +
      "BevBranA LEFT JOIN BevConsC ON (BevConsC.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
      "branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")

      spark.sql("SELECT drinkDistribution4.total/CtDrAppearanceBevA.count AS Result FROM drinkDistribution4 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)")

      spark.sql(
        "SELECT SUM(result) AS TotalConsumersBranch1CC FROM " +
          "(SELECT drinkDistribution4.total/CtDrAppearanceBevA.count AS Result FROM " +
          "drinkDistribution4 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))").show()

    }
  }
