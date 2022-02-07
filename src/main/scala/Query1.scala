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

      //spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
        "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
        "branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran"

      //spark.sql("SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM " +
        "drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)"

      //spark.sql(
        "SELECT SUM(result) AS TotalConsumersBranch1 FROM " +
         "(SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM " +
         "drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))"
//_____________________________________________________________________________________________________________________________________
      //spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
        "(SELECT DISTINCT nameDrinkBran, count FROM " +
        "BevBranA LEFT JOIN BevConsB ON (BevConsB.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
        "branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran"

      spark.sql("create table drinkDistributionAB (SELECT SUM(count) AS total int, nameDrinkBran FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsB ON (BevConsB.nameDrinkCon=BevBranA.nameDrinkBran) WHERE branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran");

      //spark.sql("SELECT drinkDistributionAB.total/CtDrAppearanceBevA.count AS Result FROM " +
        "drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)"



     // spark.sql(
      "SELECT SUM(result) AS TotalConsumersBranch1 FROM " +
       "(SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM " +
      "drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))"
//____________________________________________________________________________________________________________________________________________________
      //spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
      //"(SELECT DISTINCT nameDrinkBran, count FROM " +
        //"BevBranA LEFT JOIN BevConsC ON (BevConsC.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
       // "branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran"

      //spark.sql("SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM " +
      //"drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)"

      //spark.sql(
     // "SELECT SUM(result) AS TotalConsumersBranch1 FROM " +
       // "(SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM " +
       // "drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))"
    }
  }
