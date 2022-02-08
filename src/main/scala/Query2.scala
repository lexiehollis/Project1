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



      //spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
      //"(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
      //"branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show()

    //spark.sql("SELECT drinkDistribution2DDAA.total/CtDrAppearanceBevA.count AS Result FROM " +
      //"drinkDistribution2DDAA FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)")

    spark.sql(
      "SELECT SUM(result) AS TotalConsumersBranch2ACA FROM " +
        "(SELECT drinkDistribution2DDAA.total/CtDrAppearanceBevA.count AS Result FROM " +
        "drinkDistribution2DDAA FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))").show()
//____________________________________________

    //spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
      "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsB ON (BevConsB.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
      "branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran"

    //spark.sql("SELECT drinkDistribution2DDAB.total/CtDrAppearanceBevA.count AS Result FROM " +
      "drinkDistribution2DDAB FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)")

    spark.sql(
      "SELECT SUM(result) AS TotalConsumersBranch2ACB FROM " +
        "(SELECT drinkDistribution2DDAB.total/CtDrAppearanceBevA.count AS Result FROM " +
        "drinkDistribution2DDAB FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))").show()
//___________________________________________________

    //spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
      "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsC ON (BevConsC.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
      "branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran"

    //spark.sql("SELECT drinkDistribution2DDAC.total/CtDrAppearanceBevA.count AS Result FROM " +
      "drinkDistribution2DDAC FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)")

    spark.sql(
      "SELECT SUM(result) AS TotalConsumersBranch2ACC FROM " +
        "(SELECT drinkDistribution2DDAC.total/CtDrAppearanceBevA.count AS Result FROM " +
        "drinkDistribution2DDAC FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))").show()


//______________________________________________________________________________



    //spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
   //"(SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranC.nameDrinkBran) WHERE " +
      //"branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show()

    //spark.sql("SELECT drinkDistribution2DDCA.total/CtDrAppearanceBevC.count AS Result FROM " +
     // "drinkDistribution2DDCA FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)")

    spark.sql(
      "SELECT SUM(result) AS TotalConsumersBranch2CCA FROM " +
        "(SELECT drinkDistribution2DDCA.total/CtDrAppearanceBevC.count AS Result FROM " +
        "drinkDistribution2DDCA FULL OUTER JOIN CtDrAppearanceBevC ON (nameDrinkBran=nameDrink))").show()
//______________________________________________________
    //spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
     // "(SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN BevConsB ON (BevConsB.nameDrinkCon=BevBranC.nameDrinkBran) WHERE " +
      //"branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show()

    //spark.sql("SELECT drinkDistribution2DDCB.total/CtDrAppearanceBevC.count AS Result FROM " +
     // "drinkDistribution2DDCB FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)")
    spark.sql(
      "SELECT SUM(result) AS TotalConsumersBranch2CCB FROM " +
        "(SELECT drinkDistribution2DDCB.total/CtDrAppearanceBevC.count AS Result FROM " +
        "drinkDistributionDDBC FULL OUTER JOIN CtDrAppearanceBevC ON (nameDrinkBran=nameDrink))").show()
//__________________________________________________
   // spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
   // "(SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN BevConsC ON (BevConsC.nameDrinkCon=BevBranC.nameDrinkBran) WHERE " +
     // "branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show()

    //spark.sql("SELECT drinkDistribution2DDCC.total/CtDrAppearanceBevC.count AS Result FROM " +
     // "drinkDistribution2DDCC FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)")

    spark.sql(
      "SELECT SUM(result) AS TotalConsumersBranch2CCC FROM " +
        "(SELECT drinkDistribution2DDCC.total/CtDrAppearanceBevC.count AS Result FROM " +
        "drinkDistribution2DDCC FULL OUTER JOIN CtDrAppearanceBevC ON (nameDrinkBran=nameDrink))").show()



  }
}


