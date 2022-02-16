import org.apache.spark.sql.SparkSession

object Scenario1A {
  def scenario1a(spark: SparkSession): Unit = {
    // create a spark session
    // for Window

    println("The total consumers for Branch 1 is: ")

    spark.sql("SELECT Sum(total) AS Branch1totalConsumers from branch1total").show()

    //spark.sql("Create table branch1total (Total int) row format delimited fields terminated by ','");

    //spark.sql("SELECT * from branch1total").show()

    //spark.sql("INSERT into branch1total SELECT SUM(result) AS TotalConsumersBranch1CA FROM " +
      //"(SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM drinkDistributionA2 " +
     // "FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))")

    //spark.sql("INSERT into branch1total SELECT SUM(result) AS TotalConsumersBranch1CB FROM " +
      //"(SELECT drinkDistribution3.total/CtDrAppearanceBevA.count AS Result FROM " +
      //"drinkDistribution3 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))")

    //spark.sql("INSERT into branch1total SELECT SUM(result) AS TotalConsumersBranch1CC FROM " +
     // "(SELECT drinkDistribution4.total/CtDrAppearanceBevA.count AS Result FROM " +
      //"drinkDistribution4 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))")
//_____________________________________________________________
   // spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
    //  "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
    //  "branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show()

    //spark.sql("SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM " +
     // "drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)").show()

    //spark.sql(
    //  "SELECT SUM(result) AS TotalConsumersBranch1CA FROM " +
     //   "(SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM " +
      //  "drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))").show()
    //___________________________________________________________________________________________
   // spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
     // "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsB ON (BevConsB.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
      //"branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show

    //spark.sql("SELECT drinkDistribution3.total/CtDrAppearanceBevA.count AS Result FROM " +
     // "drinkDistribution3 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)").show

   // spark.sql(
    //  "SELECT SUM(result) AS TotalConsumersBranch1CB FROM " +
      //  "(SELECT drinkDistribution3.total/CtDrAppearanceBevA.count AS Result FROM " +
       // "drinkDistribution3 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))").show()
    //_____________________________________________________________________________________________________________________________________
   // spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
    //  "(SELECT DISTINCT nameDrinkBran, count FROM " +
     // "BevBranA LEFT JOIN BevConsC ON (BevConsC.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
    //  "branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show

    //spark.sql("SELECT drinkDistribution4.total/CtDrAppearanceBevA.count AS Result FROM " +
     // "drinkDistribution4 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)").show

    //spark.sql(
     // "SELECT SUM(result) AS TotalConsumersBranch1CC FROM " +
      //  "(SELECT drinkDistribution4.total/CtDrAppearanceBevA.count AS Result FROM " +
       // "drinkDistribution4 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))").show()

  }
}

