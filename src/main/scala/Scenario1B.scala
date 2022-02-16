
  import org.apache.spark.sql.SparkSession

  object Scenario1B {

    def scenario1b(spark: SparkSession): Unit = {

    println("This is the number of consumers in for Branch2: ")
    spark.sql("SELECT sum(count) AS Branch2totalConsumers from branches JOIN consumers ON (branches.nameDrinkBran=consumers.nameDrinkCon) WHERE " +
      "branchNum='Branch2'").show()

      //spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
      //"(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
      //"branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show()

      //spark.sql("SELECT 2DDAA.total/CtDrAppearanceBevA2.count AS Result FROM " +
      //"2DDAA FULL OUTER JOIN CtDrAppearanceBevA2 ON (nameDrinkBran=nameDrink)").show()

      //spark.sql(
      //"SELECT SUM(result) AS TotalConsumersBranch2ACA FROM " +
      //"(SELECT 2DDAA.total/CtDrAppearanceBevA2.count AS Result FROM " +
      //"2DDAA FULL OUTER JOIN CtDrAppearanceBevA2 ON (nameDrinkBran=nameDrink))").show()
      //____________________________________________

      //spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
      // "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsB ON (BevConsB.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
      //"branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show()

      //spark.sql("SELECT 2DDAB.total/CtDrAppearanceBevA2.count AS Result FROM " +
      // "2DDAB FULL OUTER JOIN CtDrAppearanceBevA2 ON (nameDrinkBran=nameDrink)").show()

      //spark.sql(
      //"SELECT SUM(result) AS TotalConsumersBranch2ACB FROM " +
      //"(SELECT 2DDAB.total/CtDrAppearanceBevA2.count AS Result FROM " +
      // "2DDAB FULL OUTER JOIN CtDrAppearanceBevA2 ON (nameDrinkBran=nameDrink))").show()
      //___________________________________________________

      //spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
      // "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsC ON (BevConsC.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
      //"branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show()

      //spark.sql("SELECT 2DDAC.total/CtDrAppearanceBevA2.count AS Result FROM " +
      //"2DDAC FULL OUTER JOIN CtDrAppearanceBevA2 ON (nameDrinkBran=nameDrink)").show()

      //spark.sql(
      // "SELECT SUM(result) AS TotalConsumersBranch2ACC FROM " +
      //"(SELECT 2DDAC.total/CtDrAppearanceBevA2.count AS Result FROM " +
      //"2DDAC FULL OUTER JOIN CtDrAppearanceBevA2 ON (nameDrinkBran=nameDrink))").show()


      //______________________________________________________________________________



      //spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
      //"(SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranC.nameDrinkBran) WHERE " +
      //"branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show()

      //spark.sql("SELECT 2DDCA.total/CtDrAppearanceBevC2.count AS Result FROM " +
      //"2DDCA FULL OUTER JOIN CtDrAppearanceBevC2 ON (nameDrinkBran=nameDrink)").show()

      //spark.sql(
      //"SELECT SUM(result) AS TotalConsumersBranch2CCA FROM " +
      // "(SELECT 2DDCA.total/CtDrAppearanceBevC2.count AS Result FROM " +
      // "2DDCA FULL OUTER JOIN CtDrAppearanceBevC2 ON (nameDrinkBran=nameDrink))").show()
      //______________________________________________________
      // spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
      // "(SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN BevConsB ON (BevConsB.nameDrinkCon=BevBranC.nameDrinkBran) WHERE " +
      // "branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show()

      //spark.sql("SELECT 2DDCB.total/CtDrAppearanceBevC2.count AS Result FROM " +
      //"2DDCB FULL OUTER JOIN CtDrAppearanceBevC2 ON (nameDrinkBran=nameDrink)").show()
      //
      // spark.sql(
      //"SELECT SUM(result) AS TotalConsumersBranch2CCB FROM " +
      // "(SELECT 2DDCB.total/CtDrAppearanceBevC2.count AS Result FROM " +
      //  "2DDCB FULL OUTER JOIN CtDrAppearanceBevC2 ON (nameDrinkBran=nameDrink))").show()
      //__________________________________________________
      //spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
      // "(SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN BevConsC ON (BevConsC.nameDrinkCon=BevBranC.nameDrinkBran) WHERE " +
      //  "branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show()

      //spark.sql("SELECT 2DDCC.total/CtDrAppearanceBevC2.count AS Result FROM " +
      //"2DDCC FULL OUTER JOIN CtDrAppearanceBevC2 ON (nameDrinkBran=nameDrink)").show()

      //spark.sql(
        //"SELECT SUM(result) AS TotalConsumersBranch2CCC FROM " +
        //  "(SELECT 2DDCC.total/CtDrAppearanceBevC2.count AS Result FROM " +
         // "2DDCC FULL OUTER JOIN CtDrAppearanceBevC2 ON (nameDrinkBran=nameDrink))").show()



    }
  }



