import org.apache.spark.sql.SparkSession

object Scenario2B {

  def scenario2b(spark: SparkSession): Unit = {

    println("The least consumed beverage on Branch2 is ")

    //Here I looked at the results from the 6 queries below and identified the lowest.
   spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalCA ORDER BY count ASC LIMIT 1").show()

  //spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalAA ORDER BY count ASC LIMIT 1").show()
    //spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalAB ORDER BY count ASC LIMIT 1").show()
  //spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalAC ORDER BY count ASC LIMIT 1").show()
   //spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalCA ORDER BY count ASC LIMIT 1").show()
   //spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalCB ORDER BY count ASC LIMIT 1").show()
  //spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalCC ORDER BY count ASC LIMIT 1").show()

//spark.sql("SELECT * from Branch2DrTotalCC").show(100)


// This code creates tables that show totals of drinks found in Branch2; I join the drinks offered in Branch 2 with
    //the consumer count lists.
    //spark.sql("Create table Branch2DrTotalAA(count int, nameDrinkBran String) row format delimited fields terminated by ','")

   // spark.sql("INSERT into table Branch2DrTotalAA SELECT SUM(count), nameDrinkBran FROM " +
   // "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
    //"branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")


    //spark.sql("Create table Branch2DrTotalAB(count int, nameDrinkBran String) row format delimited fields terminated by ','")

    //spark.sql("INSERT into table Branch2DrTotalAB SELECT SUM(count), nameDrinkBran FROM " +
   // "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsC ON (BevConsC.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
   // "branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")


    //spark.sql("Create table Branch2DrTotalAC(count int, nameDrinkBran String) row format delimited fields terminated by ','")

    //spark.sql("INSERT into table Branch2DrTotalAC SELECT SUM(count), nameDrinkBran FROM " +
   // "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsC ON (BevConsC.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
    //"branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")


    //spark.sql("Create table Branch2DrTotalCA(count int, nameDrinkBran String) row format delimited fields terminated by ','")

    //spark.sql("INSERT into table Branch2DrTotalCA SELECT SUM(count), nameDrinkBran FROM " +
    //"(SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranC.nameDrinkBran) WHERE " +
    //"branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")


    //spark.sql("Create table Branch2DrTotalCB(count int, nameDrinkBran String) row format delimited fields terminated by ','")

    //spark.sql("INSERT into table Branch2DrTotalCB SELECT SUM(count), nameDrinkBran FROM " +
   // "(SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN BevConsB ON (BevConsB.nameDrinkCon=BevBranC.nameDrinkBran) WHERE " +
   // "branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")


    //spark.sql("Create table Branch2DrTotalCC(count int, nameDrinkBran String) row format delimited fields terminated by ','")

   // spark.sql("INSERT into table Branch2DrTotalCC SELECT SUM(count), nameDrinkBran FROM " +
   // "(SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN BevConsC ON (BevConsC.nameDrinkCon=BevBranC.nameDrinkBran) WHERE " +
   // "branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")

  }
}