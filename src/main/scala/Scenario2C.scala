import org.apache.spark.sql.SparkSession

object Scenario2C {

  def scenario2c(spark: SparkSession): Unit = {


    println("PRINT the average consumed beverage of Branch2 is ")

spark.sql("SELECT * FROM averageBranch2Total").show()
    //spark.sql("Create table averageBranch2Total (count int, nameDrinkBran String)row format delimited fields terminated by ','")
   //spark.sql("INSERT INTO averageBranch2Total SELECT averageBranch2A.count, averageBranch2A.nameDrinkBran " +
    // "FROM averageBranch2A JOIN averageBranch2B ON" +
     //" (averageBranch2A.nameDrinkBran=averageBranch2B.nameDrinkBran) ")

    //spark.sql("Create table averageBranch2A (count int, nameDrinkBran String) row format delimited fields terminated by ','")
    //spark.sql("Create table averageBranch2B (count int, nameDrinkBran String) row format delimited fields terminated by ','")

    //spark.sql("Select * FROM averageBranch2B").show()

  //spark.sql("INSERT INTO averageBranch2A SELECT SUM(count), nameDrinkBran FROM " +
  //"(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN consumers ON " +
    // "(nameDrinkBran=nameDrinkCon) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) " +
     //"GROUP BY nameDrinkBran")

    //spark.sql("INSERT INTO averageBranch2B SELECT SUM(count), nameDrinkBran FROM " +
      //"(SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN consumers ON " +
      //"(nameDrinkBran=nameDrinkCon) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) " +
      //"GROUP BY nameDrinkBran ")


   // spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
    //  "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN consumers ON " +
     // "(nameDrinkBran=nameDrinkCon) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) " +
    //  "GROUP BY nameDrinkBran").show(100)

    //spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
     // "(SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN consumers ON " +
     // "(nameDrinkBran=nameDrinkCon) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) " +
      //"GROUP BY nameDrinkBran").show(100)



    //spark.sql("Create table branch2total (nameDrinkBran String, branchNum String, nameDrinkCon String, count int) row format delimited fields terminated by ','")
   //spark.sql("INSERT INTO branch2total SELECT * FROM branches JOIN consumers ON (branches.nameDrinkBran=consumers.nameDrinkCon) WHERE branchNum='Branch2'").show()

   // spark.sql ("SELECT * FROM branches JOIN consumers ON (branches.nameDrinkBran=consumers.nameDrinkCon) WHERE branchNum='Branch2'").show()

    //spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalAA ORDER BY count ASC LIMIT 1").show(54)
 // spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalAB ORDER BY count ASC LIMIT 1").show(54)
 // spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalAC ORDER BY count ASC LIMIT 1").show(54)
 // spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalCA ORDER BY count ASC LIMIT 1").show(54)
  // spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalCB ORDER BY count ASC LIMIT 1").show(54)
  // spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalCC ORDER BY count ASC LIMIT 1").show(54)

  }
}
