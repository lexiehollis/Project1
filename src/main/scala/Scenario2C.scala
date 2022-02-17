import org.apache.spark.sql.SparkSession

object Scenario2C {

  def scenario2c(spark: SparkSession): Unit = {

    println("PRINT the average consumed beverage of Branch2 is ")

    //spark.sql("DROP TABLE averageBranch2Total")

   //spark.sql("SELECT * FROM averageBranch2Total").show()
   //spark.sql("Create table averageBranch2Total (count int, nameDrinkBran) row format delimited fields terminated by ','")
   //spark.sql("INSERT INTO averageBranch2Total SELECT averageBranch2A.count " +
     //"FROM averageBranch2A JOIN averageBranch2B ON" +
      //" (averageBranch2A.nameDrinkBran=averageBranch2B.nameDrinkBran) ")

    //spark.sql("Create table averageBranch2A (count int, nameDrinkBran String) row format delimited fields terminated by ','")
    //spark.sql("Create table averageBranch2B (count int, nameDrinkBran String) row format delimited fields terminated by ','")
    //spark.sql("Select * FROM averageBranch2A").show(54)
   // spark.sql("Select * FROM averageBranch2B").show(54)

  //spark.sql("INSERT INTO averageBranch2A SELECT SUM(count), nameDrinkBran FROM " +
  //"(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN consumers ON " +
    // "(nameDrinkBran=nameDrinkCon) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) " +
     //"GROUP BY nameDrinkBran")

    //spark.sql("INSERT INTO averageBranch2B SELECT SUM(count), nameDrinkBran FROM " +
      //"(SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN consumers ON " +
      //"(nameDrinkBran=nameDrinkCon) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) " +
      //"GROUP BY nameDrinkBran ")


   //spark.sql("SELECT TOP 50|percent FROM branch2totalZZ ORDER BY COUNT ASC").show(5000)

    //Here I got the total count of rows:
    //spark.sql("SELECT count(count) FROM branch2totalZZ").show()

    //Here I limited the return to the top half +1 of rows. The final row
    spark.sql("SELECT nameDrinkBran, count FROM branch2totalZZ ORDER BY count ASC LIMIT 4807").show(5000)

    //spark.sql("SELECT * FROM branch2totalZZ").show()
    //spark.sql("Create table branch2totalZZ (nameDrinkBran String, branchNum String, nameDrinkCon String, count int) row format delimited fields terminated by ','")
   // spark.sql("INSERT INTO branch2totalZZ SELECT * FROM branches JOIN consumers ON (branches.nameDrinkBran=consumers.nameDrinkCon) WHERE branchNum='Branch2' ORDER BY count ASC")
  //spark.sql("SELECT TOP 50 percent FROM (SELECT * FROM branch2totalZ ORDER by count ASC").show(5000)
   // spark.sql ("SELECT * FROM branches JOIN consumers ON (branches.nameDrinkBran=consumers.nameDrinkCon) WHERE branchNum='Branch2'").show()


  }
}
