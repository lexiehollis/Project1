import org.apache.spark.sql.SparkSession

  object Scenario3B {

    def scenario3b(spark: SparkSession): Unit = {



     // println("These are the common beverages available in Branch 4 and 7")
     spark.sql("SELECT * FROM branch4 JOIN branch7 ON (branch4.nameDrinkBran=branch7.nameDrinkBran)").show()


     //spark.sql("SELECT * FROM branch4").show()
     //spark.sql("SELECT * FROM branch7").show()

     //spark.sql("create table branch4 (nameDrinkBran String, branchNum String) row format delimited fields terminated by ','");
    // spark.sql("INSERT INTO table branch4 SELECT nameDrinkBran, branchNum FROM " +
    // "branches WHERE branchNum='Branch4' ORDER BY nameDrinkBran ASC")

      //spark.sql("create table branch7 (nameDrinkBran String, branchNum String)row format delimited fields terminated by ','");
      //spark.sql("INSERT INTO table branch7 SELECT nameDrinkBran, branchNum int FROM " +
     //"branches WHERE branchNum='Branch7' ORDER BY nameDrinkBran ASC")







    }
  }
