import org.apache.spark.sql.SparkSession

  object Scenario3B {

    def scenario3b(spark: SparkSession): Unit = {

      println("These are the common beverages available in Branch 4 and 7")

     spark.sql("SELECT * branch4 INNER JOIN branch7 ON (branch4.nameDrinkBran=branch7.nameDrinkBran)").show()

     //spark.sql("create table branch4 nameDrinkBran String, branchNum int row format delimited fields terminated by ','");
     //spark.sql("INSERT INTO table branch4 SELECT nameDrinkBran, branchNum FROM " +
     //"branches WHERE branchNum='branch4' ORDER BY nameDrinkBran ASC")

     // spark.sql("SELECT * from branch4").show()

      //spark.sql("create table branch7 (nameDrinkBran String, branchNum int)row format delimited fields terminated by ','");

      //spark.sql("INSERT INTO table branch7 SELECT nameDrinkBran, branchNum int FROM " +
     //  "branches WHERE branchNum='branch7' ORDER BY nameDrinkBran ASC")

      spark.sql("SELECT * FROM branch7").show()






    }
  }
