import org.apache.spark.sql.SparkSession

object Scenario4 {

  def scenario4(spark: SparkSession): Unit = {

    println("Here is a partition for Scenario 3")

   //spark.sql("Drop table CBs")

    //spark.sql("Create Table CBs (Drink4 String, branchNum4 String, branchNum7 String, Drink7 String)")
   // spark.sql("INSERT INTO table CBs SELECT * FROM branch4 JOIN branch7 ON (branch4.nameDrinkBran=branch7.nameDrinkBran)")
    //spark.sql("SELECT * FROM CBs").show()

   // spark.sql("Create table CommonBeverages (nameDrinkBran String) PARTITIONED BY (branchNum String) row format delimited fields terminated by ','")
  // spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE CommonBeverages")
     //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE CommonBeverages")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE CommonBeverages")
  }
}