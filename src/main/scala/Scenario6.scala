import org.apache.spark.sql.SparkSession

object Scenario6 {

  def scenario6(spark: SparkSession): Unit = {



    spark.sql("Create table MilkAdd (Drink string, drinkssold int, animal string, plant string) row format delimited fields terminated by ','");
    spark.sql("INSERT INTO MilkAdd")

   // Spark.sql("INSERT INTO MildSELECT * FROM branches WHERE nameDrinkBran LIKE '%capp%', '%LATTE%', '%MOCHA%'")

    //spark.sql("INSERT INTO MilkAdd  nameDrinkCon FROM consumers WHERE nameDrinkCon INCLUDES??

    // all of the drinks that i think have milk; have to distinguihs by name??

    //capp, latte, mocha, lite???(lowball data? any additional milkadd drinks can be informative; conservative approcah))

    //spark.sql("Create table Milkproducts (nameDrinkBran String, branchNum String) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE Milkproducts")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE Milkproducts")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE Milkproducts")
  }
}