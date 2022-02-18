import org.apache.spark.sql.SparkSession

object Scenario6 {

  def scenario6(spark: SparkSession): Unit = {

   //spark.sql("Drop table NewConsumers")


 // spark.sql("SELECT * FROM AorP").show(5000)
    //spark.sql("SELECT * FROM NewConsumers").show(5000)

    spark.sql("Create table NewConsumers AS (Select nameDrinkCon, count int, AMilk, PMilk FROM consumers, AorP)")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE NewConsumers")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE NewConsumers")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE NewConsumers")
    //spark.sql(("LOAD DATA LOCAL INPATH 'input/MILK.csv' INTO TABLE NewConsumers"))

   // This is the table I added to the data.
   //spark.sql("CREATE Table AorP (AMilk string, PMilk string) row format delimited fields terminated by ','")
   //spark.sql("LOAD DATA LOCAL INPATH 'input/MILK.csv' INTO TABLE AorP")

    // spark.sql("SELECT * FROM MilkDrinks").show(5000)

    //spark.sql("Select count(count) FROM MilkDrinks").show()



    //spark.sql("Create Table MilkDrinks (drink String, count int) row format delimited fields terminated by ','");

   //completed inserts 8:30PM
    //spark.sql("INSERT INTO MilkDrinks SELECT nameDrinkCon, count FROM consumers WHERE nameDrinkCon LIKE '%capp%'")
   // spark.sql("INSERT INTO MilkDrinks SELECT nameDrinkCon, count FROM consumers WHERE nameDrinkCon LIKE '%LATTE%'")
    //spark.sql("INSERT INTO MilkDrinks SELECT nameDrinkCon, count FROM consumers WHERE nameDrinkCon LIKE '%MOCHA%'")

    // spark.sql("SELECT nameDrinkCon, count FROM consumers WHERE nameDrinkCon LIKE '%capp%'").show(1000)
    //spark.sql("SELECT nameDrinkCon, count FROM consumers WHERE nameDrinkCon LIKE '%LATTE%'").show()
   //spark.sql("SELECT nameDrinkCon, count FROM consumers WHERE nameDrinkCon LIKE '%MOCHA%'").show(1000)


    //spark.sql("Create table MilkAdd (Drink string, drinkssold int, animal string, plant string) row format delimited fields terminated by ','");
    //spark.sql("INSERT INTO MilkAdd")

   // Spark.sql("INSERT INTO MildSELECT * FROM branches WHERE nameDrinkBran LIKE '%capp%', '%LATTE%', '%MOCHA%'")

    //spark.sql("INSERT INTO MilkAdd  nameDrinkCon FROM consumers WHERE nameDrinkCon INCLUDES??



    //capp, latte, mocha, lite???(lowball data? any additional milkadd drinks can be informative; conservative approcah))

    //spark.sql("Create table Milkproducts (nameDrinkBran String, branchNum String) row format delimited fields terminated by ','")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE Milkproducts")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE Milkproducts")
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE Milkproducts")
  }
}