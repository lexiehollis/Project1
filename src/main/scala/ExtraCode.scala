object ExtraCode {


  //Scenario6
  //AorP//
  // This is the table I added to the data.
  //spark.sql("SELECT * FROM AorP").show(5000)
  //spark.sql("Select count(AMilk, PMilk) FROM AorP").show()
  //spark.sql("CREATE Table AorP (AMilk string, PMilk string) row format delimited fields terminated by ','")
  //spark.sql("LOAD DATA LOCAL INPATH 'input/MILK.csv' INTO TABLE AorP")

  //MilkDrinks//
  //this is a selection of only Lattes, Mochas, and Capps with their consumer counts
  //spark.sql("SELECT * FROM MilkDrinks ORDER BY count").show(5000)
  // spark.sql("Select count(count) FROM MilkDrinks").show()
  //spark.sql("Create Table MilkDrinks (drink String, count int) row format delimited fields terminated by ','");

  //spark.sql("INSERT INTO MilkDrinks SELECT nameDrinkCon, count FROM consumers WHERE nameDrinkCon LIKE '%capp%'")
  // spark.sql("INSERT INTO MilkDrinks SELECT nameDrinkCon, count FROM consumers WHERE nameDrinkCon LIKE '%LATTE%'")
  //spark.sql("INSERT INTO MilkDrinks SELECT nameDrinkCon, count FROM consumers WHERE nameDrinkCon LIKE '%MOCHA%'")

  // spark.sql("SELECT nameDrinkCon, count FROM consumers WHERE nameDrinkCon LIKE '%capp%'").show(1000)
  //spark.sql("SELECT nameDrinkCon, count FROM consumers WHERE nameDrinkCon LIKE '%LATTE%'").show()
  //spark.sql("SELECT nameDrinkCon, count FROM consumers WHERE nameDrinkCon LIKE '%MOCHA%'").show(1000)


  //spark.sql("Create table Milkproducts (nameDrinkBran String, branchNum String) row format delimited fields terminated by ','")
  //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE Milkproducts")
  //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE Milkproducts")
  //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE Milkproducts")
}




