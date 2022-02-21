import org.apache.spark.sql.SparkSession

object Scenario6 {

  def scenario6(spark: SparkSession): Unit = {

    //This query clearly shows that plant milk is chosen more often than animal, though not by a wide
    //margin. Still, highlighting plant-based milk in advertising could have a positive effect on sales,
    //given the growing interest in conscious consumption, especially among younger people. Likewise, this
    //information can help with anticipating future supply chains.
    println("This result shows the number of drinks sold categorized by the consumer's choice of milk: ")
    spark.sql("SELECT sum(count) as PLANT FROM MilkDRinks WHERE milkType='plant'").show()
    spark.sql("SELECT sum(count) as ANIMAL FROM MilkDRinks WHERE milkType='animal'").show()


    //Data analysis of the most popular drink, Special Cappuccino again illustrates that plant-based milk
    // is favored over animal-based,though not by a wide margin.
    println("This result shows the number of Special Cappuccinos (the most popular drink) sold " +
      "categorized by the consumer's choice of milk: ")

    spark.sql("SELECT sum(count) as SpCappPLANT FROM MilkDrinks " +
     "WHERE drink='Special_cappuccino' AND milkType='plant'").show()

    spark.sql("SELECT sum(count) as SpCappANIMAL FROM MilkDrinks " +
     "WHERE drink='Special_cappuccino' AND milkType='animal'").show()



   // Finally, Data analysis of Lattes, the drink containing the most milk, illustrates yet that plant-based milk
      // is favored over animal-based,though not by a wide margin.
    println("This result shows the number of lattes, which contains the most milk of all three drink types, sold " +
      "categorized by the consumer's choice of milk: ")
    spark.sql("SELECT sum(count) as LattePLANT FROM MilkDrinks " +
    "WHERE drink LIKE '%LATTE%' AND milkType='plant'").show()

    spark.sql("SELECT sum(count) as LatteANIMAL FROM MilkDrinks " +
     "WHERE drink LIKE '%LATTE%' AND milkType='animal'").show()




    //Here I created a new table after merging the consumer files with a new file of my own. This table includes only
    //lattes, mochas, and cappuccinos (milk drinks) from the consumers table and I added randomly generated milk
    //data that identifies each entry as either plant or animal milk.  I decided to use only these drinks, since it
     //is not clear if any of the others, such as "lites" might have milk. In any case, all drinks have that as an
    //option, so I decided stick only with those drink that were definitely milk drinks. This data can then be
    //enriched with thinking about how it might be expanded based on customers choice to add milk to other drinks.

   // spark.sql("Create table MilkDrinks (drink string, count int, milkType string) row format delimited fields terminated by ','")
    //spark.sql(("LOAD DATA LOCAL INPATH 'input/NewTable.csv' INTO TABLE MilkDrinks"))
  }
}