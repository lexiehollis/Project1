import org.apache.spark.sql.SparkSession

object Scenario2A {

  def scenario2a(spark: SparkSession): Unit = {

    println("The most consumed beverage on Branch1 is ")
    //Here I looked at the results from the 3 queries below and identified the highest.
    spark.sql("SELECT nameDrinkBran, count from Branch1DrTotalC ORDER BY count DESC LIMIT 1").show()


    //spark.sql("SELECT nameDrinkBran, count from Branch1DrTotal ORDER BY count DESC LIMIT 1").show
    // spark.sql("SELECT nameDrinkBran, count from Branch1DrTotalB ORDER BY count DESC LIMIT 1").show
    //spark.sql("SELECT nameDrinkBran, count from Branch1DrTotalC ORDER BY count DESC LIMIT 1").show

    //Table of DRINKTOTALS and distributions BRANCH 1 CONSA
    //spark.sql("SELECT * FROM Branch1DrTotal").show()
    //spark.sql("SELECT * FROM Branch1DrDistributeTotal").show()

    //Table of DRINKTOTALS and distributions to BRANCH 1 CONSB
    //spark.sql("SELECT * FROM Branch1DrTotalB").show()
    //spark.sql("SELECT * FROM Branch1DrDistributeTotalB").show()

    //Table of DRINKTOTALS and distributions to BRANCH 1 CONSB
    //spark.sql("SELECT * FROM Branch1DrTotalsC").show()
    //spark.sql("SELECT * FROM Branch1DrDistributionC").show()


    //TABLE CREATION PROCESS (exampleB)
    //spark.sql("Create table Branch1DrTotalB (count int, nameDrinkBran String) row format delimited fields terminated by ','")
    //spark.sql("INSERT into table Branch1DrTotalB SELECT SUM(count), nameDrinkBran FROM " +
    // "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsB ON (BevConsB.nameDrinkCon=BevBranA.nameDrinkBran) WHERE " +
    // "branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")

    //spark.sql("CREATE table Branch1DrDistributeTotalB (count Int, nameDrinkBran) row format delimited fields terminated by ','");
    //spark.sql("INSERT INTO table Branch1DrDistributeTotalB SELECT DrinkDistribution3.total/CtDrAppearanceBevA.count AS " +

  }
}
