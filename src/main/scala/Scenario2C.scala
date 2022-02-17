import org.apache.spark.sql.SparkSession

object Scenario2C {

  def scenario2c(spark: SparkSession): Unit = {


    println("PRINT the average consumed beverage of Branch2 is ")

    spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalAA ORDER BY count ASC LIMIT 1").show()
    spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalAB ORDER BY count ASC LIMIT 1").show()
    spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalAC ORDER BY count ASC LIMIT 1").show()
    spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalCA ORDER BY count ASC LIMIT 1").show()
    spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalCB ORDER BY count ASC LIMIT 1").show()
    spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalCC ORDER BY count ASC LIMIT 1").show()

  }
}
