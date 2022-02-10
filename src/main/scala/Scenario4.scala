import org.apache.spark.sql.SparkSession

object Scenario4 {

  def scenario4(spark: SparkSession): Unit = {

    println("Here is a partition for Scenario 3")

    spark.sql("Create table CommonBeverages nameDrinkBran String, branchNum int PARTITIONED BY (branchNum) row format delimited fields terminated by ','")
    spark.sql("INSERT into CommonBeverage SELECT * branch4 INNER JOIN branch7 ON (branch4.nameDrinkBran=branch7.nameDrinkBran")
  }
}