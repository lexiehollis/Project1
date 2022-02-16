import org.apache.spark.sql.SparkSession

object Scenario3A {

  def scenario3a(spark: SparkSession): Unit = {

    println("These are the beverages available in Branch 1 ")

    spark.sql("SELECT DISTINCT nameDrinkBran FROM branches WHERE branchNum='Branch1' ORDER BY nameDrinkBran ASC").show(54)

    println("These are the beverages available in Branch 8")

    spark.sql("SELECT DISTINCT nameDrinkBran FROM branches WHERE branchNum='Branch8' ORDER BY nameDrinkBran ASC").show(54)

    println("These are the beverages available in Branch 10")

    spark.sql("SELECT nameDrinkBran FROM branches WHERE branchNum='Branch10' ORDER BY nameDrinkBran").show(54)






  }
}