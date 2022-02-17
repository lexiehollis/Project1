import org.apache.spark.sql.SparkSession

object Scenario4 {

  def scenario4(spark: SparkSession): Unit = {

    println("Here is a partition for Scenario 3")
   // spark.sql("Drop table PartBranch")

    spark.sql("SELECT * FROM PartBranch").show(5000)
    //spark.sql("Create table PartBranch (nameDrinkBev String) PARTITIONED BY (branchNum string)")
   // spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
   // spark.sql("INSERT OVERWRITE TABLE PartBranch PARTITION (branchNum) SELECT nameDrinkBran, branchNum FROM branches")

  }
}













