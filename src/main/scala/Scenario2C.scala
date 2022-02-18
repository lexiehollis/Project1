
import org.apache.spark.sql.SparkSession

object Scenario2C {

  def scenario2c(spark: SparkSession): Unit = {

    println("Here I took the median rather than the average consumed drink. Six drinks had the same consumption amount " +
      "at the median, so they are all listed here.")

    //spark.sql("Drop table branch2totalZZ")

    //Here I selected from the new table and ordered the rows by consumed drinks in descending order and took the highest one,
    //which would be the median rather than the average
    spark.sql("Select * FROM B2rows ORDER BY count DESC LIMIT 10").show(6)


    //Here I created a new table where I limited the return of rows to the top half +1 of rows.
    //spark.sql("Create table B2rows(nameDrinkBran String, count int) row format delimited fields terminated by ','")
    //spark.sql("INSERT INTO B2rows SELECT nameDrinkBran, count FROM B2Total ORDER BY count ASC LIMIT 4807")


    //Here I got the total count of rows:
    //spark.sql("SELECT count(count) FROM B2Total").show()

    //Here I created a table of all consumed totals for each drink in Branch2
    //spark.sql("SELECT * FROM B2Total").show()
    //spark.sql("Create table B2Total (nameDrinkBran String, branchNum String, nameDrinkCon String, count int) row format delimited fields terminated by ','")
    //spark.sql("INSERT INTO B2Total SELECT * FROM branches JOIN consumers ON (branches.nameDrinkBran=consumers.nameDrinkCon) WHERE branchNum='Branch2' ORDER BY count ASC")
  }
}