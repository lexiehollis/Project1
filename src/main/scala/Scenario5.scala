
import org.apache.spark.sql.SparkSession

object Scenario5 {

  def scenario5 (spark: SparkSession): Unit = {

    //spark.sql("Drop TABLE branch4temp")

    //spark.sql("ALTER TABLE 2DDAA SET TBLPROPERTIES ('note' = 'This table and the others with similar names" +
    //  "were created to attempt more mathematically accuracy in results based on the data at hand. This attempt, however," +
     // "was difficult to achieve and slowed this project down considerably.  I ended up abandoning these efforts at mathematical" +
     // "accuracy')")

    //spark.sql("ALTER TABLE BevBranA SET TBLPROPERTIES ('note' = 'A quick look at just the beginning of this date " +
   // "indicated that the branch numbers were repeating in consistent pattern')")

    spark.sql("SHOW TBLPROPERTIES 2DDAA").show(20)
    spark.sql("SHOW TBLPROPERTIES BevBranA").show(20)



   // spark.sql("Create table branch4temp like branch4")

    //spark.sql("INSERT INTO branch4temp SELECT * FROM branch4 WHERE
    // nameDrinkBran NOT IN (SELECT nameDrinkBran FROM branch4 WHERE nameDrinkBran='SMALL_Coffee')")

    //spark.sql("INSERT OVERWRITE Table branch4 SELECT * from branch4temp")
    //spark.sql("Drop table branch4temp")
    //spark.sql("SELECT * FROM branch4").show(50)


  }}



