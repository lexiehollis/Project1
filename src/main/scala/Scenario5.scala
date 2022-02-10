
import org.apache.spark.sql.SparkSession

object Scenario5 {

  def scenario5 (spark: SparkSession): Unit = {



    spark.sql("ALTER TABLE 2DDAA SET TBLPROPERTIES ('note' = 'This table and the others with similar names" +
      "were created to attempt more mathematically accuracy in results based on the data at hand. This attempt, however," +
      "was difficult to achieve and slowed this project down considerably.  I ended up abandoning these efforts at mathematical" +
      "accuracy')")

    spark.sql("ALTER TABLE BevBranA SET TBLPROPERTIES ('comment' = 'A quick look at just the beginning of this date " +
      "indicated that the branch numbers were repeating in consistent pattern')")
  }}



