import org.apache.spark.sql.SparkSession

  object Query1 {
    def main(args: Array[String]): Unit = {
      // create a spark session
      // for Windows
      System.setProperty("hadoop.home.dir", "C:\\winutils")

      val spark = SparkSession.builder()
        .appName("Query1")
        .config("spark.master", "local")
        .enableHiveSupport()
        .getOrCreate()
      println("created spark session")



      //spark.sql("SELECT SUM(count) FROM BevConsA JOIN BevBranA on(nameDrinkCon=nameDrinkBran) WHERE branchNum='Branch1'");
    }
  }
