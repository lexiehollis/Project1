import org.apache.spark.sql.SparkSession

object Main {

  val spark = SparkSession.builder()
    .appName("Scenario1A")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("created spark session")

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    //println("Enter a number below to select a scenario." +
      "1. Scenario1a" +
      "2. Scenario1b" +
      "3. Scenario2a" +
      "4. Scenario2b" +
      "5. Scenario3a" +
      "6. Scenario3b" +
      "7. Scenario3c" +
      "8. Scenario4" +
      "9. Scenario5" +
      "10 Scenario6" +
      "11. Quit"

    //)

    println("Enter a Number")
    val selection = readInt()

    selection match {
      case 1 => {
        Scenario1A.scenario1a(spark)
      }
      case 2 => {
        Scenario1B.scenario1b(spark)
      }
      case 3 => {
        Scenario2A.scenario2a(spark)
      }
      case 4 => {
        Scenario2B.scenario2b(spark)
      }
      case 5 => {
        Scenario2C.scenario2c(spark)
      }
      case 6 => {
        Scenario3A.scenario3a(spark)
      }
      case 7 => {
        Scenario3B.scenario3b(spark)
      }
      case 8 => {
        Scenario4.scenario4(spark)
      }
      case 9 => {
        Scenario5.scenario5(spark)
      }
      case 10 => {
        Scenario6.scenario6(spark)
      }
      case 11 => {
        Tables.tabletest(spark)
      }

    }
  }
}



