import org.apache.spark.sql.SparkSession

object Main {


 val spark = SparkSession.builder()
    .appName("main")
    .config("spark.master", "local")

    .enableHiveSupport()
    .getOrCreate()

  System.setProperty("hadoop.home.dir", "C:\\winutils")
  var a =
  println("created spark session")

  println("Enter a number below to select a scenario: ")
  println(
    "1. Scenario1a")
  println(
    "2. Scenario1b")
  println(
    "3. Scenario2a")
  println(
    "4. Scenario2b")
  println(
    "5. Scenario2c")
  println(
    "6. Scenario3a")
  println(
    "7. Scenario3b")
  println(
    "8. Scenario4")
  println(
    "9. Scenario5")
  println(
    "10 Scenario6")
  println(
    "11. Quit"
  )


  val selection = readInt()


  def main(args: Array[String]): Unit = {

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
        Quit.quit(spark)
      }

    }



  }

}



