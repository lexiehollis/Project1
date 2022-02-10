import org.apache.spark.sql.SparkSession

class Main {

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

    println("Enter a number to select a scenario")
    val selection = readInt()

    selection match {
      case 1 =>{
        Scenario1A.scenario1a(spark)
      }
    }

  }
}
