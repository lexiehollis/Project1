
import org.apache.spark.sql.SparkSession

object Tables {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession.builder()
      .appName("Tables")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    //spark.sql("DROP TABLE BevBranA")
    //spark.sql("DROP TABLE BevBranB")
    //spark.sql("DROP TABLE BevBranC")

    //spark.sql("create table BevConsA(nameDrinkCon String, count int) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE BevConsA")
    //spark.sql("create table BevConsB(nameDrinkCon String, count int) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE BevConsB")
    //spark.sql("create table BevConsC(nameDrinkCon String, count int) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE BevConsC")


    //spark.sql("create table BevBranA(nameDrinkBran String, branchNum String) row format delimited fields terminated by ','");
    // spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE BevBranA")
    //spark.sql("create table BevBranB(nameDrinkBran String, branchNum String) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE BevBranB")
    //spark.sql("create table BevBranC(nameDrinkBran String, branchNum String) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE BevBranC")

    //THIS CODE CREATES A TABLE THAT SHOWS HOW MANY TIMES A DRINK APPEARS IN BRANCHA DOCUMENT
    //spark.sql("CREATE TABLE CtDrAppearanceBevA (count int, nameDrink String) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/CountDrinkAppearanceBevA.txt' INTO TABLE CtDrAppearanceBevA")

//THIS CODE CREATES A TABLE THAT HAS TOTAL SUMS BY DRINK; I USE THE CtDrAppearance table to distributed those totals among branches
    //spark.sql("Create table drinkdistribution3 (total int, nameDrinkBran String)");
    //spark.sql("Create table drinkdistribution4 (total int, nameDrinkBran String)");
    //spark.sql("INSERT INTO drinkdistribution4 SELECT SUM(count), nameDrinkBran FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsC ON (BevConsC.nameDrinkCon=BevBranA.nameDrinkBran) WHERE branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")


    //spark.sql("Create table drinkdistribution3 (total int, nameDrinkBran String)");
    //spark.sql("Create table drinkdistribution4 (total int, nameDrinkBran String)");
   // spark.sql("SELECT * FROM drinkdistribution3").show()
  }

}
