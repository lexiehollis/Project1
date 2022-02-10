
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
    //spark.sql("DROP TABLE CtDrAppearanceBevc2")


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


    //spark.sql("CREATE TABLE CtDrAppearanceBevA (count int, nameDrink String) row format delimited fields terminated by ','");

    //spark.sql("LOAD DATA LOCAL INPATH 'input/CountDrinkAppearanceBevA.txt' INTO TABLE CtDrAppearanceBevA")

   //spark.sql("CREATE TABLE CtDrAppearanceBevC (nameDrink String, count int) row format delimited fields terminated by ','");
   //spark.sql("LOAD DATA LOCAL INPATH 'input/CountDrinkAppearanceBevC.txt' INTO TABLE CtDrAppearanceBevC")
   // spark.sql("SELECT * FROM CtDrAppearanceBevC").show()

    //spark.sql("CREATE TABLE CtDrAppearanceBevC2 (nameDrink String, count int) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/CountDrinkAppearanceBevBranC2.txt' INTO TABLE CtDrAppearanceBevC2")
    //spark.sql("SELECT * FROM CtDrAppearanceBevC2").show()

    //spark.sql("CREATE TABLE CtDrAppearanceBevA2 (nameDrink String, count int) row format delimited fields terminated by ','");
   // spark.sql("LOAD DATA LOCAL INPATH 'input/CountDrinkAppearanceBevBranA2 2.txt' INTO TABLE CtDrAppearanceBevA2")
    //spark.sql("SELECT * FROM CtDrAppearanceBevA2").show()


    //THIS CODE CREATES A TABLE THAT HAS TOTAL SUMS BY DRINK; I USE THE CtDrAppearance table to distributed those totals among branches
    //spark.sql("Create table drinkdistribution3 (total int, nameDrinkBran String)");
    //spark.sql("Create table drinkdistribution4 (total int, nameDrinkBran String)");
    //spark.sql("INSERT INTO drinkdistribution4 SELECT SUM(count), nameDrinkBran FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsC ON (BevConsC.nameDrinkCon=BevBranA.nameDrinkBran) WHERE branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")
    //spark.sql("Create table 2DDAA (total int, nameDrinkBran String)");
    //spark.sql("Create table 2DDAB (total int, nameDrinkBran String)");
  // spark.sql("Create table 2DDAC (total int, nameDrinkBran String)");
   //spark.sql("Create table 2DDCA (total int, nameDrinkBran String)");
   //spark.sql("Create table 2DDCB(total int, nameDrinkBran String)");
   //spark.sql("Create table 2DDCC (total int, nameDrinkBran String)");

//THIS DOES SHOW SPECIFIC DRINKS TO BRANCH2
  //spark.sql("SELECT * FROM 2DDAA").show()


   //spark.sql("INSERT INTO 2DDAA SELECT SUM(count), nameDrinkBran FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")
   //spark.sql("INSERT INTO 2DDAB SELECT SUM(count), nameDrinkBran FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsB ON (BevConsB.nameDrinkCon=BevBranA.nameDrinkBran) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")
   //spark.sql("INSERT INTO 2DDAC SELECT SUM(count), nameDrinkBran FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsC ON (BevConsC.nameDrinkCon=BevBranA.nameDrinkBran) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")

 // spark.sql("INSERT INTO 2DDCA SELECT SUM(count), nameDrinkBran FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranC.nameDrinkBran) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")
  // spark.sql("INSERT INTO 2DDCB SELECT SUM(count), nameDrinkBran FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN BevConsB ON (BevConsB.nameDrinkCon=BevBranC.nameDrinkBran) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")
   //spark.sql("INSERT INTO 2DDCC SELECT SUM(count), nameDrinkBran FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN BevConsC ON (BevConsC.nameDrinkCon=BevBranC.nameDrinkBran) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran")



    //spark.sql("SELECT SUM(count), nameDrinkBran FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show()



  }

  }


