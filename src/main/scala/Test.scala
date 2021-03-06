
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession.builder()
      .appName("Test")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    //this code was to create table from a previous query
    //spark.sql("create table FIRST AS SELECT BevBranA.nameDrinkBran,BevConsA.nameDrinkCon FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran)");
    //spark.sql("SELECT * FROM LAST WHERE BevBranA.nameDrinkBran=BevA.nameDrinkCon").show(100)
    //spark.sql("SELECT * FROM FIRST").show(100)
    //
//CODE1
    //The code below tries to show [has an error] columns (sum count, drink and branch num) where the A documents are combined on
    // the distinct drinks shown in Branch document; they are ordered by branch number
    //spark.sql ("SELECT SUM(count), nameDrinkBran, branchNum FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) ORDER BY branchNum ASC)").show(50)
    //spark.sql ("SELECT nameDrinkBran, SUM(count) FROM (SELECT DISTINCT nameDrinkBran, branchNum, count FROM BevBranA LEFT JOIN BevConsB ON (BevConsB.nameDrinkCon=BevBranA.nameDrinkBran))").show(50)
//CODE2
    //this code shows distinct drink names from BranchA document and their total counts from ConsA
    // along with Branch numbers from BranchA that serve that drink; the drinknames adn branch numbers repeat; shows everything(NOTHING CONSOLIDATED)
    //spark.sql("SELECT DISTINCT nameDrinkBran, count, branchNum FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) ORDER BY nameDrinkBran ASC").show(500)

//CODE3
    //this code returns only Branch1 DISTINCT drinks but stll also returns ALL of the totals for that drink from ConsA; again drinks and branch columm NOT CONSOLIDATED
    //to get accurate count I would need to distribute drink totals among all branches that sold that drink
    //spark.sql("SELECT DISTINCT nameDrinkBran, branchNum, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) WHERE branchNum='Branch1' ORDER BY nameDrinkBran ASC").show(500)

    //This code shows how many times a drink appears in BevBranA; indicates how many times a drink is offered,
    //so I can distribute total counts from ConsA among branches(NOT EXACT, ACCURATE MATH, but the best I can do with
    //this data








    //THIS CODE SHOWS THE DISTINCT DRINKS IN BRANCH2 BEVA
    //spark.sql("SELECT DISTINCT nameDrinkBran from BevBranA WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC").show(100)

    //THIS CODE SHOWS THE DISTINCT DRINKS IN BRANCH2 BEVC
    //spark.sql("SELECT DISTINCT nameDrinkBran from BevBranC WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC").show(100)


    //spark.Sql("SELECT branchNum, SUM(count) AS BranchTotal FROM [tablename] GROUP BY branchNum"

    //THIS IS THE CODE I NEED TO WORK WITH

    //this code shows distinct drink names from Branch A document (CONSOLIDATED) with total counts for those drinks from Consumer A document
    //although the code specifies totals for Branch1 it really returns totals for ALL branches; if delete the group by at the
    //end or try to change it to ORDER BY I get an error

    //spark.sql("SELECT SUM(count), nameDrinkBran FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) WHERE branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show(30)


//OTHER CODE
    //spark.sql("SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)").show(30)
    //spark.sql("SELECT SUM(result) AS TotalConsumersBranch1 FROM (SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))").show()
//I HAVE CREATED THE TABLE BELOW AND THIS IS THE CODE TO SHOW IT; it shows how many times a drink appears in BranchA; SEE CODE FOR TABLE IN TABLES QUERY
    //spark.sql("SELECT * FROM CtDrAppearanceBevA").show()

    //THIS TABLE SHOWS TOTAL count of drinks from ConsA for each distinct drink BranA; CODE FOR CREATING IT IS BELOW
    //spark.sql("SELECT * FROM drinkDistributionA2").show()

    //spark.sql("create table drinkDistributionA2 (total int, AS (SELECT SUM(count), nameDrinkBran FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) WHERE branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran)")
  }

    //spark.sql("SELECT DISTINCT nameDrinkBran, branchNum, count FROM BevBranA LEFT JOIN BevConsB ON (BevConsB.nameDrinkCon=BevBranA.nameDrinkBran) WHERE branchNum='Branch1' ORDER BY nameDrinkBran ASC").show(500)

}
//spark.sql("SELECT * FROM averageBranch2Total").show()
//spark.sql("Create table averageBranch2Total (count int, nameDrinkBran String)row format delimited fields terminated by ','")
//spark.sql("INSERT INTO averageBranch2Total SELECT averageBranch2A.count, averageBranch2A.nameDrinkBran " +
// "FROM averageBranch2A JOIN averageBranch2B ON" +
//" (averageBranch2A.nameDrinkBran=averageBranch2B.nameDrinkBran) ")

//spark.sql("Create table averageBranch2A (count int, nameDrinkBran String) row format delimited fields terminated by ','")
//spark.sql("Create table averageBranch2B (count int, nameDrinkBran String) row format delimited fields terminated by ','")

//spark.sql("Select * FROM averageBranch2B").show()

//spark.sql("INSERT INTO averageBranch2A SELECT SUM(count), nameDrinkBran FROM " +
//"(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN consumers ON " +
// "(nameDrinkBran=nameDrinkCon) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) " +
//"GROUP BY nameDrinkBran")

//spark.sql("INSERT INTO averageBranch2B SELECT SUM(count), nameDrinkBran FROM " +
//"(SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN consumers ON " +
//"(nameDrinkBran=nameDrinkCon) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) " +
//"GROUP BY nameDrinkBran ")


// spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
//  "(SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN consumers ON " +
// "(nameDrinkBran=nameDrinkCon) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) " +
//  "GROUP BY nameDrinkBran").show(100)

//spark.sql("SELECT SUM(count), nameDrinkBran FROM " +
// "(SELECT DISTINCT nameDrinkBran, count FROM BevBranC LEFT JOIN consumers ON " +
// "(nameDrinkBran=nameDrinkCon) WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC) " +
//"GROUP BY nameDrinkBran").show(100)



//spark.sql("Create table branch2total (nameDrinkBran String, branchNum String, nameDrinkCon String, count int) row format delimited fields terminated by ','")
//spark.sql("INSERT INTO branch2total SELECT * FROM branches JOIN consumers ON (branches.nameDrinkBran=consumers.nameDrinkCon) WHERE branchNum='Branch2'").show()

// spark.sql ("SELECT * FROM branches JOIN consumers ON (branches.nameDrinkBran=consumers.nameDrinkCon) WHERE branchNum='Branch2'").show()

//spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalAA ORDER BY count ASC LIMIT 1").show(54)
// spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalAB ORDER BY count ASC LIMIT 1").show(54)
// spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalAC ORDER BY count ASC LIMIT 1").show(54)
// spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalCA ORDER BY count ASC LIMIT 1").show(54)
// spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalCB ORDER BY count ASC LIMIT 1").show(54)
// spark.sql("SELECT nameDrinkBran, count from Branch2DrTotalCC ORDER BY count ASC LIMIT 1").show(54)
