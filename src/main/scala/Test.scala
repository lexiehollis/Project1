
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


    //The code below tries to show [has an error] columns (sum count, drink and branch num) where the A documents are combined on
    // the distinct drinks shown in Branch document; they are ordered by branch number
    //spark.sql ("SELECT SUM(count), nameDrinkBran, branchNum FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) ORDER BY branchNum ASC)").show(50)

    //this code shows distinct drink names from BranchA document and their total counts from ConsA
    // along with Branch numbers from BranchA that serve that drink
    //spark.sql("SELECT DISTINCT nameDrinkBran, count, branchNum FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) ORDER BY nameDrinkBran ASC").show(500)

    //The code below shows which drinks pair up with which branch and the count (replication of counts when drink appears in more than one branch)
    //spark.sql("SELECT DISTINCT nameDrinkBran, count, branchNum FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) ORDER BY nameDrinkBran ASC").show(500)

    //this code returns only Branch1 DISTINCT drinks but stll also returns ALL of the totals for that drink from ConsA;
    //to get accurate count I would need to distribute drink totals among all branches that sold that drink
    //spark.sql("SELECT DISTINCT nameDrinkBran, branchNum, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) WHERE branchNum='Branch1' ORDER BY nameDrinkBran ASC").show(500)

    // spark.sql ("SELECT nameDrinkBran, SUM(count) FROM (SELECT DISTINCT nameDrinkBran, branchNum, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran))").show(50)

    //This code shows how many times a drink appears in BevBranA; indicates how many times a drink is offered,
    //so I can distribute total counts from ConsA among branches(NOT EXACT, ACCURATE MATH, but the best I can do with
    //this data
    //spark.sql("SELECT * FROM (SELECT nameDrinkBran, COUNT(*) FROM BevBranA GROUP BY nameDrinkBran ORDER BY COUNT(*) ASC) ").show()

    //MISC search code
    //spark.sql("SELECT nameDrinkCon, count FROM BevConsA WHERE nameDrinkCon='Cold_Coffee'").show()
    //spark.sql("SELECT DISTINCT nameDrinkBran from BevBranA WHERE branchNum='Branch1' ORDER BY nameDrinkBran ASC").show(100)
    //spark.Sql("SELECT branchNum, SUM(count) AS BranchTotal FROM [tablename] GROUP BY branchNum"

    //THIS IS THE CODE I NEED TO WORK WITH

    //this code shows distinct drink names from Branch A document with total counts for those drinks from Consumer A document
    //although the code specifies totals for Branch1 it really returns totals for ALL branches; if delete the group by at the
    //end or try to change it to ORDER BY I get an error


    //spark.sql("SELECT SUM(count), nameDrinkBran FROM (SELECT DISTINCT nameDrinkBran, count FROM BevBranA LEFT JOIN BevConsA ON (BevConsA.nameDrinkCon=BevBranA.nameDrinkBran) WHERE branchNum='Branch1' ORDER BY nameDrinkBran ASC) GROUP BY nameDrinkBran").show(30)

     //spark.sql("SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink)").show(30)
    //spark.sql("SELECT SUM(result) AS TotalConsumersBranch1 FROM (SELECT drinkDistributionA2.total/CtDrAppearanceBevA.count AS Result FROM drinkDistributionA2 FULL OUTER JOIN CtDrAppearanceBevA ON (nameDrinkBran=nameDrink))").show()
    //spark.sql("SELECT * FROM CtDrAppearanceBevA").show()
  //spark.sql("SELECT * FROM drinkDistributionA2").show()
  }
}