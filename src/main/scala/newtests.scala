
import org.apache.spark.sql.SparkSession

object newtests {

    def test (spark: SparkSession): Unit = {

    //Distinct drinkNames in BevBranA and BevABranC (54 each)
    //spark.sql("SELECT DISTINCT nameDrinkBran FROM BevBranA ORDER BY nameDrinkBran ASC").show(60)
    //spark.sql("SELECT DISTINCT nameDrinkBran FROM BevBranC ORDER BY nameDrinkBran ASC").show(60)


    //distinct drinkNames for 'Branch 2' from BevBranA and BevBranC (A=20, C=51_
    //spark.sql("SELECT DISTINCT nameDrinkBran FROM BevBranA WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC").show(50)
    //spark.sql("SELECT DISTINCT nameDrinkBran FROM BevBranC WHERE branchNum='Branch2' ORDER BY nameDrinkBran ASC").show(54)

    //This code shows how many times a drink appears in BevBranA adn THEN BevBranC

    //spark.sql("SELECT  nameDrinkBran, COUNT(*) FROM BevBranA GROUP BY nameDrinkBran ORDER BY COUNT(*) ASC").show(200)
    spark.sql("SELECT nameDrinkBran, COUNT(*) FROM BevBranC GROUP BY nameDrinkBran ORDER BY COUNT(*) ASC").show(500)
    //SELECT DISTINCT nameDrinkBran FROM BevBranA WHERE branchNum='Branch2'

    //spark.sql("(SELECT branchNum, nameDrinkBran, COUNT(*) FROM BevBranA ORDER BY branchNum)").show(50)


    //spark.sql("SELECT DISTINCT nameDrinkBran FROM BevBranA WHERE branchNum='Branch2' FROM" +
    // "(SELECT nameDrinkBran, COUNT(*) FROM BevBranA GROUP BY nameDrinkBran ORDER BY COUNT(*) ASC)").show(50)
  }
}
