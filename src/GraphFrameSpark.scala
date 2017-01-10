/**
  * Created by luser on 09/01/17.
  */

import org.graphframes.{GraphFrame, examples}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


object GraphFrameSpark {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("GFDemo").setMaster("local[2]")
    val scontext = new SparkContext(sparkconf)
    val rootlogger = Logger.getRootLogger().setLevel(Level.ERROR)
    val sqlcontext = new SQLContext(scontext)


    //GRAPH VERTICES (PERSONS) + EDGES (RELATIONS)
    val v = sqlcontext.createDataFrame(List(
      ("a", "Izzie", "Stevens", 43),
      ("b", "Meredith", "Grey", 50),
      ("c", "Maggie", "Peirce", 57),
      ("d", "Ellis", "Grey", 27),
      ("e", "Richard", "Webber", 19),
      ("f", "Susan", "Grey", 50),
      ("g", "Thatcher", "Grey", 35),
      ("h", "Rose", "", 57),
      ("i", "Derek", "Shepherd", 35),
      ("j", "Addison", "Montgomery", 43),
      ("k", "Stephanie", "Edwards", 27),
      ("l", "Catherine", "Avery", 27),
      ("m", "Jackson", "Avery", 43),
      ("n", "April", "Kepner", 35),
      ("o", "Mark", "Sloan", 27),
      ("p", "Lexie", "Grey", 57),
      ("q", "Alex", "Karev", 25),
      ("r", "Heather", "Brooks", 17),
      ("s", "Teddy", "Altman", 27),
      ("t", "Henry", "Burton", 18),
      ("u", "Jo", "Wilson", 19)
    )).toDF("id", "surname", "name", "age")
    val e = sqlcontext.createDataFrame(List(
      //MEREDITH GREY - b
      ("b", "i", "love"),
      ("b", "c", "family"),
      ("b", "d", "family"),
      ("b", "p", "family"),
      ("b", "g", "family"),
      //MAGGIE PIERCE - c
      ("c", "b", "family"),
      ("c", "d", "family"),
      ("c", "e", "family"),
      //ELLIS GREY - d
      ("d", "e", "love"),
      ("d", "c", "family"),
      ("d", "b", "family"),
      //RICHARD WEBBER - e
      ("e", "d", "love"),
      ("e", "c", "family"),
      //SUSAN GREY - f
      ("f", "g", "family"),
      //THATCHER GREY - g
      ("g", "d", "love"),
      ("g", "b", "family"),
      ("g", "p", "family"),
      //ROSE - h
      ("h", "i", "love"),
      //DEREK SHEPHERD - i
      ("i", "b", "love"),
      ("i", "h", "love"),
      //ADDISON MONTGOMERY - j
      ("j", "i", "love"),
      ("j", "o", "love"),
      //CATHERINE AVERY - l
      ("l", "e", "love"),
      ("l", "m", "family"),
      //JACKSON AVERY - m
      ("m", "k", "love"),
      ("m", "n", "love"),
      ("m", "l", "family"),
      //APRIL KEPNER - n
      ("n", "m", "love"),
      //LEXIE GREY - p
      ("p", "m", "love"),
      ("p", "o", "love"),
      ("p", "b", "family"),
      ("p", "g", "family"),
      ("p", "f", "family"),
      //ALEX KAREV - q
      ("q", "r", "love"),
      ("q", "p", "love"),
      ("q", "a", "love"),
      //TEDDY ALTMAN - s
      ("s", "o", "love"),
      //HENRY BURTON - t
      ("t", "s", "love"),
      //JO WILSON - u
      ("u", "q", "love")
    )).toDF("src", "dst", "relationship")

    val g = GraphFrame(v, e)

    /*
    * DISPLAY THE SURNAMES
    */
    g.vertices.select("surname").show()
    println("--------------------------------\n")

    /*
    * DISPLAY THE RELATIONSHIPS
    */
    println("ALL THE RELATIONSHIPS")
    g.find("(a)-[e]->(b)").select("a.surname", "a.name", "e.relationship", "b.surname", "b.name").show()
    println("--------------------------------\n")

    /*
    * DISPLAY IN/OUT DEGREES
    */
    println("INDEGREES OF THE GRAPH")
    g.inDegrees.sort("id").show()
    println("OUTDEGREES OF THE GRAPH")
    g.outDegrees.sort("id").show()
    println("--------------------------------\n")

    /*
    * DISPLAY OLDEST PERSON
    */
    println("THE OLDEST PERSON(S) OF THE GRAPH")
    val tempList = new ListBuffer[String]
    var maxAge = 0
    for (x <- g.vertices.groupBy().max("age").collect()) {
      tempList += x.toString()
    }

    tempList.foreach {i =>
      maxAge = i.replace("[","").replace("]","").toInt
      maxAge.toString
    }

    g.vertices.select("surname", "name", "age").where(s"age = $maxAge").show()
    println("--------------------------------\n")

    /*
    * PERSON WHO IS MOST LOVED
    */
    println("LEADERBORD FOR PEOPLE WHO ARE MOST LOVE")
    val e2 = g.edges.filter("relationship = 'love'")
    val g2 = GraphFrame(v, e2)
    g2.vertices.join(g2.inDegrees, "id").select("surname", "name", "inDegree").orderBy(desc("inDegree")).show()
    println("--------------------------------\n")

    /*
    * PERSON WHO LOVE MOST
    */
    println("LEADERBORD FOR PEOPLE WHO LOVE MOST")
    g.vertices.join(g2.outDegrees, "id").select("surname", "name", "outDegree").orderBy(desc("outDegree")).show()
    println("--------------------------------\n")

    /*
    * PERSON WHO ARE UNHAPPY IN LOVE
    */
    println("PEOPLE WHO ARE UNHAPPY IN LOVE")
    val unhappy: DataFrame = g2.find("(a)-[]->(b); !(b)-[]->(a)")
    unhappy.select("a.surname", "a.name").groupBy("surname", "name").count().orderBy(desc("count")).show()
    println("--------------------------------\n")

    /*
    * PERSON WHO ARE HAPPY IN LOVE + AGE > 20
    */
    println("NUMBER OF PEOPLE WHO ARE HAPPY IN LOVE + AGE > 20")
    val happy: DataFrame = g2.find("(a)-[e]->(b); (b)-[e2]->(a)")
      .filter("a.age > 20")
      .select("a.surname", "a.name")
      .dropDuplicates()
      .groupBy()
      .count()
    happy.show()
    println("--------------------------------\n")

    /*
    * SUBGRAPH OF PEOPLE WHO ARE IN A FAMILY
    */
    println("CREATION OF SUBGRAPH OF FAMILY MEMBERS...")
    val path = g.find("(a)-[e]->(b)")
      .filter("e.relationship = 'family'")
    val e3 = path.select("e.src", "e.dst", "e.relationship")
    val v3 = path.select("a.id", "a.surname", "a.name", "a.age").orderBy("id").dropDuplicates()
    val g3 = GraphFrame(v3, e3)
    println("SUBGRAPH CREATED !")
    println("\n--------------------------------\n")

    /*
    * NUMBER OF FAMILIES + MEMBERS
    */
    println("FAMILIES OF THE GRAPH")
    val fams = g3.stronglyConnectedComponents.maxIter(2).run()
    val familyNumb = fams.select("component").dropDuplicates().count()
    println(s"There is ${familyNumb} families in the graph")

    val diffFamiList = new ListBuffer[String]
    for (x <- fams.select("component").dropDuplicates().collect()) {
      diffFamiList += x.toString()
    }

    var j = 1
    diffFamiList.foreach{ i =>
      println(s"Family $j : ")
      fams.select("surname", "name").filter(s"component = '${i.replace("[","").replace("]","")}'").show()
      j += 1
    }
    println("--------------------------------\n")

    /*
    * JO WILSON'S LOVED ONES > 18 YEARS OLD
    */
    println("ADULT LOVED ONES OF JO WILSON")
    g2.find("(a)-[e]->(b)")
      .filter("a.name = 'Wilson'")
      .filter("b.age > 18")
      .select("b.surname","b.name", "b.age")
      .show()
    println("--------------------------------\n")

    /*
    * THATCHER GREY ACQUAINTANCES (AT MOST SECOND LINE)
    */
    println("ACQUAINTANCES OF THATCHER GREY")
    g3.find("(a)-[]->(b) ; (b)-[]->(c) ; (c)-[]->(d)")
      .filter("a.surname = 'Thatcher'")
      .filter("c.surname != 'Thatcher'")
      .select("b.surname", "b.name", "c.surname","c.name")
        .dropDuplicates()
      .show()
    println("--------------------------------\n")

    /*
     * MOST IMPORTANT/POPULAR PERSON
    */
    println("MOST POPULAR/IMPORTANT PERSON(S)")
    val tempList2 = new ListBuffer[String]
    var tauxMaxPop = 0
    val joinDeg = g.outDegrees.join(g.inDegrees, "id")
    val columnsToSum = ListBuffer(joinDeg.col("outDegree"), joinDeg.col("inDegree"))
    val mostPopular = joinDeg.withColumn("sums", columnsToSum.reduce(_ + _))
    for (x <- mostPopular.groupBy().max("sums").collect()) {
      tempList2 += x.toString()
    }

    tempList2.foreach {i =>
      tauxMaxPop = i.replace("[","").replace("]","").toInt
      tauxMaxPop.toString
    }

    mostPopular.select("id", "sums").where(s"sums = $tauxMaxPop").show()
    println("--------------------------------\n")


    /*
     * LEAST IMPORTANT/POPULAR PERSON
    */
    println("LEAST POPULAR/IMPORTANT PERSON(S)")
    var tauxMinPop = 0
    val leastPopular = joinDeg.withColumn("sums", columnsToSum.reduce(_ - _))
    for (x <- leastPopular.groupBy().min("sums").collect()) {
      tempList2 += x.toString()
    }

    tempList2.foreach {i =>
      tauxMaxPop = i.replace("[","").replace("]","").toInt
      tauxMaxPop.toString
    }

    leastPopular.select("id", "sums").where(s"sums = $tauxMinPop").show()
    println("--------------------------------\n")
  }
}
