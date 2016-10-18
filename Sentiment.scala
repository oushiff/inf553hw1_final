/**
  * Created by Franz on 9/18/16.
  */


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.parsing.json._


object Sentiment {

  var sentiMap = collection.mutable.Map[String, String]()

  def wordStandize(word: String): String = {
    val trimedWord = word.trim
    val firstChar = trimedWord.charAt(0)
    if (firstChar == '@' || firstChar == '#')
      return ""
    var lowerWord = trimedWord.toLowerCase
    if (lowerWord.contains("://") || lowerWord.contains("rt@") || lowerWord == "rt")
      return  ""

    return """^\W+|\W+$""".r replaceAllIn(lowerWord, "")
  }

  def toUTF (word: String): String = {
    return java.net.URLEncoder.encode(word.replaceAll("\\p{C}", ""), "utf-8")
  }


  def main(arg: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Sentiment_score")
    val sc = new SparkContext(sparkConf)

    val textTwitter = sc.textFile(arg(0))
    val textAFINN = sc.textFile(arg(1))

    for (line <- textAFINN) {
      val tokens = line.split("\t")
      sentiMap(tokens(0).trim) = tokens(1).trim
    }


    val lines = textTwitter
      .flatMap(line=>JSON.parseFull(line).get.asInstanceOf[Map[String,String]].get("text"))
      .map(line=>{
        line
          .split(" ")
          .map(word=>sentiMap.getOrElse(toUTF(wordStandize(word)), "0").toInt)
          .reduce(_ + _)
      })
      .zipWithIndex()
      .map(input=>(input._2 + 1, input._1))

    //lines.foreach(println)

    lines.saveAsTextFile("fei_fan_tweets_sentiment_first20")



  }
}