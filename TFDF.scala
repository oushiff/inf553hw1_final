/**
  * Created by Franz on 9/19/16.
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.parsing.json._

object TFDF {

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
    val sparkConf = new SparkConf().setAppName("TFDF")
    val sc = new SparkContext(sparkConf)

    val textTwitter = sc.textFile(arg(0))

    val lines = textTwitter
      .flatMap(line=>JSON.parseFull(line).get.asInstanceOf[Map[String,String]].get("text"))
      .zipWithIndex()
      .flatMap ( input => {
        input._1.split(" ")
          .map(word => ((toUTF(wordStandize(word)),input._2+1),1))
      } )
      .filter(input=>input._1._1.length > 0)
      .reduceByKey(_ + _)
      .map(input => (input._1._1, Array((input._1._2, input._2))))
      .reduceByKey(_ ++ _)
      .map(input=>(input._1, input._2.length, input._2.toList.sorted))
      .sortBy(r=>r._1)

    //lines.foreach(println)

    lines.saveAsTextFile("fei_fan_tweets_tfdf_first20")

  }
}