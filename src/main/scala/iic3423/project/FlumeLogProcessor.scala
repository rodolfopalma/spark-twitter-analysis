import scala.util.matching.Regex

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume._

object FlumeLogProcessor {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: FlumeLogProcessor <hostname> <port> <target_dir>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("FlumeLogProcessor")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val stream = FlumeUtils.createStream(ssc, args(0), args(1).toInt)
    // ref: https://github.com/looker/spark_log_data/blob/master/src/main/scala/logDataWebinar.scala
    stream.count().map(cnt => "Received " + cnt + " events from Flume").print()
    val mapStream = stream.map(event => new String(event.event.getBody().array(), "UTF-8"))
    mapStream.foreachRDD{
      rdd => rdd.filter{
        line => line.matches(".*sales.*")
      }.saveAsTextFile(args(2))
    }
    ssc.start()
    ssc.awaitTermination()
  }
}