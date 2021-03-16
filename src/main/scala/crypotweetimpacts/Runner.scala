package cryptotweetimpacts;

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Hello Spark SQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    // Setup tweet history
    val muskTweets         = spark.read.json("data/tweets/musk-tweets-3200.json")
    val muskDogecoinTweets = filterByDogecoin(spark, muskTweets)
    val muskBitcoinTweets  = filterByBitcoin(spark, muskTweets)

    val simmonsTweets =
      spark.read.json("data/tweets/genesimmons-tweets-3200.json")
    val simmonsDogecoinTweets = filterByDogecoin(spark, simmonsTweets)
    val simmonsBitcoinTweets  = filterByBitcoin(spark, simmonsTweets)

    // Setup cryptocurrency history
    val dogecoinHistory =
      spark.read.json("data/cryptocurrencies/dogecoin-history.json")
    val dogeClosingPriceHistory =
      closingPricesByDate(spark, dogecoinHistory)

    val bitcoinHistory =
      spark.read.json("data/cryptocurrencies/bitcoin-history.json")
    val bitcoinClosingPriceHistory =
      closingPricesByDate(spark, bitcoinHistory)

    // Calculate correlations
    val corrMuskVsDogecoin = correlateTweetsAndCurrencyReturns(
      tag = "ELON MUSK VS DOGECOIN",
      spark = spark,
      userTweets = muskDogecoinTweets,
      closingPriceHistory = dogeClosingPriceHistory
    )

    val corrSimmonsVsDogecoin = correlateTweetsAndCurrencyReturns(
      tag = "GENE SIMMONS VS DOGECOIN",
      spark = spark,
      userTweets = simmonsDogecoinTweets,
      closingPriceHistory = dogeClosingPriceHistory
    )

    val corrMuskVsBitcoin = correlateTweetsAndCurrencyReturns(
      tag = "ELON MUSK VS BITCOIN",
      spark = spark,
      userTweets = muskBitcoinTweets,
      closingPriceHistory = bitcoinClosingPriceHistory
    )

    val corrSimmonsVsBitcion = correlateTweetsAndCurrencyReturns(
      tag = "GENE SIMMONS VS BITCOIN",
      spark = spark,
      userTweets = simmonsBitcoinTweets,
      closingPriceHistory = bitcoinClosingPriceHistory
    )

    println(s"SPEARMAN CORR -> ELON MUSK VS DOGECOIN: " + corrMuskVsDogecoin)
    println(
      s"SPEARMAN CORR -> GENE SIMMONS VS DOGECOIN: " + corrSimmonsVsDogecoin
    )
    println(s"SPEARMAN CORR -> ELON MUSK VS BITCOIN: " + corrMuskVsBitcoin)
    println(
      s"SPEARMAN CORR -> GENE SIMMONS VS BITCOIN: " + corrSimmonsVsBitcion
    )

  }

  def correlateTweetsAndCurrencyReturns(
      spark: SparkSession,
      userTweets: DataFrame,
      closingPriceHistory: DataFrame,
      tag: String
  ): Double = {
    import spark.implicits._
    import org.apache.spark.mllib.stat.Statistics

    val joinExpression =
      userTweets.col("date") === closingPriceHistory.col("date")

    val joined =
      userTweets
        .join(closingPriceHistory, joinExpression)
        .drop(userTweets.col("date"))
        .withColumn("retweet_count_double", $"retweet_count".cast("double"))
        .withColumn(
          "daily_return",
          abs(log($"same_day_close" / $"prev_day_close"))
        )

    joined
      .select(
        "date",
        "retweet_count",
        "daily_return",
        "prev_day_close",
        "next_day_close"
      )

    val rddX =
      joined.select($"retweet_count_double").coalesce(1).rdd.map(_.getDouble(0))
    val rddY =
      joined.select($"daily_return").coalesce(1).rdd.map(_.getDouble(0))

    Statistics.corr(rddX, rddY, "spearman")
  }

  def closingPricesByDate(
      spark: SparkSession,
      cryptoHistory: DataFrame
  ): DataFrame = {
    import spark.implicits._

    val closePriceByDate = cryptoHistory
      .select("time", "close")
      .withColumn("date", from_unixtime($"time", "yyyy-MM-dd"))
      .orderBy(desc("date"))
      .drop("time")
      .selectExpr("date", "close")
      .withColumnRenamed("close", "same_day_close")

    val windowSpec = Window.partitionBy().orderBy(desc("date"))

    closePriceByDate
      .withColumn("prev_day_close", lag($"same_day_close", -1) over windowSpec)
      .na
      .drop
      .withColumn("next_day_close", lag($"same_day_close", 1) over windowSpec)
      .na
      .drop
      .withColumn("second_day_close", lag($"same_day_close", 2) over windowSpec)
      .na
      .drop
  }

  def filterByDogecoin(spark: SparkSession, tweets: DataFrame): DataFrame = {
    import spark.implicits._

    tweets
      .select("created_at", "public_metrics.retweet_count")
      .filter(
        lower($"text").contains("dogecoin") ||
        lower($"text").contains("doge") 
      )
      .withColumn("date", to_date($"created_at", "yyyy-MM-dd"))
      .groupBy($"date")
      .agg(sum($"retweet_count").as("retweet_count"))
      .orderBy(desc("date"))
  }

  def filterByBitcoin(spark: SparkSession, tweets: DataFrame): DataFrame = {
    import spark.implicits._

    tweets
      .select("created_at", "public_metrics.retweet_count")
      .filter(
        lower($"text").contains("bitcoin") ||
        lower($"text").contains("btc") 
      )
      .withColumn("date", to_date($"created_at", "yyyy-MM-dd"))
      .groupBy($"date")
      .agg(sum($"retweet_count").as("retweet_count"))
      .orderBy(desc("date"))
  }
}
