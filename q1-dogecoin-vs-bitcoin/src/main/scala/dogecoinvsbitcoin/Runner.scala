package dogecoinvsbitcoin;

import org.apache.log4j._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{TimestampType, DateType, StringType}

object Runner {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Dogecoin Vs Bitcoin")
      .master("local[*]")
      .getOrCreate()

    totalOccurrences(spark)
    proportions(spark)
    popularHours(spark)
    correlation(spark)

    streamTotalOccurrences(spark)

    spark.stop()
  }

  def streamTotalOccurrences(spark: SparkSession): Unit = {
    val DataDir = "data"

    FilteredStreamClient.init(
      dataDir = DataDir,
      bearerToken = System.getenv("TWITTER_BEARER_TOKEN"),
      // rules are defined in tuples as (<value>, <tag>)
      rules = Seq(
        ("#bitcoin", "bitcoin"),
        ("#dogecoin", "dogecoin")
      )
    )

    // Create static DF to infer schema
    val staticDf = spark.read.json(DataDir)
    staticDf.printSchema()

    val streamDf = spark.readStream.schema(staticDf.schema).json(DataDir)

    val totalCount = countTotalOccurrences(spark, streamDf)
    totalCount
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

  // Total # of all tags used throughout timeframe
  def totalOccurrences(spark: SparkSession): Unit = {
    val df = spark.read.json("data")

    println("Original DF interpreted schema:")
    df.printSchema()
    df.show(5)

    val totalCount = countTotalOccurrences(spark, df)
    showTotalOccurrencesStatic(totalCount)
  }

  def countTotalOccurrences(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    // Count up all the tags
    val totalCount = df
      .groupBy("matching_rules.tag")
      .count()
      .filter($"count" > 99)
      .orderBy(desc("count"))
    
    totalCount
  }

  def showTotalOccurrencesStatic(totalCount: DataFrame): Unit = {
    println("Schema for totalCount DF:")
    totalCount.printSchema()
    totalCount.explain(true)
    totalCount.show(false)
  }

  // Proportion of what people are mentioning when they Tweet about cryptocurrency
  def proportions(spark: SparkSession): Unit = {

    import spark.implicits._
    val df = spark.read.json("data")

    println("Original DF interpreted schema:")
    df.printSchema()
    df.show(5)

    // Total # of all tags used throughout timeframe
    val totalCount = df
      .groupBy("matching_rules.tag")
      .count()
      .filter($"count" > 99)
      .orderBy(desc("count"))

    // Get the total of all the tags used
    val total = totalCount.agg(sum("count")).as("total").first()(0)

    println(s"Total # of crypto tags = $total")

    // Get the proportion of which tags are used form the total used
    val proportions = totalCount
      .select(col("tag"), round((col("count") / total), 4).as("proportion"))
      .filter($"proportion" > 0.005)

    println("Schema of the proportions DF:")
    proportions.printSchema()
    proportions.explain(true)
    proportions.show(false)
  }

  // Find the most popular hours to Tweet about crypto
  def popularHours(spark: SparkSession): Unit = {

    import spark.implicits._
    val df = spark.read.json("data")

    println("Original DF interpreted schema:")
    df.printSchema()
    df.show(5, false)

    // Take the original DF with all the tags and group them by hourly
    val hours = df
      .select(col("matching_rules.tag"), col("timestamp").cast(TimestampType))
      .groupBy($"tag", window($"timestamp", "1 hour"))
      .count()
      .drop("timestamp")

    println("Hours schema:")
    hours.printSchema()
    hours.explain(true)
    hours.show(5, false)

    // Count all the mentions of cryptocurrencies based on hourly timeframes.
    val popularHours = hours
      .groupBy($"window")
      .agg(sum("count").as("count"))
      .filter($"count" > 999)
      .sort($"count".desc)

    println("Most popular hours schema:")
    popularHours.printSchema()
    popularHours.show(false)
  }

  // Show the corrrelation in movement between Dogecoin and Bitcoin
  def correlation(spark: SparkSession): Unit = {

    import spark.implicits._
    val df = spark.read.json("data")

    println("Original DF interpreted schema:")
    df.printSchema()
    df.show(5)

    // Filter out unnecessary tags & cast timestamp as proper type.
    val bitDoge = df
      .select(
        col("matching_rules.tag"),
        col("timestamp").cast(TimestampType)
      )
      .filter(!array_contains($"tag", "crypto"))
      .filter(!array_contains($"tag", "cryptocurrency"))
      .filter(!array_contains($"tag", "ToTheMoon "))
      .filter(
        !(array_contains($"matching_rules.tag", "dogecoin") &&
          array_contains($"matching_rules.tag", "bitcoin"))
      )

    println("First DF transformation schema:")
    bitDoge.printSchema()
    bitDoge.explain(true)
    bitDoge.show(5)

    // Group the tags by hourly timeframes and get the counts of each.
    val hourly = bitDoge
      .select(col("timestamp"), col("tag").cast(StringType))
      .groupBy($"tag", window($"timestamp", "1 hour"))
      .count()

    println("Schema for the hourly DF:")
    hourly.printSchema()
    hourly.explain(true)
    hourly.show(5, false)

    // Pivot the tag column values out into their own columns
    val separated = hourly
      .groupBy($"window")
      .pivot($"tag")
      .sum("count")
      .withColumnRenamed("[bitcoin]", "bitcoin")
      .withColumnRenamed("[dogecoin]", "dogecoin")
      .orderBy("window")

    println("Schema for pivoted DF:")
    separated.printSchema()
    separated.explain(true)
    separated.show(5, false)

    // Calculate correlation coefficient
    val corrCoefficient = separated.stat.corr("bitcoin", "dogecoin")

    println(
      s"The Pearson Correlation Coefficient between bitcoin and dogecoin mentions is: $corrCoefficient"
    )
  }

}
