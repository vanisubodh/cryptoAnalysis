package crypotweetimpacts.api
import scala.collection.mutable.ArrayBuffer
import dogecoinvsbitcoin.services._
import java.io._
import org.apache.commons.io.{IOUtils}
import com.carrotsearch.hppc.CharSet
import java.nio.file.Paths

case class Ohlcv(
    time: Long,
    high: Float,
    low: Float,
    open: Float,
    volumefrom: Float,
    volumeto: Double,
    close: Float,
    conversionType: String,
    conversionSymbol: String
)
case class OhlcvEnvelop(
    Aggregated: Boolean,
    TimeFrom: Long,
    TimeTo: Long,
    Data: Array[Ohlcv]
)
case class CryptoCompareResponse(
    Response: String,
    Message: String,
    HasWarning: Boolean,
    Type: Int,
    RateLimit: Object,
    Data: OhlcvEnvelop
)
object CryptocurrencyClient {
  def main(args: Array[String]): Unit = {
    val hourlyBitcoinData = fetchHourlyHistoryByFsym("BTC", 1000)
    writeToJsonFile(hourlyBitcoinData, "bitcoin-history.json")
  }

  def fetchHourlyHistoryByFsym(fsym: String, limit: Int): Array[Ohlcv] = {
    val cryptocompareApiRoot = "https://min-api.cryptocompare.com/data/v2"
    val key = System.getenv("CRYPTOCOMPARE_KEY")

    val historyApi =
      s"$cryptocompareApiRoot/histoday?fsym=$fsym&tsym=USD&limit=$limit"
    println("API url:" + historyApi)

    val response = Http.get(historyApi, key)
    val content = IOUtils.toString(
      response.getContent(),
      org.apache.commons.codec.Charsets.UTF_8
    )

    println("Received data length:" + content.length())

    val gson = new com.google.gson.Gson()
    val json = gson
      .fromJson[CryptoCompareResponse](content, classOf[CryptoCompareResponse])

    println("Converted CryptoCompareResponse:" + json);

    json.Data.Data
  }

  def writeToJsonFile(history: Array[Ohlcv], filehandle: String) {
    val gson = new com.google.gson.Gson()
    val writer = new FileWriter(filehandle, true)

    history.foreach(o => {
      val jsonString = gson.toJson(o)
      writer.write(jsonString + "\n")
    })

    writer.flush()
    writer.close()
  }
}
