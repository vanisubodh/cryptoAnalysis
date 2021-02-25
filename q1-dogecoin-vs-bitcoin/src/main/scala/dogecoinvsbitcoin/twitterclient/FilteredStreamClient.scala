package dogecoinvsbitcoin

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.nio.file.Paths
import java.nio.file.Files
import scala.concurrent.Future

import dogecoinvsbitcoin.services.FilteredStreamApi

object FilteredStreamClient {
  def init(
      dataDir: String,
      bearerToken: String,
      rules: Seq[(String, String)]
  ) = {
    streamTweetsInBackground(dataDir, bearerToken, rules)
    waitForInputTweets(dataDir)
  }

  private def streamTweetsInBackground(
      dataDir: String,
      bearerToken: String,
      rules: Seq[(String, String)]
  ) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      tweetstreamToDir(dataDir, bearerToken, rules)
    }
  }

  private def tweetstreamToDir(
      dirname: String,
      bearerToken: String,  
      rules: Seq[(String, String)],
      linesPerFile: Int = 20
  ) = {
    val readerEntity = FilteredStreamApi.getStreamReader(bearerToken, rules)

    if (null != readerEntity) {
      val reader = new BufferedReader(
        new InputStreamReader(readerEntity.getContent())
      )
      var line = reader.readLine()
      //initial filewriter, replaced every linesPerFile
      var fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
      var lineNumber = 1
      val millis = System.currentTimeMillis() //get millis to identify the file
      while (line != null) {
        if (lineNumber % linesPerFile == 0) {
          fileWriter.close()
          Files.move(
            Paths.get("tweetstream.tmp"),
            Paths.get(
              s"$dirname/tweetstream-$millis-${lineNumber / linesPerFile}"
            )
          )
          fileWriter = new PrintWriter(Paths.get("tweetstream.tmp").toFile)
        }
        fileWriter.println(line)
        line = reader.readLine()
        lineNumber += 1
      }
    }
  }

  private def waitForInputTweets(dataDir: String) = {
    var start = System.currentTimeMillis()
    var filesFoundInDir = false
    while (!filesFoundInDir && (System.currentTimeMillis() - start) < 30000) {
      filesFoundInDir = Files.list(Paths.get(dataDir)).findFirst().isPresent()
      Thread.sleep(500)
    }
    if (!filesFoundInDir) {
      println(
        "Error: Unable to populate tweetstream after 30 seconds.  Exiting.."
      )
      System.exit(1)
    }
  }
}
