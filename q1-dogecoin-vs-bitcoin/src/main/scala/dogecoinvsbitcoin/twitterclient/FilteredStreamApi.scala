package dogecoinvsbitcoin.services;

import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import java.util.ArrayList
import org.json.JSONArray;
import org.json.JSONObject;
import com.google.gson.Gson
import org.apache.http.util.EntityUtils
import org.apache.http.HttpEntity

case class Rule(value: String, tag: String)
case class Rules(add: ArrayList[Rule])

object FilteredStreamApi {
  val FilteredStreamBaseUrl =
    "https://api.twitter.com/2/tweets/search/stream"

  def getStreamReader(bearerToken: String, rules: Seq[(String, String)]): HttpEntity  = {
    setupRules(bearerToken, rulesToJson(rules))
    Http.get(FilteredStreamBaseUrl, bearerToken)
  }

  private def setupRules(bearerToken: String, rules: String): Unit = {
    val existingRuleIds = getRules(bearerToken)
    if (existingRuleIds.size() > 0) {
      deleteRules(bearerToken, existingRuleIds)
    }

    createRules(bearerToken, rules)
  }

  private def getRules(bearerToken: String) = {
    val rules = new ArrayList[String]()
    val entity = Http.get(FilteredStreamBaseUrl + "/rules", bearerToken)

    if (null != entity) {
      val json = new JSONObject(EntityUtils.toString(entity, "UTF-8"));
      if (json.length > 1) {
        val array = (json.get("data")).asInstanceOf[JSONArray]
        for (i <- 0 to array.length() - 1) {
          val jsonObject = (array.get(i)).asInstanceOf[JSONObject]
          rules.add(jsonObject.getString("id"))
        }
      }
    }

    rules
  }

  private def deleteRules(bearerToken: String, existingRuleIds: ArrayList[String]) {
    val ruleIdsJson = new Gson().toJson(existingRuleIds)
    val deleteRulesPayload = s"""{"delete": {"ids": ${ruleIdsJson}}}"""

    val entity = Http.post(
      url = FilteredStreamBaseUrl + "/rules",
      jsonPayload = deleteRulesPayload,
      bearerToken = bearerToken
    )

    if (null != entity) {
      println(EntityUtils.toString(entity, "UTF-8"));
    }
  }

  private def createRules(bearerToken: String, rules: String): Unit = {
    val entity = Http.post(
      url = FilteredStreamBaseUrl + "/rules",
      jsonPayload = rules,
      bearerToken = bearerToken
    )

    if (null != entity) {
      println(EntityUtils.toString(entity, "UTF-8"));
    }
  }

  private def rulesToJson(rules: Seq[(String, String)]): String = {
    val gson = new Gson()
    val ruleList = new ArrayList[Rule]

    rules
      .map(r => Rule(r._1, r._2))
      .foreach {
        ruleList.add(_)
      }

    gson.toJson(Rules(ruleList))
  }
}
