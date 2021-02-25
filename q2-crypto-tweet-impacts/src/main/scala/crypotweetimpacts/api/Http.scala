
package dogecoinvsbitcoin.services

import org.apache.http.impl.client.HttpClients
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.utils.URIBuilder
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.NameValuePair
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.apache.http.HttpEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.entity.StringEntity

object Http {
  def get(url: String, bearerToken: String): HttpEntity = {
    val httpClient = makeHttpClient
    val uriBuilder = new URIBuilder(url);

    val httpGet = new HttpGet(uriBuilder.build())
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
    httpGet.setHeader("Content-Type", "application/json");

    val response = httpClient.execute(httpGet)
    response.getEntity()
  }

  def post(url: String, jsonPayload: String, bearerToken: String): HttpEntity = {
    val httpClient = makeHttpClient
    val uriBuilder = new URIBuilder(url);

    val httpPost = new HttpPost(uriBuilder.build())
    httpPost.setHeader(
      "Authorization",
      String.format("Bearer %s", bearerToken)
    );
    httpPost.setHeader("content-type", "application/json")

    val body = new StringEntity(jsonPayload)
    httpPost.setEntity(body);

    val response = httpClient.execute(httpPost)
    response.getEntity()
  }

  def makeHttpClient: CloseableHttpClient = {
    HttpClients.custom
      .setDefaultRequestConfig(
        RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build()
      )
      .build()
  }
}