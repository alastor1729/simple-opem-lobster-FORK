package com.bhp.dp.utils

import com.bhp.dp.config.ConfigConverter
import com.bhp.dp.visiting.configurations.LoadDefinition
import com.bhp.dp.FileMoverInfo
import org.apache.spark.internal.Logging
import sttp.client3._
import sttp.model.StatusCode
import util.retry.blocking.{Failure => RetryFailure, Retry, RetryStrategy, Success => RetrySuccess}
import util.retry.blocking.RetryStrategy.RetryStrategyProducer

import scala.concurrent.duration.DurationInt

case class ExistingFileBusConfig(
    id: String,
    partitionKey: String,
    definitionId: String
)

class FileMoverException(message: String, cause: Throwable) extends Exception(message, cause)

object FileMoverUtils extends Logging {
  private val client = HttpURLConnectionBackend()

  private implicit val retryStrategy: RetryStrategyProducer =
    RetryStrategy.fibonacciBackOff(initialWaitDuration = 5.seconds, maxAttempts = 3)

  def getConfigBySubject(
      fileMoverInfo: FileMoverInfo,
      accountName: String,
      container: String,
      subject: String,
      teamsUtil: MsTeamsUtil
  ): Option[LoadDefinition] = {
    val request = basicRequest
      .get(
        uri"${fileMoverInfo.url}/api/configsBySubject?accountName=$accountName&container=$container&subject=$subject"
      )
      .header("x-functions-key", fileMoverInfo.secret)
      .contentType("application/json")
      .response(asString.getRight)

    val response = Retry(request.send(client)) match {
      case RetrySuccess(r)         => r
      case RetryFailure(exception) =>
        // this will report the error to Infra team channel
        teamsUtil.sendGenericError(
          exception,
          s"Failed pulling config from Datalake Services for $accountName.$container -> $subject"
        )

        // this will eventually report the problem to the corresponding team channel
        throw new FileMoverException(
          "Failed pulling config from Datalake Services. Infra team has been alerted.",
          exception
        )
    }

    response.code match {
      case StatusCode.NoContent => None
      case _                    => Some(ConfigConverter.convertFullJsonConfig(response.body))
    }
  }
}
