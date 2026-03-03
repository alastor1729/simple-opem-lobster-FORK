package com.bhp.dp

import com.bhp.dp.config.{ConfigLoader, LocalConfigLoader}
import com.bhp.dp.utils.{MsTeamsUtil, RawLoaderJobUtils, SparkJob}
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.ThreadContext


object RawLoader extends SparkJob with Logging {

  def main(args: Array[String]): Unit = {

    ThreadContext.put(
      "clusterName",
      sys.env.getOrElse("DB_CLUSTER_NAME", "unknown")
    )

    val session = buildSparkSession("RawLoader")

    logger.info("Running Raw-Loader-Implementation~~~~~~~~")

    val rawLoaderArgs = new RawLoaderArgs(args.map(_.trim))

    val msTeamsUtil = rawLoaderArgs.msTeamsInfo.map(MsTeamsUtil.apply)

    if (!RawLoaderJobUtils.isLocal) {
      if (msTeamsUtil.isEmpty) {
        throw new IllegalArgumentException("MS Teams Setup must be provided for non-local runs")
      }

      if (rawLoaderArgs.ingestConfigName.nonEmpty) {
        throw new IllegalArgumentException(s"Cannot specify override config name for non-local run")
      }
    }

    if (rawLoaderArgs.rawDataPaths.nonEmpty) {
      val configLoader = rawLoaderArgs.ingestConfigName match {
        case Some(path) => LocalConfigLoader(path)
        case None       => ConfigLoader(rawLoaderArgs.fileMoverInfo, msTeamsUtil.get)
      }

      // paths to ingest were provided, run non-streaming
      val job = RawLoaderJobImpl(
        session,
        rawLoaderArgs.rawDataPaths,
        msTeamsUtil,
        rawLoaderArgs.datalakeInfos,
        configLoader,
        rawLoaderArgs.manualSendEvents,
        rawLoaderArgs.isReingest,
        parallelIngestCount = rawLoaderArgs.ingestThreads
      )

      try {
        job.execute()
      } catch {
        case e: Exception =>
          if (RawLoaderJobUtils.isLocal) {
            logger.info("Failure loading!!!!!!", e)
            session.close()
            System.exit(1)
          } else {
            throw e
          }
      }

    } else {
      val streamingImpl = RawLoaderStreamingImpl(
        session,
        rawLoaderArgs.datalakeInfos,
        ConfigLoader(rawLoaderArgs.fileMoverInfo, msTeamsUtil.get),
        rawLoaderArgs.inboundServiceBusInfo,
        msTeamsUtil.get
      )
      streamingImpl.listen(rawLoaderArgs.ingestThreads)
    }

    if (RawLoaderJobUtils.isLocal) {
      session.close()
      System.exit(0)
    }
  }

}
