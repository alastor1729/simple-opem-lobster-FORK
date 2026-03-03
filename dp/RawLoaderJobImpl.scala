package com.bhp.dp

import com.bhp.dp.config.{ConfigHelper, IConfigLoader}
import com.bhp.dp.events.EventSender
import com.bhp.dp.logging.LoggingConstants
import com.bhp.dp.utils._
import com.bhp.dp.visiting.components.WriteInfo
import com.bhp.dp.visiting.configurations.LoadDefinition
import com.bhp.dp.visiting.utils.{AppendLineage, SchemaMutations, SnapshotDateReader}
import com.bhp.dp.visiting.utils.SnapshotDateReader.SOURCE_SNAPSHOT_KEY
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.logging.log4j.{CloseableThreadContext, ThreadContext}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.util
import java.util.{Properties, UUID}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.Executors
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.{Failure, Success, Try, Using}

class RawLoaderJobImpl(
    sparkSession: SparkSession,
    rawDataPaths: Seq[String],
    msTeasmUtil: Option[MsTeamsUtil],
    datalakeInfos: Seq[DatalakeInfo],
    configLoader: IConfigLoader, // this can either be a config override, or a FileMover supported loader
    sendEvents: Boolean = false,
    isReingest: Boolean = false,
    parallelIngestCount: Int = 10
) extends Logging {

  import RawLoaderJobImpl._

  // The actual execution method of version 1 of the raw loader
  def execute(): Unit = {
    logger.info(s"""Running manual ingest
                   |rawDataPaths: ${rawDataPaths.mkString(",")}
                   |isReingest: $isReingest""".stripMargin)

    val fs = FileSystemUtils.getFileSystem(sparkSession)

    val rawAsPaths = rawDataPaths.map(r => fs.makeQualified(new Path(r)))
    val rawFolders = rawAsPaths.filter(p => fs.getFileStatus(p).isDirectory)

    val rawPathsToParse = rawAsPaths
      .groupBy(rawPath => {
        val possibleParents = rawFolders.filter(p => rawPath.toString.startsWith(p.toString))

        if (possibleParents.nonEmpty) {
          // Specified path is contained by a folder... roll up into the folder.
          possibleParents.minBy(_.toString.length)
        } else {
          rawPath
        }

      })
      .keySet

    val parentToIngestInfo = rawPathsToParse.map(rp => {
      val subPaths = getFiles(fs, rp.toString)

      val rawIsFile = !rawFolders.contains(rp)

      val ingestInfosPerParent: Seq[IngestInfo] = subPaths.flatMap(p => {

        val subject = p

        val datalakeInfoOpt = if (datalakeInfos.size == 1) {
          Some(datalakeInfos.head)
        } else {
          datalakeInfos.find(dlI => {
            val mountString =
              s"${ConfigHelper.getLobMountString(dlI.lineOfBusiness)}${dlI.environment}"
            subject.contains(mountString)
          })
        }

        datalakeInfoOpt match {
          case Some(datalakeInfo) =>
            val eventSenders = if (sendEvents) datalakeInfo.eventSenders else Seq.empty

            val trimmedSubject =
              ConfigHelper.cleanAbfssSubject(subject)

            val container = ConfigHelper
              .getContainerAbfss(subject)
              .getOrElse("unknown")

            val loadDefinition = configLoader.getConfig(datalakeInfo, container, trimmedSubject)

            if (loadDefinition.isEmpty) {
              if (rawIsFile) {
                // single file specified, throw to fail job
                throw new IllegalArgumentException(
                  s"Could not locate config for subject $p converted in ${datalakeInfo.accountInfo
                    .map(_.name)
                    .getOrElse("unknown")}.$container"
                )
              } else {
                // folder specified, likely an odd file (maybe _latest) in the folder.
                // just warn... may need to change / TODO for Ronald???
                logger.warn(
                  s"Could not locate config for subject $p in ${datalakeInfo.accountInfo.map(_.name).getOrElse("unknown")}.$container"
                )
              }
            }

            // remove DBFS for original subject
            loadDefinition.map(ld =>
              IngestInfo(
                subject.replace("dbfs:", ""),
                trimmedSubject,
                p,
                ld,
                eventSenders
              )
            )
          case None =>
            if (rawIsFile) {
              // single file specified, throw to fail job
              throw new IllegalArgumentException(
                s"Could not locate matching datalake for subject $p"
              )
            } else {
              // folder specified, likely an odd file (maybe _latest) in the folder. just warn.
              logger.warn(s"Could not locate matching datalake for subject $p")
            }
            None
        }
      })

      rp -> ingestInfosPerParent
    })

    if (isReingest) {

      parentToIngestInfo.foreach { case (parent, infosPerParent) =>
        if (fs.getFileStatus(parent).isDirectory) {
          // parent is a directory, delete all
          logger.info(s"Deleting all INGESTED files for source $parent")
          val allLoadDefinitions = infosPerParent.map(_.loadDefinition).toSet

          allLoadDefinitions.foreach(ld => {
            val dataSourceTokens = getDataSourceTokens(ld) ++ (SnapshotDateReader
              .optionalSnapshotFromFilePath(parent.toString) match {
              case Some(snapshot) => Map(SOURCE_SNAPSHOT_KEY -> snapshot)
              case None           => Map.empty[String, String]
            })

            ld.source
              .getAllRuntimeTokens(parent.toString)
              .foreach(tokens => {
                ld.target.deleteData(sparkSession, dataSourceTokens ++ tokens, None)
              })

          })
        } else {
          // parent is just a file, delete per file
          logger.info(s"Deleting INGESTED files for source $parent")
          infosPerParent.foreach(info => {
            val loadDefinition = info.loadDefinition

            val dataSourceTokens = getDataSourceTokens(loadDefinition) ++ Map(
              SOURCE_SNAPSHOT_KEY -> SnapshotDateReader.readSnapshotFromFilePath(
                info.originalSubject
              )
            )

            loadDefinition.source
              .getAllRuntimeTokens(info.originalSubject)
              .foreach(tokens => {
                loadDefinition.target
                  .deleteData(sparkSession, dataSourceTokens ++ tokens, Some(info.originalSubject))
              })
          })
        }

      }
    }

    implicit val ec: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(parallelIngestCount))

    val statusEc = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))

    val totalCount   = parentToIngestInfo.map(_._2.size).sum
    val currentCount = new AtomicInteger(0)
    val completed    = new AtomicBoolean(false)

    val statusFuture = Future {
      logger.info(s"Ingested ${currentCount.get()}/$totalCount")
      var previousCount = 0
      while (!completed.get() && previousCount < totalCount) {
        completed.synchronized {
          completed.wait(2.minutes.toMillis)
        }

        val count = currentCount.get()
        if (previousCount != 0) {
          logger.info(s"Ingested $count/$totalCount (${(count - previousCount) / 2}/min)")
        } else if (count != 0) {
          logger.info(s"Ingested $count/$totalCount")
        }

        previousCount = count
      }
    }(statusEc)

    val contextValues = ThreadContext.getImmutableContext
    val ingestFutures = parentToIngestInfo.flatMap { case (_, infos) =>
      infos.map(info => {
        val subject        = info.originalSubject
        val loadDefinition = info.loadDefinition
        subject -> Future {
          Using(
            CloseableThreadContext
              .put(LoggingConstants.INGEST_ID, info.ingestId)
              .put(LoggingConstants.FILE_PATH, subject)
              .putAll(contextValues)
          ) { _ =>
            {

              logger.info(
                s"Writing: $subject From: ${loadDefinition.source.configurationSummary} To: ${loadDefinition.target.configurationSummary}"
              )

              val fileContextValues = ThreadContext.getImmutableContext
              try {
                WriteSourceToTarget(
                  sparkSession,
                  loadDefinition,
                  subject,
                  info.ingestId,
                  info.ingestId,
                  info.eventSenders,
                  fileContextValues
                )
                logger.info(s"Finished: $subject")
              } catch {
                case loadException: LoadException =>
                  msTeasmUtil.foreach(su => su.sendLoadError(loadException, isManual = true))
                  logger.error(
                    s"Exception: $subject --> ${loadException.getMessage}",
                    loadException
                  )
                  throw loadException.getCause
              } finally {
                currentCount.incrementAndGet()
              }
            }
          }
        }
      })
    }

    Await.ready(Future.sequence(ingestFutures.map(_._2)), Duration.Inf)

    completed.synchronized {
      completed.set(true)
      completed.notifyAll()
    }

    Await.ready(statusFuture, 5.minutes)

    val failureCases = ingestFutures.flatMap { case (subject, future) =>
      future.value match {
        case Some(Success(_))         => None
        case Some(Failure(exception)) => Some(subject -> exception.toString)
        case None                     => None
      }
    }

    if (failureCases.nonEmpty) {
      val exceptions = failureCases.map(_._2)
      val subjects   = failureCases.map(_._1)

      val andMoreSubjects = if (subjects.size > 10) s"\nand ${subjects.size - 10} more" else ""
      val andMoreExceptions =
        if (exceptions.size > 2) s"\n\nand ${exceptions.size - 2} more" else ""
      val message =
        s"""The following subjects failed:
           |${subjects.take(10).mkString("\n")}$andMoreSubjects
           |
           |With the following exceptions:
           |${exceptions.take(2).mkString("\n\n")}$andMoreExceptions
           |""".stripMargin

      throw new Exception(message)
    }

    logger.info(s"""Ingested ${ingestFutures.size} files successfully.""")
  }

  def getFiles(fs: FileSystem, stringPath: String): Seq[String] = {
    val iter = fs.listFiles(new Path(stringPath), true)

    val lb = ListBuffer.empty[Path]
    while (iter.hasNext) {
      val n = iter.next()
      lb += n.getPath
    }

    lb.toList
      .filterNot(p => p.toString.endsWith(".crc") || p.toString.endsWith("_SUCCESS"))
      .map(_.toString)
  }
}

case class IngestInfo(
    originalSubject: String,
    trimmedSubject: String,
    path: String,
    loadDefinition: LoadDefinition,
    eventSenders: Seq[EventSender],
    ingestId: String = UUID.randomUUID().toString
)

object RawLoaderJobImpl extends Logging {

  def apply(
      sparkSession: SparkSession,
      rawDataPaths: Seq[String],
      msTeamsUtil: Option[MsTeamsUtil],
      datalakeInfos: Seq[DatalakeInfo],
      configLoader: IConfigLoader,
      sendEvents: Boolean = false,
      isReingest: Boolean = false,
      parallelIngestCount: Int = 10
  ): RawLoaderJobImpl = {

    new RawLoaderJobImpl(
      sparkSession,
      rawDataPaths,
      msTeamsUtil,
      datalakeInfos,
      configLoader,
      sendEvents,
      isReingest,
      parallelIngestCount
    )
  }

  def getDataSourceTokens(loadDefinition: LoadDefinition): Map[String, String] = {
    loadDefinition.source.tokens ++
      loadDefinition.target.tokens ++
      CreateRuntimeTokens()
  }

  def WriteSourceToTarget(
      sparkSession: SparkSession,
      loadDefinition: LoadDefinition,
      source: String,
      ingestId: String,
      correlationId: String,
      eventSender: Seq[EventSender],
      fileContextValues: util.Map[String, String] = new util.HashMap[String, String]()
  ): Seq[WriteInfo] = {

    val configName = loadDefinition.configName.getOrElse("UNKNOWN")
    eventSender.foreach(_.sendStart(source, configName, ingestId, correlationId))

    try {
      val snapshotDate = SnapshotDateReader.readSnapshotFromFilePath(source)
      val dataSourceTokens = getDataSourceTokens(loadDefinition) ++
        Map(SOURCE_SNAPSHOT_KEY -> snapshotDate)

      val threadSparkSession = sparkSession.newSession()
      val sourceName         = new File(source).getName
      threadSparkSession.sparkContext.setJobDescription(sourceName)

      val sourceData = loadDefinition.source.getSourceData(threadSparkSession, source, ingestId)

      // Write all offsets of the source data in parallel (ex: multiple ranges read in from an excel file)
      val writeInfo = sourceData.dataframe.par
        .flatMap(dfm => {
          // since this uses .par it may run in a different Thread. set the context.
          Using(CloseableThreadContext.putAll(fileContextValues)) { _ =>
            {
              if (dfm.dataFrame.isEmpty) {
                logger.warn(
                  s"Skipping write of $source to ${loadDefinition.target.configurationSummary} due to empty DataFrame"
                )
                None
              } else {
                Some(
                  loadDefinition.target.writeData(
                    dfm.copy(
                      tokens = dfm.tokens ++ dataSourceTokens,
                      dataFrame = AppendLineage.appendLineageToDf(
                        dfm.tokens ++ dataSourceTokens,
                        SchemaMutations.rename(dfm.dataFrame)
                      )
                    ),
                    ingestId
                  )
                )
              }
            }
          }.get
        })
        .seq

      eventSender.foreach(
        _.sendSuccess(source, configName, ingestId, correlationId, snapshotDate, writeInfo)
      )

      writeInfo
    } catch {
      case e: Exception =>
        eventSender.foreach(_.sendFailure(source, configName, ingestId, correlationId))

        val loadException = new LoadException(
          loadDefinition = loadDefinition,
          subject = source,
          correlationId = correlationId,
          ingestId = ingestId,
          cause = e
        )

        throw loadException
    }
  }

  private val dateTime = DateTimeFormatter.ofPattern(Snapshot.SNAPSHOT_DATE_FORMAT_STRING)
  private val date     = DateTimeFormatter.ofPattern("yyyy_MM_dd")
  private val time     = DateTimeFormatter.ofPattern("HH_mm_ss")

  private lazy val properties =
    Try(new PropertiesReader("rawloader.properties").properties).getOrElse({
      val p = new Properties
      p.setProperty("project.artifactId", "LOCAL_TEST_BUILD")
      p.setProperty("project.version", "LOCAL_TEST_BUILD")
      p
    })

  def CreateRuntimeTokens(): Map[String, String] = {

    val justNow = LocalDateTime.now

    Map(
      "datetime"   -> justNow.format(dateTime),
      "date"       -> justNow.format(date),
      "time"       -> justNow.format(time),
      "artifactId" -> properties.getProperty("project.artifactId"),
      "version"    -> properties.getProperty("project.version")
    )
  }
}
