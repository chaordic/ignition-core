package ignition.core.jobs.utils

import java.io.InputStream
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing, S3ObjectSummary}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import ignition.core.utils.CollectionUtils._
import ignition.core.utils.DateUtils._
import ignition.core.utils.ExceptionUtils._
import ignition.core.utils.{AutoCloseableIterator, ByteUtils, HadoopUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.{Partitioner, SparkContext}
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.{Codec, Source}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


object SparkContextUtils {
  private case class BigFileSlice(index: Int)

  private case class HadoopFilePartition(size: Long, paths: Seq[String])

  private case class IndexedPartitioner(numPartitions: Int, index: Map[Any, Int]) extends Partitioner {
    override def getPartition(key: Any): Int = index(key)
  }

  case class SizeBasedFileHandling(averageEstimatedCompressionRatio: Int = 8, compressedExtensions: Set[String] = Set(".gz")) {

    def isBig(f: HadoopFile, uncompressedBigSize: Long): Boolean = estimatedSize(f) >= uncompressedBigSize

    def estimatedSize(f: HadoopFile): Long = if (isCompressed(f))
      f.size * averageEstimatedCompressionRatio
    else
      f.size

    def isCompressed(f: HadoopFile): Boolean = compressedExtensions.exists(f.path.endsWith)
  }

  private lazy val amazonS3ClientFromEnvironmentVariables: AmazonS3 = new AmazonS3Client(new DefaultAWSCredentialsProviderChain())

  private def close(inputStream: InputStream, path: String): Unit = {
    try {
      inputStream.close()
    } catch {
      case NonFatal(ex) =>
        println(s"Fail to close resource from '$path': ${ex.getMessage} -- ${ex.getFullStackTraceString}")
    }
  }

  object S3SplittedPath {
    val s3Pattern = "s3[an]?://([^/]+)(.+)".r

    def from(fullPath: String): Option[S3SplittedPath] =
      fullPath match {
        case s3Pattern(bucket, prefix) => Option(S3SplittedPath(bucket, prefix.dropWhile(_ == '/')))
        case _ => None
      }
  }

  case class S3SplittedPath(bucket: String, key: String) {
    def join: String = s"s3a://$bucket/$key"
  }

  case class HadoopFile(path: String, isDir: Boolean, size: Long)

  case class WithOptDate[E](date: Option[DateTime], value: E)

  implicit class SparkContextImprovements(sc: SparkContext) {

    private lazy val logger = LoggerFactory.getLogger(getClass)

    lazy val _hadoopConf = sc.broadcast(sc.hadoopConfiguration.iterator().asScala.map { case entry => entry.getKey -> entry.getValue }.toMap)

    private def getFileSystem(path: Path): FileSystem = {
      path.getFileSystem(sc.hadoopConfiguration)
    }

    private def getStatus(commaSeparatedPaths: String, removeEmpty: Boolean): Seq[FileStatus] = {
      val paths = ignition.core.utils.HadoopUtils.getPathStrings(commaSeparatedPaths).map(new Path(_)).toSeq
      val fs = getFileSystem(paths.head)
      for {
        path <- paths
        status <- Option(fs.globStatus(path)).getOrElse(Array.empty).toSeq
        if !removeEmpty || status.getLen > 0 || status.isDirectory // remove empty files if necessary
      } yield status
    }

    private def delete(path: Path): Unit = {
      val fs = getFileSystem(path)
      fs.delete(path, true)
    }

    // This call is equivalent to a ls -d in shell, but won't fail if part of a path matches nothing,
    // For instance, given path = s3a://bucket/{a,b}, it will work fine if a exists but b is missing
    def sortedGlobPath(_paths: Seq[String], removeEmpty: Boolean = true): Seq[String] = {
      val paths = _paths.flatMap(path => ignition.core.utils.HadoopUtils.getPathStrings(path))
      paths.flatMap(p => getStatus(p, removeEmpty)).map(_.getPath.toString).distinct.sorted
    }

    // This function will expand the paths then group they and give to RDDs
    // We group to avoid too many RDDs on union (each RDD take some memory on driver)
    // We avoid passing a path too big to one RDD to avoid a Hadoop bug where just part of the path is processed when the path is big
    private def processPaths[T:ClassTag](f: (String) => RDD[T], paths: Seq[String], minimumPaths: Int): RDD[T] = {
      val splittedPaths = paths.flatMap(ignition.core.utils.HadoopUtils.getPathStrings)
      if (splittedPaths.size < minimumPaths)
        throw new Exception(s"Not enough paths found for $paths")

      val rdds = splittedPaths.grouped(5000).map(pathGroup => f(pathGroup.mkString(",")))

      new UnionRDD(sc, rdds.toList)
    }

    private def processTextFiles(paths: Seq[String], minimumPaths: Int): RDD[String] = {
      processPaths((p) => sc.textFile(p), paths, minimumPaths)
    }

    private def filterPaths(paths: Seq[String],
                            requireSuccess: Boolean,
                            inclusiveStartDate: Boolean,
                            startDate: Option[DateTime],
                            inclusiveEndDate: Boolean,
                            endDate: Option[DateTime],
                            lastN: Option[Int],
                            ignoreMalformedDates: Boolean)(implicit dateExtractor: PathDateExtractor): Seq[String] = {
      val sortedPaths = sortedGlobPath(paths)
      val filteredByDate = if (startDate.isEmpty && endDate.isEmpty)
        sortedPaths
      else
        sortedPaths.filter { p =>
          val tryDate = Try { dateExtractor.extractFromPath(p) }
          if (tryDate.isFailure && ignoreMalformedDates)
            false
          else {
            val date = tryDate.get
            val goodStartDate = startDate.isEmpty || (inclusiveStartDate && date.saneEqual(startDate.get) || date.isAfter(startDate.get))
            val goodEndDate = endDate.isEmpty || (inclusiveEndDate && date.saneEqual(endDate.get) || date.isBefore(endDate.get))
            goodStartDate && goodEndDate
          }
        }

      // Use a stream here to avoid checking the success if we are going to just take a few files
      val filteredBySuccessAndReversed = filteredByDate.reverse.toStream.dropWhile(p => requireSuccess && sortedGlobPath(Seq(s"$p/{_SUCCESS,_FINISHED}"), removeEmpty = false).isEmpty)

      if (lastN.isDefined)
        filteredBySuccessAndReversed.take(lastN.get).reverse.toList
      else
        filteredBySuccessAndReversed.reverse.toList
    }


    def getFilteredPaths(paths: Seq[String],
                         requireSuccess: Boolean,
                         inclusiveStartDate: Boolean,
                         startDate: Option[DateTime],
                         inclusiveEndDate: Boolean,
                         endDate: Option[DateTime],
                         lastN: Option[Int],
                         ignoreMalformedDates: Boolean)(implicit dateExtractor: PathDateExtractor): Seq[String] = {
      require(lastN.isEmpty || endDate.isDefined, "If you are going to get the last files, better specify the end date to avoid getting files in the future")
      filterPaths(paths, requireSuccess, inclusiveStartDate, startDate, inclusiveEndDate, endDate, lastN, ignoreMalformedDates)
    }

    lazy val hdfsPathPrefix = sc.master.replaceFirst("spark://(.*):7077", "hdfs://$1:9000/")

    def synchToHdfs(paths: Seq[String], pathsToRdd: (Seq[String], Int) => RDD[String], forceSynch: Boolean): Seq[String] = {
      val filesToOutput = 1500
      def mapPaths(actionWhenNeedsSynching: (String, String) => Unit): Seq[String] = {
        paths.map(p => {
          val hdfsPath = p.replaceFirst("s3[an]://", hdfsPathPrefix)
          if (forceSynch || getStatus(hdfsPath, false).isEmpty || getStatus(s"$hdfsPath/*", true).filterNot(_.isDirectory).size != filesToOutput) {
            actionWhenNeedsSynching(p, hdfsPath)
          }
          hdfsPath
        })
      }
      // We delete first because we may have two paths in the same parent
      mapPaths((p, hdfsPath) => delete(new Path(hdfsPath).getParent))// delete parent to avoid old files being accumulated
      // FIXME: We should be using a variable from the SparkContext, not a hard coded value (1500).
      mapPaths((p, hdfsPath) => pathsToRdd(Seq(p), 0).coalesce(filesToOutput, true).saveAsTextFile(hdfsPath))
    }


    @deprecated("It may incur heavy S3 costs and/or be slow with small files, use getParallelTextFiles instead", "2015-10-27")
    def getTextFiles(paths: Seq[String], synchLocally: Boolean = false, forceSynch: Boolean = false, minimumPaths: Int = 1): RDD[String] = {
      if (synchLocally)
        processTextFiles(synchToHdfs(paths, processTextFiles, forceSynch), minimumPaths)
      else
        processTextFiles(paths, minimumPaths)
    }

    @deprecated("It may incur heavy S3 costs and/or be slow with small files, use filterAndGetParallelTextFiles instead", "2015-10-27")
    def filterAndGetTextFiles(path: String,
                              requireSuccess: Boolean = false,
                              inclusiveStartDate: Boolean = true,
                              startDate: Option[DateTime] = None,
                              inclusiveEndDate: Boolean = true,
                              endDate: Option[DateTime] = None,
                              lastN: Option[Int] = None,
                              synchLocally: Boolean = false,
                              forceSynch: Boolean = false,
                              ignoreMalformedDates: Boolean = false,
                              minimumPaths: Int = 1)(implicit dateExtractor: PathDateExtractor): RDD[String] = {
      val paths = getFilteredPaths(Seq(path), requireSuccess, inclusiveStartDate, startDate, inclusiveEndDate, endDate, lastN, ignoreMalformedDates)
      if (paths.size < minimumPaths)
        throw new Exception(s"Tried with start/end time equals to $startDate/$endDate for path $path but but the resulting number of paths $paths is less than the required")
      getTextFiles(paths, synchLocally, forceSynch, minimumPaths)
    }

    private def stringHadoopFile(paths: Seq[String], minimumPaths: Int): RDD[Try[String]] = {
      processPaths((p) => sc.sequenceFile(p, classOf[LongWritable], classOf[org.apache.hadoop.io.BytesWritable])
                .map({ case (k, v) => Try { ByteUtils.toString(v.getBytes, 0, v.getLength, "UTF-8") } }), paths, minimumPaths)
    }

    def filterAndGetStringHadoopFiles(path: String,
                                      requireSuccess: Boolean = false,
                                      inclusiveStartDate: Boolean = true,
                                      startDate: Option[DateTime] = None,
                                      inclusiveEndDate: Boolean = true,
                                      endDate: Option[DateTime] = None,
                                      lastN: Option[Int] = None,
                                      ignoreMalformedDates: Boolean = false,
                                      minimumPaths: Int = 1)(implicit dateExtractor: PathDateExtractor): RDD[Try[String]] = {
      val paths = getFilteredPaths(Seq(path), requireSuccess, inclusiveStartDate, startDate, inclusiveEndDate, endDate, lastN, ignoreMalformedDates)
      if (paths.size < minimumPaths)
        throw new Exception(s"Tried with start/end time equals to $startDate/$endDate for path $path but but the resulting number of paths $paths is less than the required")
      else
        stringHadoopFile(paths, minimumPaths)
    }

    private def objectHadoopFile[T:ClassTag](paths: Seq[String], minimumPaths: Int): RDD[T] = {
      processPaths(sc.objectFile[T](_), paths, minimumPaths)
    }

    def filterAndGetObjectHadoopFiles[T:ClassTag](path: String,
                                                  requireSuccess: Boolean = false,
                                                  inclusiveStartDate: Boolean = true,
                                                  startDate: Option[DateTime] = None,
                                                  inclusiveEndDate: Boolean = true,
                                                  endDate: Option[DateTime] = None,
                                                  lastN: Option[Int] = None,
                                                  ignoreMalformedDates: Boolean = false,
                                                  minimumPaths: Int = 1)(implicit dateExtractor: PathDateExtractor): RDD[T] = {
      val paths = getFilteredPaths(Seq(path), requireSuccess, inclusiveStartDate, startDate, inclusiveEndDate, endDate, lastN, ignoreMalformedDates)
      if (paths.size < minimumPaths)
        throw new Exception(s"Tried with start/end time equals to $startDate/$endDate for path $path but but the resulting number of paths $paths is less than the required")
      else
        objectHadoopFile(paths, minimumPaths)
    }

    private def readSmallFiles(smallFiles: List[HadoopFile],
                               maxBytesPerPartition: Long,
                               minPartitions: Int,
                               sizeBasedFileHandling: SizeBasedFileHandling,
                               innerFilter: Set[String] = Set.empty): RDD[String] = {
      val smallPartitionedFiles = sc.parallelize(smallFiles.map(_.path).map(file => file -> null), 2).partitionBy(createSmallFilesPartitioner(smallFiles, maxBytesPerPartition, minPartitions, sizeBasedFileHandling))
      val hadoopConf = _hadoopConf
      smallPartitionedFiles.mapPartitions { files =>
        val conf = hadoopConf.value.foldLeft(new Configuration()) { case (acc, (k, v)) => acc.set(k, v); acc }
        val codecFactory = new CompressionCodecFactory(conf)
        files.map { case (path, _) => path } flatMap { path =>
          val hadoopPath = new Path(path)
          val fileSystem = hadoopPath.getFileSystem(conf)
          val inputStream = Option(codecFactory.getCodec(hadoopPath)) match {
            case Some(compression) => compression.createInputStream(fileSystem.open(hadoopPath))
            case None => fileSystem.open(hadoopPath)
          }
          try {
            Source.fromInputStream(inputStream)(Codec.UTF8).getLines().filter(line => innerFilter.isEmpty || innerFilter.exists(line.contains))
              .foldLeft(ArrayBuffer.empty[String])(_ += _)

          } catch {
            case NonFatal(ex) =>
              println(s"Failed to read resource from '$path': ${ex.getMessage} -- ${ex.getFullStackTraceString}")
              throw new Exception(s"Failed to read resource from '$path': ${ex.getMessage} -- ${ex.getFullStackTraceString}")
          } finally {
            close(inputStream, path)
          }
        }
      }
    }

    private def readCompressedBigFile(file: HadoopFile,
                                      maxBytesPerPartition: Long, minPartitions: Int,
                                      sizeBasedFileHandling: SizeBasedFileHandling,
                                      sampleCount: Int = 100,
                                      innerFilter: Set[String] = Set.empty): RDD[String] = {
      val estimatedSize = sizeBasedFileHandling.estimatedSize(file)
      val totalSlices = (estimatedSize / maxBytesPerPartition + 1).toInt
      val slices = (0 until totalSlices).map(BigFileSlice.apply)

      val partitioner = {
        val indexedPartitions: Map[Any, Int] = slices.map(s => s -> s.index).toMap
        IndexedPartitioner(totalSlices, indexedPartitions)
      }
      val hadoopConf = _hadoopConf

      val partitionedSlices = sc.parallelize(slices.map(s => s -> null), 2).partitionBy(partitioner)
      partitionedSlices.mapPartitions { slices =>
        val conf = hadoopConf.value.foldLeft(new Configuration()) { case (acc, (k, v)) => acc.set(k, v); acc }
        val codecFactory = new CompressionCodecFactory(conf)
        val hadoopPath = new Path(file.path)
        val fileSystem = hadoopPath.getFileSystem(conf)
        slices.flatMap { case (slice, _) =>
          try {
            val inputStream = Option(codecFactory.getCodec(hadoopPath)) match {
              case Some(compression) => compression.createInputStream(fileSystem.open(hadoopPath))
              case None => fileSystem.open(hadoopPath)
            }

            val lines = Source.fromInputStream(inputStream)(Codec.UTF8).getLines().filter(line => innerFilter.isEmpty || innerFilter.exists(line.contains))
              .foldLeft(ArrayBuffer.empty[String])(_ += _)

            val lineSample = lines.take(sampleCount).toList
            val linesPerSlice = {
              val sampleSize = lineSample.map(_.size).sum
              val estimatedAverageLineSize = Math.round(sampleSize / sampleCount.toFloat)
              val estimatedTotalLines = Math.round(estimatedSize / estimatedAverageLineSize.toFloat)
              estimatedTotalLines / totalSlices + 1
            }

            val linesAfterSeek = (lineSample.toIterator ++ lines).drop(linesPerSlice * slice.index)

            val finalLines = if (slice.index + 1 == totalSlices) // last slice, read until the end
              linesAfterSeek
            else
              linesAfterSeek.take(linesPerSlice)

            AutoCloseableIterator.wrap(finalLines, () => close(inputStream, s"${file.path}, slice $slice"))
          } catch {
            case NonFatal(e) =>
              throw new Exception(s"Error on read compressed big file, slice=$slice, file=$file", e)
          }
        }
      }
    }

    private def readBigFiles(bigFiles: List[HadoopFile],
                             maxBytesPerPartition: Long,
                             minPartitions: Int,
                             sizeBasedFileHandling: SizeBasedFileHandling,
                             innerFilter: Set[String] = Set.empty): RDD[String] = {
      def confWith(maxSplitSize: Long): Configuration = (_hadoopConf.value ++ Seq(
        "mapreduce.input.fileinputformat.split.minsize" -> maxSplitSize.toString,
        "mapreduce.input.fileinputformat.split.maxsize" -> maxSplitSize.toString))
        .foldLeft(new Configuration()) { case (acc, (k, v)) => acc.set(k, v); acc }

      def read(file: HadoopFile, conf: Configuration) = sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](conf = conf, fClass = classOf[TextInputFormat],
        kClass = classOf[LongWritable], vClass = classOf[Text], path = file.path).map(pair => pair._2.toString)

      val confUncompressed = confWith(maxBytesPerPartition)

      val union = new UnionRDD(sc, bigFiles.map { file =>

        if (sizeBasedFileHandling.isCompressed(file))
          readCompressedBigFile(file, maxBytesPerPartition, minPartitions, sizeBasedFileHandling, innerFilter = innerFilter)
        else
          read(file, confUncompressed)
      })

      if (union.partitions.size < minPartitions)
        union.coalesce(minPartitions)
      else
        union
    }

    def parallelReadTextFiles(files: List[HadoopFile],
                              maxBytesPerPartition: Long = 128 * 1000 * 1000,
                              minPartitions: Int = 100,
                              sizeBasedFileHandling: SizeBasedFileHandling = SizeBasedFileHandling(),
                              synchLocally: Option[String] = None,
                              forceSynch: Boolean = false,
                              innerFilter: Set[String] = Set.empty): RDD[String] = {
      val filteredFiles = files.filter(_.size > 0)

      if (synchLocally.isDefined)
        doSync(filteredFiles, maxBytesPerPartition = maxBytesPerPartition, minPartitions = minPartitions, synchLocally = synchLocally.get,
          sizeBasedFileHandling = sizeBasedFileHandling, forceSynch = forceSynch)
      else {
        val (bigFiles, smallFiles) = filteredFiles.partition(f => sizeBasedFileHandling.isBig(f, maxBytesPerPartition))
        sc.union(
          readSmallFiles(smallFiles, maxBytesPerPartition, minPartitions, sizeBasedFileHandling, innerFilter),
          readBigFiles(bigFiles, maxBytesPerPartition, minPartitions, sizeBasedFileHandling, innerFilter))
      }
    }

    private def createSmallFilesPartitioner(files: List[HadoopFile], maxBytesPerPartition: Long, minPartitions: Long, sizeBasedFileHandling: SizeBasedFileHandling): Partitioner = {
      implicit val ordering: Ordering[HadoopFilePartition] = Ordering.by(p => -p.size) // Small partitions come first (highest priority)

      val pq: mutable.PriorityQueue[HadoopFilePartition] = mutable.PriorityQueue.empty

      (0L until minPartitions).foreach(_ => pq += HadoopFilePartition(0, Seq.empty))

      val partitions = files.foldLeft(pq) {
        case (acc, file) =>
          val fileSize = sizeBasedFileHandling.estimatedSize(file)

          acc.headOption.filter(bucket => bucket.size + fileSize < maxBytesPerPartition) match {
            case Some(found) =>
              val updated = found.copy(size = found.size + fileSize, paths = file.path +: found.paths)
              acc.tail += updated
            case None => acc += HadoopFilePartition(fileSize, Seq(file.path))
          }
      }.filter(_.paths.nonEmpty).toList // Remove empty partitions

      val indexedPartitions: Map[Any, Int] = partitions.zipWithIndex.flatMap {
        case (bucket, index) => bucket.paths.map(path => path -> index)
      }.toMap

      IndexedPartitioner(partitions.size, indexedPartitions)
    }

    def isGoodDate(date: DateTime, startDate: DateTime, endDate: DateTime): Boolean = {
      val startDateToCompare =  startDate.withTimeAtStartOfDay()
      val endDateToCompare = endDate.withTime(23, 59, 59, 999)
      val goodStartDate = date.saneEqual(startDateToCompare) || date.isAfter(startDateToCompare)
      val goodEndDate = date.saneEqual(endDateToCompare) || date.isBefore(endDateToCompare)

      goodStartDate && goodEndDate
    }

    private def executeDriverList(paths: Seq[String], startDate: Option[DateTime], endDate: Option[DateTime], apiKeysToProcess: Set[String] = Set.empty)(implicit pathDateExtractor: PathDateExtractor): List[HadoopFile] = {
      val conf = _hadoopConf.value.foldLeft(new Configuration()) { case (acc, (k, v)) => acc.set(k, v); acc }

      paths.flatMap { path =>
        val hadoopPath = new Path(path)
        val fileSystem = hadoopPath.getFileSystem(conf)
        val tryFind = try {
          val status = fileSystem.getFileStatus(hadoopPath)

          if (status.isDirectory) {
            var shouldList = true

            val folderPath = status.getPath.toString

            val dateTime = Try { pathDateExtractor.extractFromPath(folderPath) }.toOption

            if (startDate.isDefined && endDate.isDefined && dateTime.isDefined) {
              shouldList = isGoodDate(dateTime.get, startDate.get, endDate.get)
            } else {
              logger.info(s"No date found for path ${folderPath}")
            }

            if (shouldList) {
              val sanitize = fileSystem.listStatus(hadoopPath)
              val sanitized = sanitize.map(status => HadoopFile(status.getPath.toString, status.isDirectory, status.getLen)).toList

              if (apiKeysToProcess.exists(apiKey => sanitized.exists(hadoopFile => hadoopFile.path.contains(apiKey)))) {
                val filtered = sanitized.filter(file => file.path.endsWith("_SUCCESS") || file.path.endsWith("_FINISHED") || apiKeysToProcess.exists(file.path.contains))

                // TODO: Remover após refatoração
                filtered.filter(file => file.isDir).foreach(file => logger.info(s"Adding ${file.path} to dirs list"))

                Option(filtered)
              } else {
                Option(sanitized)
              }
            } else {
              None
            }
          } else if (status.isFile) {
            Option(List(HadoopFile(status.getPath.toString, status.isDirectory, status.getLen)))
          } else {
            None
          }
        } catch {
          case e: java.io.FileNotFoundException =>
            logger.info(s"FileNotFoundException ${path}")
            None
        }

        tryFind.getOrElse(List.empty)
      }.toList
    }

    private def driverListFiles(path: String, startDate: Option[DateTime], endDate: Option[DateTime], apiKeysToProcess: Set[String] = Set.empty)(implicit pathDateExtractor: PathDateExtractor): List[HadoopFile] = {
      def innerListFiles(remainingDirectories: List[HadoopFile]): List[HadoopFile] = {
        if (remainingDirectories.isEmpty) {
          Nil
        } else {
          val (dirs, files) = executeDriverList(remainingDirectories.map(_.path), startDate, endDate, apiKeysToProcess).partition(_.isDir)
          files ++ innerListFiles(dirs)
        }
      }

      innerListFiles(List(HadoopFile(path, isDir = true, 0)))
    }

    def s3ListCommonPrefixes(path: S3SplittedPath, delimiter: String = "/")
                            (implicit s3: AmazonS3): Stream[S3SplittedPath] = {
      def inner(current: ObjectListing): Stream[String] =
        if (current.isTruncated) {
          logger.trace(s"list common prefixed truncated for ${path.bucket} ${path.key}: ${current.getCommonPrefixes}")
          current.getCommonPrefixes.asScala.toStream ++ inner(s3.listNextBatchOfObjects(current))
        } else {
          logger.trace(s"list common prefixed finished for ${path.bucket} ${path.key}: ${current.getCommonPrefixes}")
          current.getCommonPrefixes.asScala.toStream
        }

      val request = new ListObjectsRequest(path.bucket, path.key, null, delimiter, 1000)
      inner(s3.listObjects(request)).map(prefix => path.copy(key = prefix))
    }

    def s3ListObjects(path: S3SplittedPath)
                     (implicit s3: AmazonS3): Stream[S3ObjectSummary] = {
      def inner(current: ObjectListing): Stream[S3ObjectSummary] =
        if (current.isTruncated) {
          logger.trace(s"list objects truncated for ${path.bucket} ${path.key}: $current")
          current.getObjectSummaries.asScala.toStream ++ inner(s3.listNextBatchOfObjects(current))
        } else {
          logger.trace(s"list objects finished for ${path.bucket} ${path.key}")
          current.getObjectSummaries.asScala.toStream
        }

      inner(s3.listObjects(path.bucket, path.key))
    }

    def s3NarrowPaths(splittedPath: S3SplittedPath,
                      inclusiveStartDate: Boolean = true,
                      startDate: Option[DateTime] = None,
                      inclusiveEndDate: Boolean = true,
                      endDate: Option[DateTime] = None,
                      ignoreHours: Boolean = true)
                     (implicit s3: AmazonS3, pathDateExtractor: PathDateExtractor): Stream[WithOptDate[S3SplittedPath]] = {

      def isGoodDate(date: DateTime): Boolean = {
        val startDateToCompare =  startDate.map(date => if (ignoreHours) date.withTimeAtStartOfDay() else date)
        val endDateToCompare = endDate.map(date => if (ignoreHours) date.withTime(23, 59, 59, 999) else date)
        val goodStartDate = startDateToCompare.isEmpty || (inclusiveStartDate && date.saneEqual(startDateToCompare.get) || date.isAfter(startDateToCompare.get))
        val goodEndDate = endDateToCompare.isEmpty || (inclusiveEndDate && date.saneEqual(endDateToCompare.get) || date.isBefore(endDateToCompare.get))
        goodStartDate && goodEndDate
      }

      def classifyPath(path: S3SplittedPath): Either[S3SplittedPath, (S3SplittedPath, DateTime)] =
        Try(pathDateExtractor.extractFromPath(path.join)) match {
          case Success(date) => Right(path -> date)
          case Failure(_) => Left(path)
        }

      val commonPrefixes = s3ListCommonPrefixes(splittedPath).map(classifyPath)

      logger.trace(s"s3NarrowPaths for $splittedPath, common prefixes: $commonPrefixes")

      if (commonPrefixes.isEmpty)
        Stream(WithOptDate(Try(pathDateExtractor.extractFromPath(splittedPath.join)).toOption, splittedPath))
      else
        commonPrefixes.toStream.flatMap {
          case Left(prefixWithoutDate) =>
            logger.trace(s"s3NarrowPaths prefixWithoutDate: $prefixWithoutDate")

            s3NarrowPaths(prefixWithoutDate, inclusiveStartDate, startDate, inclusiveEndDate, endDate, ignoreHours)
          case Right((prefixWithDate, date)) if isGoodDate(date) => Stream(WithOptDate(Option(date), prefixWithDate))
          case Right(_) => Stream.empty
        }
    }

    // Sorted from most recent to least recent path
    private def sortPaths[P](paths: Stream[WithOptDate[P]]): Stream[WithOptDate[P]] = {
      paths.sortBy { p => p.date.getOrElse(new DateTime(1970, 1, 1, 1, 1)) }(Ordering[DateTime].reverse)
    }

    private def sortedS3List(path: String,
                             inclusiveStartDate: Boolean,
                             startDate: Option[DateTime],
                             inclusiveEndDate: Boolean,
                             endDate: Option[DateTime],
                             exclusionPattern: Option[String])
                            (implicit s3: AmazonS3, dateExtractor: PathDateExtractor): Stream[WithOptDate[Array[S3ObjectSummary]]] = {


      S3SplittedPath.from(path) match {
        case Some(splittedPath) =>
          val prefixes: Stream[WithOptDate[S3SplittedPath]] =
            s3NarrowPaths(splittedPath, inclusiveStartDate = inclusiveStartDate, inclusiveEndDate = inclusiveEndDate,
              startDate = startDate, endDate = endDate)

          sortPaths(prefixes)
            .map { case WithOptDate(date, path) => WithOptDate(date, s3ListObjects(path).toArray) } // Will list the most recent path first and only if needed the others
        case _ => Stream.empty
      }
    }


    def listAndFilterFiles(path: String,
                           requireSuccess: Boolean = false,
                           inclusiveStartDate: Boolean = true,
                           startDate: Option[DateTime] = None,
                           inclusiveEndDate: Boolean = true,
                           endDate: Option[DateTime] = None,
                           lastN: Option[Int] = None,
                           ignoreMalformedDates: Boolean = false,
                           endsWith: Option[String] = None,
                           exclusionPattern: Option[String] = Option(".*_temporary.*|.*_\\$folder.*"),
                           predicate: HadoopFile => Boolean = _ => true,
                           apiKeysToProcess: Set[String] = Set.empty)
                          (implicit dateExtractor: PathDateExtractor): List[HadoopFile] = {
      def isSuccessFile(file: HadoopFile): Boolean = {
        file.path.endsWith("_SUCCESS") || file.path.endsWith("_FINISHED")
      }

      def excludePatternValidation(file: HadoopFile): Boolean = {
        exclusionPattern.map(pattern => !file.path.matches(pattern)).getOrElse(true)
      }

      def endsWithValidation(file: HadoopFile): Boolean = {
        endsWith.map { pattern =>
          file.path.endsWith(pattern) || isSuccessFile(file)
        }.getOrElse(true)
      }

      def dateValidation(files: WithOptDate[Array[HadoopFile]]): Boolean = {
        val tryDate = files.date

        if (tryDate.isEmpty && ignoreMalformedDates)
          true
        else if (tryDate.isEmpty)
          throw new Exception(s"Not date found for path $path, expanded files: ${files.value.toList.head}, consider using ignoreMalformedDates=true if not date is expected on this path")
        else {
          val date = tryDate.get
          val goodStartDate = startDate.isEmpty || (inclusiveStartDate && date.saneEqual(startDate.get) || date.isAfter(startDate.get))
          def goodEndDate = endDate.isEmpty || (inclusiveEndDate && date.saneEqual(endDate.get) || date.isBefore(endDate.get))
          goodStartDate && goodEndDate
        }
      }

      def successFileValidation(files: WithOptDate[Array[HadoopFile]]): Boolean = {
        if (requireSuccess)
          files.value.exists(isSuccessFile)
        else
          true
      }

      def preValidations(files: WithOptDate[Array[HadoopFile]]): Option[WithOptDate[Array[HadoopFile]]] = {
        if (!successFileValidation(files))
          None
        else {
          val filtered = files.copy(value = files.value
            .filter(excludePatternValidation).filter(endsWithValidation).filter(predicate))

          if (filtered.value.isEmpty || !dateValidation(filtered))
            None
          else
            Option(filtered)
        }
      }

      val groupedAndSortedByDateFiles = sortedSmartList(path, inclusiveStartDate = inclusiveStartDate, inclusiveEndDate = inclusiveEndDate,
        startDate = startDate, endDate = endDate, exclusionPattern = exclusionPattern, apiKeysToProcess = apiKeysToProcess).flatMap(preValidations)

      val allFiles = if (lastN.isDefined) {
        groupedAndSortedByDateFiles.take(lastN.get).flatMap(_.value)
      } else {
        groupedAndSortedByDateFiles.flatMap(_.value)
      }

      allFiles.sortBy(_.path).toList
    }

    def sortedSmartList(path: String,
                        inclusiveStartDate: Boolean = false,
                        startDate: Option[DateTime] = None,
                        inclusiveEndDate: Boolean = false,
                        endDate: Option[DateTime] = None,
                        exclusionPattern: Option[String] = None,
                        apiKeysToProcess: Set[String] = Set.empty)(implicit pathDateExtractor: PathDateExtractor): Stream[WithOptDate[Array[HadoopFile]]] = {
      def toHadoopFile(s3Object: S3ObjectSummary): HadoopFile =
        HadoopFile(s"s3a://${s3Object.getBucketName}/${s3Object.getKey}", isDir = false, s3Object.getSize)

      def listPath(path: String): Stream[WithOptDate[Array[HadoopFile]]] = {
        if (path.startsWith("s3")) {
          sortedS3List(path, inclusiveStartDate = inclusiveStartDate, startDate = startDate, inclusiveEndDate = inclusiveEndDate,
            endDate = endDate, exclusionPattern = exclusionPattern)(amazonS3ClientFromEnvironmentVariables, pathDateExtractor).map {
            case WithOptDate(date, paths) => WithOptDate(date, paths.map(toHadoopFile).toArray)
          }
        } else {
          val pathsWithDate: Stream[WithOptDate[Iterable[HadoopFile]]] = driverListFiles(path, startDate, endDate, apiKeysToProcess)
            .map(p => (Try { pathDateExtractor.extractFromPath(p.path) }.toOption, p))
            .groupByKey()
            .map { case (date, path) => WithOptDate(date, path) }
            .toStream
          sortPaths(pathsWithDate).map { case WithOptDate(date, paths) => WithOptDate(date, paths.toArray) }
        }
      }

      HadoopUtils.getPathStrings(path).toStream.flatMap(listPath)
    }

    def filterAndGetParallelTextFiles(path: String,
                                      requireSuccess: Boolean = false,
                                      inclusiveStartDate: Boolean = true,
                                      startDate: Option[DateTime] = None,
                                      inclusiveEndDate: Boolean = true,
                                      endDate: Option[DateTime] = None,
                                      lastN: Option[Int] = None,
                                      ignoreMalformedDates: Boolean = false,
                                      endsWith: Option[String] = None,
                                      predicate: HadoopFile => Boolean = _ => true,
                                      maxBytesPerPartition: Long = 128 * 1000 * 1000,
                                      minPartitions: Int = 100,
                                      sizeBasedFileHandling: SizeBasedFileHandling = SizeBasedFileHandling(),
                                      minimumFiles: Int = 1,
                                      synchLocally: Option[String] = None,
                                      forceSynch: Boolean = false,
                                      apiKeysToProcess: Set[String] = Set.empty,
                                      innerFilter: Set[String] = Set.empty)
                                     (implicit dateExtractor: PathDateExtractor): RDD[String] = {

      val foundFiles = listAndFilterFiles(path, requireSuccess, inclusiveStartDate, startDate, inclusiveEndDate,
        endDate, lastN, ignoreMalformedDates, endsWith, predicate = predicate, apiKeysToProcess = apiKeysToProcess)

      if (foundFiles.size < minimumFiles)
        throw new Exception(s"Tried with start/end time equals to $startDate/$endDate for path $path but the resulting number of files $foundFiles is less than the required")

      parallelReadTextFiles(foundFiles, maxBytesPerPartition = maxBytesPerPartition, minPartitions = minPartitions,
        sizeBasedFileHandling = sizeBasedFileHandling, synchLocally = synchLocally, forceSynch = forceSynch, innerFilter = innerFilter)
    }

    private def doSync(hadoopFiles: List[HadoopFile],
                       synchLocally: String,
                       forceSynch: Boolean,
                       maxBytesPerPartition: Long,
                       minPartitions: Int,
                       sizeBasedFileHandling: SizeBasedFileHandling): RDD[String] = {
      require(!synchLocally.contains("*"), "Globs are not supported on the sync key")

      def syncPath(suffix: String) = s"$hdfsPathPrefix/_core_ignition_sync_hdfs_cache/$suffix"

      val hashKey = Integer.toHexString(hadoopFiles.toSet.hashCode())

      lazy val foundLocalPaths = getStatus(syncPath(s"$synchLocally/$hashKey/{_SUCCESS,_FINISHED}"), removeEmpty = false)

      val cacheKey = syncPath(s"$synchLocally/$hashKey")

      if (forceSynch || foundLocalPaths.isEmpty) {
        delete(new Path(syncPath(s"$synchLocally/")))
        val data = parallelReadTextFiles(hadoopFiles, maxBytesPerPartition, minPartitions, sizeBasedFileHandling = sizeBasedFileHandling,  synchLocally = None)
        data.saveAsTextFile(cacheKey)
      }

      sc.textFile(cacheKey)
    }
  }
}
