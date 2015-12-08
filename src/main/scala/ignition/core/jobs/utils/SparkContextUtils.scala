package ignition.core.jobs.utils

import java.io.InputStream

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectListing, S3ObjectSummary}
import ignition.core.utils.{AutoCloseableIterator, ByteUtils}
import ignition.core.utils.DateUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.{Partitioner, SparkContext}
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.{Codec, Source}
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.NonFatal
import ignition.core.utils.ExceptionUtils._


object SparkContextUtils {

  def close(inputStream: InputStream, path: String): Unit = {
    try {
      inputStream.close()
    } catch {
      case NonFatal(ex) =>
        println(s"Fail to close resource from '$path': ${ex.getMessage} -- ${ex.getFullStackTraceString}")
    }
  }

  case class BigFileSlice(index: Int)
  implicit lazy val amazonS3ClientFromEnvironmentVariables = new AmazonS3Client(new EnvironmentVariableCredentialsProvider())

  case class IndexedPartitioner(numPartitions: Int, index: Map[Any, Int]) extends Partitioner {
    override def getPartition(key: Any): Int = index(key)
  }

  case class HadoopFile(path: String, isDir: Boolean, size: Long)

  private case class HadoopFilePartition(size: Long, paths: Seq[String])

  implicit class SparkContextImprovements(sc: SparkContext) {

    lazy val _hadoopConf = sc.broadcast(sc.hadoopConfiguration.iterator().map { case entry => entry.getKey -> entry.getValue }.toMap)

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
    // For instance, given path = s3n://bucket/{a,b}, it will work fine if a exists but b is missing
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

    private def processParallelTextFiles(paths: Seq[String],
                                         minimumPaths: Int,
                                         maxBytesPerPartition: Long,
                                         minPartitions: Int,
                                         listOnWorkers: Boolean): RDD[String] = {
      val splittedPaths = paths.flatMap(ignition.core.utils.HadoopUtils.getPathStrings)
      if (splittedPaths.size < minimumPaths)
        throw new Exception(s"Not enough paths found for $paths")

      parallelListAndReadTextFiles(splittedPaths.toList, maxBytesPerPartition, minPartitions, listOnWorkers)
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
          val hdfsPath = p.replace("s3n://", hdfsPathPrefix)
          if (forceSynch || getStatus(hdfsPath, false).isEmpty || getStatus(s"$hdfsPath/*", true).filterNot(_.isDirectory).size != filesToOutput) {
            val _hdfsPath = new Path(hdfsPath)
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

    def getParallelTextFiles(paths: Seq[String],
                             maxBytesPerPartition: Long = 256 * 1000 * 1000,
                             minPartitions: Int = 100,
                             synchLocally: Boolean = false, forceSynch: Boolean = false, minimumPaths: Int = 1, listOnWorkers: Boolean = false): RDD[String] = {
      if (synchLocally)
        processParallelTextFiles(synchToHdfs(paths, processTextFiles, forceSynch), minimumPaths, maxBytesPerPartition, minPartitions, listOnWorkers)
      else
        processParallelTextFiles(paths, minimumPaths, maxBytesPerPartition, minPartitions, listOnWorkers)
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

    def filterAndGetParallelTextFiles(path: String,
                                      maxBytesPerPartition: Long = 256 * 1000 * 1000,
                                      minPartitions: Int = 100,
                                      requireSuccess: Boolean = false,
                                      inclusiveStartDate: Boolean = true,
                                      startDate: Option[DateTime] = None,
                                      inclusiveEndDate: Boolean = true,
                                      endDate: Option[DateTime] = None,
                                      lastN: Option[Int] = None,
                                      synchLocally: Boolean = false,
                                      forceSynch: Boolean = false,
                                      ignoreMalformedDates: Boolean = false,
                                      minimumPaths: Int = 1,
                                      listOnWorkers: Boolean = false)(implicit dateExtractor: PathDateExtractor): RDD[String] = {
      val paths = getFilteredPaths(Seq(path), requireSuccess, inclusiveStartDate, startDate, inclusiveEndDate, endDate, lastN, ignoreMalformedDates)
      if (paths.size < minimumPaths)
        throw new Exception(s"Tried with start/end time equals to $startDate/$endDate for path $path but but the resulting number of paths $paths is less than the required")
      getParallelTextFiles(paths, maxBytesPerPartition, minPartitions, synchLocally, forceSynch, minimumPaths, listOnWorkers)
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

    case class SizeBasedFileHandling(averageEstimatedCompressionRatio: Int = 8,
                                     compressedExtensions: Set[String] = Set(".gz")) {

      def isBig(f: HadoopFile, uncompressedBigSize: Long): Boolean = estimatedSize(f) >= uncompressedBigSize

      def estimatedSize(f: HadoopFile) = if (isCompressed(f))
        f.size * averageEstimatedCompressionRatio
      else
        f.size

      def isCompressed(f: HadoopFile): Boolean = compressedExtensions.exists(f.path.endsWith)
    }


    def readSmallFiles(smallFiles: List[HadoopFile],
                       maxBytesPerPartition: Long,
                       minPartitions: Int,
                       sizeBasedFileHandling: SizeBasedFileHandling): RDD[String] = {
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
            Source.fromInputStream(inputStream)(Codec.UTF8).getLines().foldLeft(ArrayBuffer.empty[String])(_ += _)
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

    def readCompressedBigFile(file: HadoopFile, maxBytesPerPartition: Long, minPartitions: Int,
                              sizeBasedFileHandling: SizeBasedFileHandling, sampleCount: Int = 100): RDD[String] = {
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
          val inputStream = Option(codecFactory.getCodec(hadoopPath)) match {
            case Some(compression) => compression.createInputStream(fileSystem.open(hadoopPath))
            case None => fileSystem.open(hadoopPath)
          }
          val lines = Source.fromInputStream(inputStream)(Codec.UTF8).getLines()

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
        }
      }
    }

    def readBigFiles(bigFiles: List[HadoopFile],
                     maxBytesPerPartition: Long,
                     minPartitions: Int,
                     sizeBasedFileHandling: SizeBasedFileHandling): RDD[String] = {
      def confWith(maxSplitSize: Long): Configuration = (_hadoopConf.value ++ Seq(
        "mapreduce.input.fileinputformat.split.maxsize" -> maxSplitSize.toString))
        .foldLeft(new Configuration()) { case (acc, (k, v)) => acc.set(k, v); acc }

      def read(file: HadoopFile, conf: Configuration) = sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](conf = conf, fClass = classOf[TextInputFormat],
        kClass = classOf[LongWritable], vClass = classOf[Text], path = file.path).map(pair => pair._2.toString)

      val confUncompressed = confWith(maxBytesPerPartition)

      val union = new UnionRDD(sc, bigFiles.map { file =>

        if (sizeBasedFileHandling.isCompressed(file))
          readCompressedBigFile(file, maxBytesPerPartition, minPartitions, sizeBasedFileHandling)
        else
          read(file, confUncompressed)
      })

      if (union.partitions.size < minPartitions)
        union.coalesce(minPartitions)
      else
        union
    }

    def parallelListAndReadTextFiles(paths: List[String],
                                     maxBytesPerPartition: Long,
                                     minPartitions: Int,
                                     listOnWorkers: Boolean,
                                     sizeBasedFileHandling: SizeBasedFileHandling = SizeBasedFileHandling()): RDD[String] = {
      val foundFiles = if (listOnWorkers) parallelListFiles(paths) else driverListFiles(paths)
      parallelReadTextFiles(foundFiles, maxBytesPerPartition, minPartitions, sizeBasedFileHandling)
    }

    def parallelReadTextFiles(files: List[HadoopFile],
                              maxBytesPerPartition: Long,
                              minPartitions: Int,
                              sizeBasedFileHandling: SizeBasedFileHandling = SizeBasedFileHandling()): RDD[String] = {
      val (bigFiles, smallFiles) = files.filter(_.size > 0).partition(f => sizeBasedFileHandling.isBig(f, maxBytesPerPartition))
      sc.union(
        readSmallFiles(smallFiles, maxBytesPerPartition, minPartitions, sizeBasedFileHandling),
        readBigFiles(bigFiles, maxBytesPerPartition, minPartitions, sizeBasedFileHandling))
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


    private def executeListOnWorkers(paths: RDD[String]): List[HadoopFile] = {
      val hadoopConf = _hadoopConf
      paths.flatMap { path =>
        val conf = hadoopConf.value.foldLeft(new Configuration()) { case (acc, (k, v)) => acc.set(k, v); acc }
        val hadoopPath = new Path(path)
        val fileSystem = hadoopPath.getFileSystem(conf)
        val tryFind = try {
          val status = fileSystem.getFileStatus(hadoopPath)
          if (status.isDirectory) {
            val sanitize = Option(fileSystem.listStatus(hadoopPath)).getOrElse(Array.empty)
            Option(sanitize.map(status => HadoopFile(status.getPath.toString, status.isDirectory, status.getLen)).toList)
          } else if (status.isFile) {
            Option(List(HadoopFile(status.getPath.toString, status.isDirectory, status.getLen)))
          } else {
            None
          }
        } catch {
          case e: java.io.FileNotFoundException =>
            None
        }

        tryFind.getOrElse {
          // Maybe is glob or not found
          val sanitize = Option(fileSystem.globStatus(hadoopPath)).getOrElse(Array.empty)
          sanitize.map(status => HadoopFile(status.getPath.toString, status.isDirectory, status.getLen)).toList
        }
      }.collect().toList
    }


    def parallelListFiles(paths: List[String]): List[HadoopFile] = {

      val directories = paths.map(HadoopFile(_, isDir = true, 0))

      def innerListFiles(remainingDirectories: List[HadoopFile]): List[HadoopFile] = {
        if (remainingDirectories.isEmpty) {
          Nil
        } else {
          val remainingPaths = remainingDirectories.map(_.path)
          val pathsRDD = sc.parallelize(remainingPaths, remainingPaths.size / 2)
          val (dirs, files) = executeListOnWorkers(pathsRDD).partition(_.isDir)
          files ++ innerListFiles(dirs)
        }
      }
      innerListFiles(directories)
    }


    private def executeDriverList(paths: Seq[String]): List[HadoopFile] = {
      val conf = _hadoopConf.value.foldLeft(new Configuration()) { case (acc, (k, v)) => acc.set(k, v); acc }
      paths.flatMap { path =>
        val hadoopPath = new Path(path)
        val fileSystem = hadoopPath.getFileSystem(conf)
        val tryFind = try {
          val status = fileSystem.getFileStatus(hadoopPath)
          if (status.isDirectory) {
            val sanitize = Option(fileSystem.listStatus(hadoopPath)).getOrElse(Array.empty)
            Option(sanitize.map(status => HadoopFile(status.getPath.toString, status.isDirectory, status.getLen)).toList)
          } else if (status.isFile) {
            Option(List(HadoopFile(status.getPath.toString, status.isDirectory, status.getLen)))
          } else {
            None
          }
        } catch {
          case e: java.io.FileNotFoundException =>
            None
        }

        tryFind.getOrElse {
          // Maybe is glob or not found
          val sanitize = Option(fileSystem.globStatus(hadoopPath)).getOrElse(Array.empty)
          sanitize.map(status => HadoopFile(status.getPath.toString, status.isDirectory, status.getLen)).toList
        }
      }.toList
    }

    def driverListFiles(paths: List[String]): List[HadoopFile] = {

      val directories = paths.map(HadoopFile(_, isDir = true, 0))

      def innerListFiles(remainingDirectories: List[HadoopFile]): List[HadoopFile] = {
        if (remainingDirectories.isEmpty) {
          Nil
        } else {
          val (dirs, files) = executeDriverList(remainingDirectories.map(_.path)).partition(_.isDir)
          files ++ innerListFiles(dirs)
        }
      }
      innerListFiles(directories)
    }

    private def s3List(bucket: String, prefix: String, predicate: S3ObjectSummary => Boolean = _ => true)
                      (implicit s3: AmazonS3Client): List[S3ObjectSummary] = {
      def inner(acc: mutable.ArrayBuffer[S3ObjectSummary], listing: ObjectListing): List[S3ObjectSummary] = {
        acc ++= listing.getObjectSummaries.toList.filter(predicate)
        if (listing.isTruncated)
          inner(acc, s3.listNextBatchOfObjects(listing))
        else
          acc.toList
      }

      inner(new mutable.ArrayBuffer[S3ObjectSummary], s3.listObjects(bucket, prefix))
    }

    def s3ListAndFilterFiles(bucket: String,
                             prefix: String,
                             start: Option[DateTime] = None,
                             end: Option[DateTime] = None,
                             endsWith: Option[String] = None,
                             exclusionPattern: Option[String] = Option("_$folder$"),
                             predicate: HadoopFile => Boolean = _ => true)
                            (implicit s3: AmazonS3Client, pathDateExtractor: PathDateExtractor): List[HadoopFile] = {

      def excludePatternValidation(s3Object: S3ObjectSummary, exclusionPatternOption: Option[String]): Option[S3ObjectSummary] =
        exclusionPatternOption match {
          case Some(pattern) if s3Object.getKey.contains(pattern) => None
          case Some(_) | None => Option(s3Object)
        }

      def endsWithValidation(s3Object: S3ObjectSummary, endsWithOption: Option[String]): Option[S3ObjectSummary] =
        endsWithOption match {
          case Some(pattern) if s3Object.getKey.endsWith(pattern) => Option(s3Object)
          case Some(_) => None
          case None => Option(s3Object)
        }

      def extractDateFromKey(s3Object: S3ObjectSummary): Option[DateTime] =
        Try(pathDateExtractor.extractFromPath(s"s3n://$bucket/${s3Object.getKey}")).toOption

      def startValidation(s3Object: S3ObjectSummary, extractedDate: DateTime, startOption: Option[DateTime]): Option[S3ObjectSummary] =
        startOption match {
          case Some(startDate) if startDate.isEqualOrBefore(extractedDate) => Option(s3Object)
          case Some(_) => None
          case None => Option(s3Object)
        }

      def endValidation(s3Object: S3ObjectSummary, extractedDate: DateTime, endOption: Option[DateTime]): Option[S3ObjectSummary] =
        endOption match {
          case Some(endDate) if endDate.isEqualOrAfter(extractedDate) => Option(s3Object)
          case Some(_) => None
          case None => Option(s3Object)
        }

      def applyPredicate(file: HadoopFile): Option[HadoopFile] =
        if (predicate(file))
          Option(file)
        else
          None

      def toHadoopFile(s3Object: S3ObjectSummary): HadoopFile =
        HadoopFile(s"s3n://${s3Object.getBucketName}/${s3Object.getKey}", isDir = false, s3Object.getSize)

      val allValidations: S3ObjectSummary => Boolean = s3Object => {
        val validatedFile = for {
          withValidPattern <- excludePatternValidation(s3Object, exclusionPattern)
          withValidEndsWith <- endsWithValidation(withValidPattern, endsWith)
          extractedDate <- extractDateFromKey(withValidEndsWith)
          withValidStart <- startValidation(withValidEndsWith, extractedDate, start)
          withValidEnd <- endValidation(withValidStart, extractedDate, end)
          hadoopFile = toHadoopFile(withValidEnd)
          valid <- applyPredicate(hadoopFile)
        } yield valid
        validatedFile.isDefined
      }

      s3List(bucket, prefix, allValidations)(s3).map(toHadoopFile)
    }


    def s3FilterAndGetParallelTextFiles(bucket: String,
                                        prefix: String,
                                        startDate: Option[DateTime] = None,
                                        endDate: Option[DateTime] = None,
                                        endsWith: Option[String] = None,
                                        predicate: HadoopFile => Boolean = _ => true,
                                        maxBytesPerPartition: Long = 256 * 1000 * 1000,
                                        minPartitions: Int = 100,
                                        sizeBasedFileHandling: SizeBasedFileHandling = SizeBasedFileHandling())
                                       (implicit  s3Client: AmazonS3Client = amazonS3ClientFromEnvironmentVariables,
                                        dateExtractor: PathDateExtractor): RDD[String] = {
      val foundFiles = s3ListAndFilterFiles(bucket, prefix, startDate, endDate, predicate = predicate)(s3Client, dateExtractor)
      parallelReadTextFiles(foundFiles, maxBytesPerPartition, minPartitions, sizeBasedFileHandling)
    }

  }
}
