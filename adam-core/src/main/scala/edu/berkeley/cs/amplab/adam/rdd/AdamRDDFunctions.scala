/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.rdd

import parquet.hadoop.metadata.CompressionCodecName
import org.apache.hadoop.mapreduce.Job
import parquet.hadoop.ParquetOutputFormat
import parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import parquet.hadoop.util.ContextUtil
import org.apache.avro.specific.SpecificRecord
import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, ADAMRecord}
import edu.berkeley.cs.amplab.adam.models.{SequenceRecord, SequenceDictionary, SingleReadBucket, ReferencePosition, ADAMRod}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import java.io.File

class AdamRDDFunctions[T <% SpecificRecord : Manifest](rdd: RDD[T]) extends Serializable {

  def adamSave(filePath: String, blockSize: Int = 128 * 1024 * 1024,
               pageSize: Int = 1 * 1024 * 1024, compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
               disableDictionaryEncoding: Boolean = false): RDD[T] = {
    val job = new Job(rdd.context.hadoopConfiguration)
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    ParquetOutputFormat.setCompression(job, compressCodec)
    ParquetOutputFormat.setEnableDictionary(job, !disableDictionaryEncoding)
    ParquetOutputFormat.setBlockSize(job, blockSize)
    ParquetOutputFormat.setPageSize(job, pageSize)
    AvroParquetOutputFormat.setSchema(job, manifest[T].erasure.asInstanceOf[Class[T]].newInstance().getSchema)
    // Add the Void Key
    val recordToSave = rdd.map(p => (null, p))
    // Save the values to the ADAM/Parquet file
    recordToSave.saveAsNewAPIHadoopFile(filePath,
      classOf[java.lang.Void], manifest[T].erasure.asInstanceOf[Class[T]], classOf[ParquetOutputFormat[T]],
      ContextUtil.getConfiguration(job))
    // Return the origin rdd
    rdd
  }

}

class AdamRecordRDDFunctions(rdd: RDD[ADAMRecord]) extends Serializable with Logging {
  initLogging()

  def adamSortReadsByReferencePosition(): RDD[ADAMRecord] = {
    log.info("Sorting reads by reference position")

    // NOTE: In order to keep unmapped reads from swamping a single partition
    // we place them in a range of referenceIds at the end of the file.
    // The referenceId is an Int and typical only a few dozen values are even used.
    // These referenceId values are not stored; they are only used during sorting.
    val unmappedReferenceIds = new Iterator[Int] with Serializable {
      var currentOffsetFromEnd = 0

      def hasNext: Boolean = true

      def next(): Int = {
        currentOffsetFromEnd += 1
        if (currentOffsetFromEnd > 10000) {
          currentOffsetFromEnd = 0
        }
        Int.MaxValue - currentOffsetFromEnd
      }
    }

    rdd.map(p => {
      val referencePos = ReferencePosition(p) match {
        case None =>
          // Move unmapped reads to the end of the file
          ReferencePosition(unmappedReferenceIds.next(), Long.MaxValue)
        case Some(pos) => pos
      }
      (referencePos, p)
    }).sortByKey().map(p => p._2)
  }

  def sequenceDictionary(): SequenceDictionary =
    rdd.distinct().aggregate(SequenceDictionary())(
      (dict: SequenceDictionary, rec: ADAMRecord) => dict ++ SequenceRecord.fromADAMRecord(rec),
      (dict1: SequenceDictionary, dict2: SequenceDictionary) => dict1 ++ dict2)

  def adamMarkDuplicates(): RDD[ADAMRecord] = {
    MarkDuplicates(rdd)
  }

  def adamBQSR(dbSNP: File): RDD[ADAMRecord] = {
    // Construct a map of chromosome to a set of SNP locations on that
    // chromosome.
    // The aggregate operation combines the sets of chromosome -> SNP locations.
    val dbsnpMap = scala.io.Source.fromFile(dbSNP).getLines().map(posLine => {
      val split = posLine.split("\t")
      val contig = split(0)
      val pos = split(1).toInt
      (contig, pos)
    }).aggregate(Map[String, Set[Int]]())((dbMap, pair) => {
      dbMap + (pair._1 -> (dbMap.getOrElse(pair._1, Set[Int]()) + pair._2))
    }, (map1 : Map[String, Set[Int]], map2 : Map[String, Set[Int]]) => {
      map1 ++ map2.map{ case (k,v) => k -> (v ++ map1.getOrElse(k, v.empty)) }
    })

    val broadcastDbSNP = rdd.context.broadcast(dbsnpMap)

    RecalibrateBaseQualities(rdd, broadcastDbSNP)
  }

  // Returns a tuple of (failedQualityMetrics, passedQualityMetrics)
  def adamFlagStat(): (FlagStatMetrics, FlagStatMetrics) = {
    FlagStat(rdd)
  }

  /**
   * Groups all reads by record group and read name
   * @return SingleReadBuckets with primary, secondary and unmapped reads
   */
  def adamSingleReadBuckets(): RDD[SingleReadBucket] = {
    SingleReadBucket(rdd)
  }

  /**
   * Groups all reads by reference position and returns a non-aggregated pileup RDD.
   * @return ADAMPileup without aggregation
   */
  def adamRecords2Pileup(): RDD[ADAMPileup] = {
    val helper = new Reads2PileupProcessor
    helper.process(rdd)
  }
}

class AdamPileupRDDFunctions(rdd: RDD[ADAMPileup]) extends Serializable with Logging {
  initLogging()

  /**
   * Aggregates pileup bases together.
   *
   * @param coverage Coverage value is used to increase number of reducer operators.
   * @return RDD with aggregated bases.
   *
   * @see AdamRodRDDFunctions#adamAggregateRods
   */
  def adamAggregatePileups(coverage: Int = 30): RDD[ADAMPileup] = {
    val helper = new PileupAggregator
    helper.aggregate(rdd, coverage)
  }

  /**
   * Converts ungrouped pileup bases into reference grouped bases.
   *
   * @param coverage Coverage value is used to increase number of reducer operators.
   * @return RDD with rods grouped by reference position.
   */
  def adamPileupsToRods(coverage: Int = 30): RDD[ADAMRod] = {
    val groups = rdd.groupBy((p: ADAMPileup) => p.getPosition, coverage)

    groups.map(kv => ADAMRod(kv._1, kv._2.toList))
  }

}

class AdamRodRDDFunctions(rdd: RDD[ADAMRod]) extends Serializable with Logging {
  initLogging()

  /**
   * Given an RDD of rods, splits the rods up by the specific sample they correspond to.
   * Returns a flat RDD.
   *
   * @return Rods split up by samples and _not_ grouped together.
   */
  def adamSplitRodsBySamples(): RDD[ADAMRod] = {
    rdd.flatMap(_.splitBySamples)
  }

  /**
   * Given an RDD of rods, splits the rods up by the specific sample they correspond to.
   * Returns an RDD where the samples are grouped by the reference position.
   *
   * @return Rods split up by samples and grouped together by position.
   */
  def adamDivideRodsBySamples(): RDD[(Long, List[ADAMRod])] = {
    rdd.keyBy(_.position).map(r => (r._1, r._2.splitBySamples))
  }

  /**
   * Inside of a rod, aggregates pileup bases together.
   *
   * @return RDD with aggregated rods.
   *
   * @see AdamPileupRDDFunctions#adamAggregatePileups
   */
  def adamAggregateRods(): RDD[ADAMRod] = {
    val helper = new PileupAggregator
    rdd.map(r => (r.position, r.pileups))
      .map(kv => (kv._1, helper.flatten(kv._2)))
      .map(kv => new ADAMRod(kv._1, kv._2))
  }

  /**
   * Returns the average coverage for all pileups.
   *
   * @note Coverage value does not include locus positions where no reads are mapped, as no rods exist for these positions.
   * @note If running on an RDD with multiple samples where the rods have been split by sample, will return the average
   *       coverage per sample, _averaged_ over all samples. If the RDD contains multiple samples and the rods have _not_ been split,
   *       this will return the average coverage per sample, _summed_ over all samples.
   *
   * @return Average coverage across mapped loci.
   */
  def adamRodCoverage(): Double = {
    val totalBases: Long = rdd.map(_.pileups.length.toLong).reduce(_ + _)

    // coverage is the total count of bases, over the total number of loci
    totalBases.toDouble / rdd.count.toDouble
  }
}
