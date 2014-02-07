/*
 * Copyright (c) 2013. Mount Sinai School of Medicine
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

package edu.berkeley.cs.amplab.adam.rdd.variation

import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.models.{SequenceDictionary, ADAMVariantContext}
import org.apache.spark.{Logging, SparkContext}
import edu.berkeley.cs.amplab.adam.converters.VariantContextConverter
import fi.tkk.ics.hadoop.bam._
import org.apache.hadoop.io.LongWritable
import edu.berkeley.cs.amplab.adam.avro.ADAMGenotype
import org.apache.spark.SparkContext._
import org.broadinstitute.variant.vcf.{VCFHeaderLine, VCFHeader}
import scala.collection.JavaConversions._


object ADAMVariationContext {
  implicit def sparkContextToADAMVariationContext(sc: SparkContext): ADAMVariationContext = new ADAMVariationContext(sc)

  implicit def rddToADAMVariantContextRDD(rdd: RDD[ADAMVariantContext]) = new ADAMVariantContextRDDFunctions(rdd)
  implicit def rddToADAMGenotypeRDD(rdd: RDD[ADAMGenotype]) = new ADAMGenotypeRDDFunctions(rdd)
}


private object ADAMVCFOutputFormat {
  private var header : Option[VCFHeader] = None

  def getHeader : VCFHeader = header match {
    case Some(h) => h
    case None => setHeader(Seq())
  }

  def setHeader(samples: Seq[String]) : VCFHeader = {
    header = Some(new VCFHeader(
      (VariantContextConverter.infoHeaderLines ++ VariantContextConverter.formatHeaderLines).toSet : Set[VCFHeaderLine],
      samples
    ))
    header.get
  }
}

/**
 * Wrapper for Hadoop-BAM to work around requirement for no-args constructor. Depends on
 * ADAMVCFOutputFormat object to maintain global state (such as samples)
 *
 * @tparam K
 */
private class ADAMVCFOutputFormat[K] extends KeyIgnoringVCFOutputFormat[K](VCFFormat.VCF) {
  setHeader(ADAMVCFOutputFormat.getHeader)
}



class ADAMVariationContext(sc: SparkContext) extends Serializable with Logging {
  import ADAMVariationContext._

  initLogging()

  /**
  * This method will create a new RDD of VariantContext objects
  * @param filePath: input VCF file to read
  * @return RDD of variants
  */
  def adamVCFLoad(filePath: String): RDD[ADAMVariantContext] = {
    log.info("Reading VCF file from %s".format(filePath))
    val vcc = new VariantContextConverter
    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      sc.hadoopConfiguration
    )
    log.info("Converted %d records".format(records.count))
    records.flatMap(p => vcc.convert(p._2.get))
  }

  def adamVCFSave(filePath: String, variants: RDD[ADAMVariantContext], dict: Option[SequenceDictionary] = None) = {
    val vcfFormat = VCFFormat.inferFromFilePath(filePath)
    assert(vcfFormat == VCFFormat.VCF, "BCF not yet supported") // TODO: Add BCF support

    log.info("Writing %s file to %s".format(vcfFormat, filePath))

    // Initialize global header object required by Hadoop VCF Writer
    ADAMVCFOutputFormat.setHeader(variants.sampleNames.collect)

    // TODO: Sort variants according to sequence dictionary (if supplied)
    val converter = new VariantContextConverter(dict)
    val gatkVCs: RDD[VariantContextWritable] = variants.map(v => {
      val vcw = new VariantContextWritable
      vcw.set(converter.convert(v))
      vcw
    })
    val withKey = gatkVCs.keyBy(v => new LongWritable(v.get.getStart))

    val conf = sc.hadoopConfiguration
    conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, vcfFormat.toString)
    withKey.saveAsNewAPIHadoopFile(filePath,
      classOf[LongWritable], classOf[VariantContextWritable], classOf[ADAMVCFOutputFormat[LongWritable]],
      conf
    )

    log.info("Write %d records".format(gatkVCs.count))
  }
}


