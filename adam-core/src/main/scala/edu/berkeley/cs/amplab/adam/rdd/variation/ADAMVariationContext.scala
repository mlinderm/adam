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
import org.broadinstitute.variant.vcf.VCFHeader


object ADAMVariationContext {
  implicit def sparkContextToADAMVariationContext(sc: SparkContext): ADAMVariationContext = new ADAMVariationContext(sc)

  implicit def rddToADAMVariantContextRDD(rdd: RDD[ADAMVariantContext]) = new ADAMVariantContextRDDFunctions(rdd)
  implicit def rddToADAMGenotypeRDD(rdd: RDD[ADAMGenotype]) = new ADAMGenotypeRDDFunctions(rdd)
}

private class ADAMVCFOutputFormat[K]() extends KeyIgnoringVCFOutputFormat[K](VCFFormat.VCF) {
  { // TODO: This needs to be broken out in a static object so that we can set the samples
    val hdr = new VCFHeader()
    VariantContextConverter.infoHeaderLines.foreach(hdr.addMetaDataLine(_))
    VariantContextConverter.formatHeaderLines.foreach(hdr.addMetaDataLine(_))
    setHeader(hdr)
  }
}

class ADAMVariationContext(sc: SparkContext) extends Serializable with Logging {
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
    assert(vcfFormat == VCFFormat.VCF, "BCF not yet supported")

    log.info("Writing %s file to %s".format(vcfFormat, filePath))

    val converter = new VariantContextConverter(dict)

    // TODO: Sort variants according to sequence dictionary (if supplied)
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


