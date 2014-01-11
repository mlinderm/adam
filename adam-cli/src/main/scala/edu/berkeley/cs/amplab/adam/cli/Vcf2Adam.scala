/*
 * Copyright (c) 2013. The Broad Institute of MIT/Harvard
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

package edu.berkeley.cs.amplab.adam.cli

import org.apache.spark.{Logging, SparkContext}
import org.kohsuke.args4j.Argument
import org.apache.hadoop.mapreduce.Job
import org.broadinstitute.variant.vcf.VCFFileReader;
import org.broadinstitute.variant.vcf.VCFHeader;

object Vcf2Adam extends AdamCommandCompanion {

  val commandName = "vcf2adam"
  val commandDescription = "Convert a VCF file to the corresponding ADAM format"

  def apply(cmdLine: Array[String]) = {
    new Vcf2Adam(Args4j[Vcf2AdamArgs](cmdLine))
  }
}

class Vcf2AdamArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "VCF", usage = "The VCF file to convert", index = 0)
  var vcfFile: String = _
  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM Variant data", index = 1)
  var outputPath: String = null
}

class Vcf2Adam(val args: Vcf2AdamArgs) extends AdamSparkCommand[Vcf2AdamArgs] with Logging {
  val companion = Vcf2Adam

  def run(sc: SparkContext, job: Job) {
    val reader = new VCFFileReader(args.vcfFile, false)
    val header = new VCFHeader(reader.getHeader())
    List<VCFContigHeaderLine> header_contigs = header.getContigLines()
    val it = header_contigs.iterator
    while (it.hasNext()) {
      println(it.next().toString())
    }
  }
}
