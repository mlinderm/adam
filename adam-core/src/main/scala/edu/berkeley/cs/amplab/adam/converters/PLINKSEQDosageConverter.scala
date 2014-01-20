/*
 * Copyright (c) 2014. Mount Sinai School of Medicine
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

package edu.berkeley.cs.amplab.adam.converters

import org.apache.spark.Logging
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext
import com.google.common.base.Splitter
import java.util.regex.Pattern
import edu.berkeley.cs.amplab.adam.avro._
import edu.berkeley.cs.amplab.adam.util.VCFStringUtils
import scala.collection.JavaConverters._

private[adam] class PLINKSEQDosageConverter(samples: Seq[String]) extends Serializable with Logging {
  initLogging()

  /**
   * Converts a row from PLINK/SEQ dosage input with genotype likelihoods,
   * as say produced by Impute2
   *
   * We expect each row to have 5 initial fields - ID CHROM POS REF ALT - followed
   * by three fields - genotype probabilities - for each sample.
   *
   * https://atgu.mgh.harvard.edu/plinkseq/dosage.shtml
   *
   * @param row
   * @return
   */
  def convert(row: String): ADAMVariantContext = {
    var tokens = Splitter.on(Pattern.compile("""\s""")).split(row).iterator

    val rsID = VCFStringUtils.rsIDtoInt(tokens.next)

    val variant = ADAMVariant.newBuilder()
      .setContig(ADAMContig.newBuilder().setContigName(tokens.next).build)
      .setPosition(tokens.next.toInt - 1)  // ADAM is 0-indexed
      .setReferenceAllele(tokens.next)
      .setVariantAllele(tokens.next)
      .build

    val db = ADAMDatabaseVariantAnnotation.newBuilder()
      .setVariant(variant)
      .setDbsnpId(rsID)
      .build


    val gts = for (sample <- samples) yield {
      // Probabilities for HOM_REF, HET, HOM_ALT
      val GL = Array(tokens.next.toDouble, tokens.next.toDouble, tokens.next.toDouble)
      var most = (0, GL(0))
      for (i <- 1 to 2) {
        if (GL(i) > most._2) most = (i, GL(i))
      }

      // "Hard-call" the genotype
      val alleles = most match {
        case (0, _) => List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Ref)
        case (1, _) => List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt)
        case (2, _) => List(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Alt)
      }

      // TODO convert the probabilities in PL equivalent

      ADAMGenotype.newBuilder()
        .setVariant(variant)
        .setSampleId(sample)
        .setAlleles(alleles.asJava)
        .build
    }

    ADAMVariantContext(variant, gts, databases = Some(db))
  }
}
