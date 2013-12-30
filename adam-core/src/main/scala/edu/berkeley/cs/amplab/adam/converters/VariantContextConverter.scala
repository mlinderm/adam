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
package edu.berkeley.cs.amplab.adam.converters

import org.broadinstitute.variant.variantcontext.VariantContext
import edu.berkeley.cs.amplab.adam.avro.ADAMVariant
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext

/**
 * This class converts VCF data to and from ADAM. This translation occurs at the abstraction level
 * of the GATK VariantContext which represents VCF data, and at the ADAMVariantContext level, which
 * aggregates ADAM variant/genotype/annotation data together.
 *
 * If an annotation has a corresponding set of fields in the VCF standard, a conversion to/from the
 * GATK VariantContext should be implemented in this class.
 */
private[adam] class VariantContextConverter extends Serializable {

  /**
   * Converts a single GATK variant into an ADAMVariantContext. This involves converting:
   *
   * - Alleles seen segregating at site
   * - Genotypes of samples
   * - Variant domain data
   *
   * @param vc GATK Variant context to convert.
   * @return ADAM variant context containing allele, genotype, and domain data.
   */
  def convert(vc: VariantContext): ADAMVariantContext = {
    val variant: ADAMVariant.Builder = ADAMVariant.newBuilder
      .setPosition(1)
    ADAMVariantContext(variant.build)
  }

}
