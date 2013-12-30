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
package edu.berkeley.cs.amplab.adam.models

import edu.berkeley.cs.amplab.adam.avro.ADAMVariant

object ADAMVariantContext {


}

/**
 * Container class for variant contexts. Provides no methods, or anything of the like, just direct access
 * to the data contained within.
 *
 * Of note: this class is meant to organize variants, genotypes, and their annotated data. If annotated data
 * is specific to the locus, but not to a specific variant/genotype, it should exist as field wrapped in an
 * option. If an annotation is specific to a genotype, variant, or the combined genotype of a sample, it should
 * be implemented as a map. This is because the map can be left empty, and because the map provides associative
 * access.
 *
 * Any updates that add annotations should impact three or four files:
 * - adam-format/.../adam.avdl
 * - this file
 * - rdd/AdamContext.scala --> adamVariantLoad function
 * - If there is a corresponding conversion of that data between VCF and ADAM, commands/VariantContextConverter.scala
 */
case class ADAMVariantContext(variant: ADAMVariant) {
}
