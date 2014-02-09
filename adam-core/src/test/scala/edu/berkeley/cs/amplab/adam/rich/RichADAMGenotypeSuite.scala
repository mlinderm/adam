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

package edu.berkeley.cs.amplab.adam.rich

import org.scalatest.FunSuite
import edu.berkeley.cs.amplab.adam.avro.{ADAMGenotypeAllele, ADAMGenotype, ADAMContig, ADAMVariant}
import edu.berkeley.cs.amplab.adam.rich.RichADAMGenotype._
import scala.collection.JavaConversions._

class RichADAMGenotypeSuite extends FunSuite {
  def v0 = ADAMVariant.newBuilder
    .setContig(ADAMContig.newBuilder.setContigName("chr1").build)
    .setPosition(0).setReferenceAllele("A").setVariantAllele("T")
    .build

  test("all types for diploid genotype") {
    val gb = ADAMGenotype.newBuilder.setVariant(v0)

    val hom_ref = gb.setAlleles(List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Ref)).build
    assert(hom_ref.getType === GenotypeType.HOM_REF)

    val het1 = gb.setAlleles(List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt)).build
    assert(het1.getType === GenotypeType.HET)
    val het2 = gb.setAlleles(List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt)).build
    assert(het2.getType === GenotypeType.HET)

    val hom_alt = gb.setAlleles(List(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Alt)).build
    assert(hom_alt.getType === GenotypeType.HOM_ALT)

    val no_call = gb.setAlleles(List(ADAMGenotypeAllele.NoCall, ADAMGenotypeAllele.NoCall)).build
    assert(no_call.getType === GenotypeType.NO_CALL)
  }

}
