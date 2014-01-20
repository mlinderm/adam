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

import org.scalatest.FunSuite
import edu.berkeley.cs.amplab.adam.avro.ADAMGenotypeAllele
import java.util

class PLINKSEQDosageConverterSuite extends FunSuite {
  test("Single Variant") {
    val converter = new PLINKSEQDosageConverter(List("sample1"))

    val vc = converter.convert("rs1 chr1 1 A T 0 1 0")
    val variant = vc.variant
    assert(variant.getContig.getContigName === "chr1")
    assert(variant.getPosition === 0)
    assert(variant.getReferenceAllele === "A")
    assert(variant.getVariantAllele === "T")

    assert(vc.genotypes.size === 1)
    val genotype = vc.genotypes.head
    assert(genotype.getSampleId === "sample1")
    assert(genotype.getAlleles === util.Arrays.asList(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt))

    assert(vc.databases.isDefined)
    val database = vc.databases.get
    assert(database.getDbsnpId === 1)
  }
}
