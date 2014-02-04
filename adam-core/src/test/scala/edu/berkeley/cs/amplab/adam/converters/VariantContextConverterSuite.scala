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

import scala.collection.JavaConverters._
import scala.collection.JavaConversions
import org.scalatest.FunSuite
import org.broadinstitute.variant.variantcontext.{Allele, VariantContextBuilder, GenotypeBuilder}
import java.lang.Integer
import edu.berkeley.cs.amplab.adam.models.{SequenceRecord, SequenceDictionary}

class VariantContextConverterSuite extends FunSuite {
  val dictionary = SequenceDictionary(SequenceRecord(1, "chr1", 249250621, "file://ucsc.hg19.fasta", "1b22b98cdeb4a9304cb5d48026a85128"))

  test("Convert site-only SNV") {
    val vc = new VariantContextBuilder()
      .alleles(List(Allele.create("A",true), Allele.create("T")).asJavaCollection)
      .start(1L)
      .stop(1L)
      .chr("chr1")
      .make()

    val converter = new VariantContextConverter(Some(dictionary))

    val adamVCs = converter.convert(vc)
    assert(adamVCs.length === 1)
    val adamVC = adamVCs.head

    assert(adamVC.genotypes.length === 0)

    val variant = adamVC.variant

    val contig = variant.getContig
    assert(contig.getContigName === "chr1")
    assert(contig.getReferenceLength === 249250621)
    assert(contig.getReferenceURL === "file://ucsc.hg19.fasta")
    assert(contig.getReferenceMD5 === "1b22b98cdeb4a9304cb5d48026a85128")

    assert(variant.getReferenceAllele === "A")
    assert(variant.getPosition === 0L)
  }

  test("Convert genotypes with phase information") {
    val vcb = new VariantContextBuilder()
      .alleles(List(Allele.create("A",true), Allele.create("T")).asJavaCollection)
      .start(1L)
      .stop(1L)
      .chr("chr1")

    val genotypeAttributes = JavaConversions.mapAsJavaMap(Map[String, Object]("PQ" -> new Integer(50), "PS" -> "1"))
    val genotype = GenotypeBuilder.create("NA12878", vcb.getAlleles(), genotypeAttributes)
    val vc = vcb.genotypes(List(genotype).asJavaCollection).make()

    val converter = new VariantContextConverter(Some(dictionary))

    val adamVCs = converter.convert(vc)
    assert(adamVCs.length === 1)
    val adamVC = adamVCs.head

    assert(adamVC.genotypes.length === 1)

    val variant = adamVC.variant
    assert(variant.getReferenceAllele === "A")
    assert(variant.getPosition === 0L)
  }

}
