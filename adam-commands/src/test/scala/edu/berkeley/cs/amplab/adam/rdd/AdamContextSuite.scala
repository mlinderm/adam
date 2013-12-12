/**
 * Copyright (c) 2013 Genome Bridge LLC
 */
package edu.berkeley.cs.amplab.adam.rdd

import parquet.filter.UnboundRecordFilter
import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, ADAMRecord, Base}
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.spark.SparkContext._

class AdamContextSuite extends SparkFunSuite {

  sparkTest("can read a small .SAM file") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
    val reads : RDD[ADAMRecord] = sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
    assert( reads.count() === 20 )
  }

  sparkTest("can generate small pileup from bam file file") {
    val path = ClassLoader.getSystemClassLoader.getResource("small_realignment_targets.bam").getFile
    val reads : RDD[ADAMRecord] = sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
    Console.println("read reads, creating pileup")
    val pileup: RDD[ADAMPileup] = reads.adamRecords2Pileup()
      .keyBy(_.getPosition)
      .sortByKey(ascending = true, numPartitions = 1)
      .map(_._2)
    val pileup_count = pileup.count()
    Console.println("pileup created")

    assert(pileup_count === 704 + 3)  // there are three insertions in total
    val pileup_collected = pileup.collect()

    // check just the first position
    val first_index = 0
    assert(pileup_collected(first_index).getPosition === 701293 - 1) // samtools mpileup is 1-based
    assert(pileup_collected(first_index).getReadBase === Base.T)
    assert(pileup_collected(first_index).getCountAtPosition === 1)
    assert(pileup_collected(first_index).getReferenceBase === Base.T)

    // 702258 (start position of second read) has a SNP (reference: G, read: C)
    val second_index = first_index + 100
    assert(pileup_collected(second_index).getPosition === 702258 - 1)
    assert(pileup_collected(second_index).getReadBase === Base.G)
    assert(pileup_collected(second_index).getReferenceBase === Base.C)

    // second read has CIGAR 32M1D33M1I34M, so a deletion that is eventually followed
    // by an insertion; MD: 0G24A6^T67
    // first check the deletion
    val third_index = second_index + 32
    assert(pileup_collected(third_index).getPosition === 702290 - 1)
    assert(pileup_collected(third_index).getReadBase === null)
    assert(pileup_collected(third_index).getReferenceBase === Base.T)
    // now check the insertion
    val fourth_index = third_index + 33
    assert(pileup_collected(fourth_index).getPosition === 702323 - 1)
    assert(pileup_collected(fourth_index).getReadBase === Base.C)
    assert(pileup_collected(fourth_index).getReferenceBase === Base.C)
    assert(pileup_collected(fourth_index).getRangeOffset === null)
    assert(pileup_collected(fourth_index).getRangeLength === null)
    // we expect to see first the next reference base, then the inserted base
    assert(pileup_collected(fourth_index+1).getPosition === 702324 - 1)
    assert(pileup_collected(fourth_index+1).getReadBase === Base.T)
    assert(pileup_collected(fourth_index+1).getReferenceBase === Base.T)
    assert(pileup_collected(fourth_index+1).getRangeOffset === null)
    assert(pileup_collected(fourth_index+1).getRangeLength === null)
    // now here comes the inserted base
    assert(pileup_collected(fourth_index+2).getPosition === 702324 - 1)
    assert(pileup_collected(fourth_index+2).getReadBase === Base.A)
    assert(pileup_collected(fourth_index+2).getReferenceBase === null)
    assert(pileup_collected(fourth_index+2).getRangeOffset === 0)
    assert(pileup_collected(fourth_index+2).getRangeLength === 1)

    // the last read has CIGAR 73M4D27M and MD 1C71^GCTC25T1
    // we only check the deletion
    val fifth_index = pileup_collected.length - 27 - 3 - 1
    assert(pileup_collected(fifth_index).getPosition === 869645 - 1)
    assert(pileup_collected(fifth_index).getReadBase === null)
    assert(pileup_collected(fifth_index).getReferenceBase === Base.G)
    assert(pileup_collected(fifth_index).getRangeOffset === 0)
    assert(pileup_collected(fifth_index).getRangeLength === 4)
    assert(pileup_collected(fifth_index+1).getPosition === 869646 - 1)
    assert(pileup_collected(fifth_index+1).getReadBase === null)
    assert(pileup_collected(fifth_index+1).getReferenceBase === Base.C)
    assert(pileup_collected(fifth_index+1).getRangeOffset === 1)
    assert(pileup_collected(fifth_index+1).getRangeLength === 4)
    assert(pileup_collected(fifth_index+2).getPosition === 869647 - 1)
    assert(pileup_collected(fifth_index+2).getReadBase === null)
    assert(pileup_collected(fifth_index+2).getReferenceBase === Base.T)
    assert(pileup_collected(fifth_index+2).getRangeOffset === 2)
    assert(pileup_collected(fifth_index+2).getRangeLength === 4)
    assert(pileup_collected(fifth_index+3).getPosition === 869648 - 1)
    assert(pileup_collected(fifth_index+3).getReadBase === null)
    assert(pileup_collected(fifth_index+3).getReferenceBase === Base.C)
    assert(pileup_collected(fifth_index+3).getRangeOffset === 3)
    assert(pileup_collected(fifth_index+3).getRangeLength === 4)

    // TODO: add read with multi-base insertions
  }
}


