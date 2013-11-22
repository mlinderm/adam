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
 
package edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget

import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup,ADAMRecord}
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import scala.collection.immutable.{HashSet, NumericRange}

object TargetOrdering extends Ordering[IndelRealignmentTarget] {
  def compare (a: IndelRealignmentTarget, b: IndelRealignmentTarget) : Int = a.getReadRange.start compare b.getReadRange.start

  def lt (target: IndelRealignmentTarget, read: ADAMRecord): Boolean = target.getReadRange.start < read.getStart

  def equals (target: IndelRealignmentTarget, read: ADAMRecord): Boolean = {
    (target.getReadRange.start == read.getStart) && (target.getReadRange.end == read.end.get)
  }

  def overlap (a: IndelRealignmentTarget, b: IndelRealignmentTarget) : Boolean = {
    ((a.getReadRange.start >= b.getReadRange.start && a.getReadRange.start <= b.getReadRange.end) || 
     (a.getReadRange.end >= b.getReadRange.start && a.getReadRange.end <= b.getReadRange.start))
  }
}

class IndelRange (indelRange: NumericRange.Inclusive[Long], readRange: NumericRange.Inclusive[Long]) {
  
  def merge (ir: IndelRange): IndelRange = {
    assert (indelRange == ir.getIndelRange)
    // do not need to check read range - read range must contain indel range, so if
    // indel range is the same, read ranges will overlap
    
    new IndelRange (indelRange,
		    (readRange.start min ir.getReadRange.start) to (readRange.end max ir.getReadRange.end))
  }

  def getIndelRange (): NumericRange.Inclusive[Long] = indelRange

  def getReadRange (): NumericRange.Inclusive[Long] = readRange

}

class SNPRange (snpSite: Long, readRange: NumericRange.Inclusive[Long]) {
  
  def merge (sr: SNPRange): SNPRange = {
    assert (snpSite == sr.getSNPSite)
    // do not need to check read range - read range must contain snp site, so if
    // snp site is the same, read ranges will overlap
    
    new SNPRange (snpSite,
		    (readRange.start min sr.getReadRange.start) to (readRange.end max sr.getReadRange.end))
  }

  def getSNPSite (): Long = snpSite

  def getReadRange (): NumericRange.Inclusive[Long] = readRange

}

object IndelRealignmentTarget {

  val mismatchThreshold = 0.15

  def apply (rod: Seq[ADAMPileup]): IndelRealignmentTarget = {
    
    def mapEvent (pileup: ADAMPileup): IndelRange = {
      Option(pileup.getReadBase) match {
	case None => { // deletion
	  new IndelRange((pileup.getPosition.toLong - pileup.getRangeOffset.toLong) to (pileup.getPosition.toLong + pileup.getRangeLength.toLong - pileup.getRangeOffset.toLong),
			 pileup.getReadStart.toLong to pileup.getReadEnd.toLong)
	}
	case Some(o) => { // insert
	  new IndelRange(pileup.getPosition.toLong to pileup.getPosition.toLong,
			 pileup.getReadStart.toLong to pileup.getReadEnd.toLong)
	}
      }
    }

    def mapPoint (pileup: ADAMPileup): SNPRange = {
      new SNPRange(pileup.getPosition, pileup.getReadStart.toLong to pileup.getReadEnd.toLong)
    }

    val indels = rod.filter(_.getRangeOffset != null)
    val matches = rod.filter(r => r.getRangeOffset == null && r.getNumSoftClipped == 0)
      .filter(r => r.getReadBase == r.getReferenceBase)
    val mismatches = rod.filter(r => r.getRangeOffset == null  && r.getNumSoftClipped == 0)
      .filter(r => r.getReadBase != r.getReferenceBase)

    val matchQuality = matches.map(_.getSangerQuality).reduce(_ + _)
    val mismatchQuality = mismatches.map(_.getSangerQuality).reduce(_ + _)
    
    if (mismatchQuality.toDouble / matchQuality.toDouble >= mismatchThreshold) {
      new IndelRealignmentTarget(indels.map(mapEvent).toSet, mismatches.map(mapPoint).toSet)
    } else {
      new IndelRealignmentTarget(indels.map(mapEvent).toSet, HashSet[SNPRange]())
    }
  }

  def emptyTarget (): IndelRealignmentTarget = {
    new IndelRealignmentTarget(new HashSet[IndelRange](), new HashSet[SNPRange]())
  }

}

class IndelRealignmentTarget (indelSet: Set[IndelRange], snpSet: Set[SNPRange]) {

  lazy val readRange = (indelSet.toList.map (_.getReadRange) ++ snpSet.toList.map(_.getReadRange))
    .reduce ((a: NumericRange.Inclusive[Long], b: NumericRange.Inclusive[Long]) => (a.start min b.start) to (a.end max b.end))

  def merge (target: IndelRealignmentTarget): IndelRealignmentTarget = {
    new IndelRealignmentTarget (indelSet ++ target.getIndelSet, snpSet ++ target.getSNPSet)
  }

  def isEmpty (): Boolean = {
    indelSet.isEmpty && snpSet.isEmpty
  }

  def getReadRange (): NumericRange.Inclusive[Long] = readRange
 
  protected def getSNPSet (): Set[SNPRange] = snpSet

  protected def getIndelSet (): Set[IndelRange] = indelSet

}
