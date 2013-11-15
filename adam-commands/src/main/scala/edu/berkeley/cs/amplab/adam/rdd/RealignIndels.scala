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

package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget.{RealignmentTargetFinder,IndelRealignmentTarget,TargetOrdering}
import spark.{Logging, RDD}
import spark.broadcast.Broadcast
import scala.collection.immutable.TreeSet
import edu.berkeley.cs.amplab.adam.util.ImplicitJavaConversions
import scala.annotation.tailrec

private[rdd] object RealignIndels {

  def apply(rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {
    new RealignIndels().realignIndels(rdd)
  }
}

private[rdd] class RealignIndels extends Serializable with Logging {
  initLogging()

  val maxIndelSize = 3000
  val maxConcensusNumber = 30
  val lodThreshold = 5

  @tailrec def mapToTarget (read: ADAMRecord,
			    targets: TreeSet[IndelRealignmentTarget]): IndelRealignmentTarget = {

    if (targets.size == 1) {
      if (TargetOrdering.equals (targets.head, read)) {
	targets.head
      } else {
	IndelRealignmentTarget.emptyTarget
      }
    } else {
      val (head, tail) = targets.splitAt(targets.size / 2) 
      val reducedSet = if (TargetOrdering.lt(tail.head, read)) {
	head
      } else {
	tail
      }
      mapToTarget (read, reducedSet)
    }
  }

  def realignTargetGroup (targetGroup: (IndelRealignmentTarget, Seq[ADAMRecord])): Seq[ADAMRecord] = {
    val (target, reads) = targetGroup
    
    if (target.isEmpty) {
      reads
    } else {

      var mismatchSum = 0L

      val realignedReads 
      reads
    }
  }

  def sumMismatchQuality (read: ADAMRecord): Int = {
    
    var referencePos = record.getStart
    var readPos = 0

    val cigar = read.samtoolsCigar
    val mdTag = MdTag(record.getMismatchingPositions.toString, referencePos)

    var mismatchQual = 0

    cigar.getCigarElements.foreach(cigarElement =>
      cigarElement.getOperator match {
	case CigarOperator.M =>
	  if (!mdTag.isMatch(referencePos)) {
	    mismatchQual += read.phredQuals (readPos)
	  }

	  readPos += 1
	  referencePos += 1
	case _ =>
	  if (cigarElement.getOperator.consumesReadBases()) {
            readPos += cigarElement.getLength
          }
          if (cigarElement.getOperator.consumesReferenceBases()) {
            referencePos += cigarElement.getLength
          }
      })
  }

  def getReference (reads: Seq[ADAMRecord]): String = {
  }

  def realignIndels (rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {

    // find realignment targets
    log.info("Generating realignment targets...")
    val targets = RealignmentTargetFinder(rdd)

    // group reads by target
    log.info("Grouping reads by target...")
    val broadcastTargets = rdd.context.broadcast(targets)
    val readsMappedToTarget = rdd.groupBy (mapToTarget(_, broadcastTargets.value))

    // realign target groups
    log.info("Sorting reads by reference in ADAM RDD")
    readsMappedToTarget.flatMap(realignTargetGroup)
  }

}
