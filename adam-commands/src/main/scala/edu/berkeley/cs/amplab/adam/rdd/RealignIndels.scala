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

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget.{RealignmentTargetFinder,IndelRealignmentTarget,TargetOrdering}
import org.apache.spark.broadcast.Broadcast
import scala.collection.immutable.TreeSet
import edu.berkeley.cs.amplab.adam.util.ImplicitJavaConversions
import scala.annotation.tailrec
import scala.collection.mutable.Map
import net.sf.samtools.{Cigar, CigarOperator, CigarElement}
import scala.collection.immutable.NumericRange
import edu.berkeley.cs.amplab.adam.models.Consensus
import edu.berkeley.cs.amplab.adam.util.ImplicitJavaConversions._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.rich.RichCigar
import edu.berkeley.cs.amplab.adam.rich.RichCigar._
import edu.berkeley.cs.amplab.adam.util.MdTag

private[rdd] object RealignIndels {

  def apply(rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {
    new RealignIndels().realignIndels(rdd)
  }
}

private[rdd] class RealignIndels extends Serializable with Logging {
  initLogging()

  val maxIndelSize = 3000
  val maxConsensusNumber = 30
  val lodThreshold = 5

  @tailrec final def mapToTarget (read: ADAMRecord,
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
    val (target, reads): (IndelRealignmentTarget, Seq[ADAMRecord]) = targetGroup
    
    if (target.isEmpty) {
      reads
    } else {

      var mismatchSum = 0L

      var realignedReads = List[ADAMRecord]()
      var readsToClean = List[ADAMRecord]()
      var consensus = List[Consensus]()

      reads.foreach(r => {
	
	if (r.samtoolsCigar.numAlignmentBlocks == 2) {
	  r.samtoolsCigar = leftAlignIndel(r.samtoolsCigar)
	  r.mdTag.moveAlignment(r, r.samtoolsCigar)
	}

	if (r.mdTag.hasMismatches) {
	  readsToClean = r :: readsToClean
	  
	  consensus = Consensus.generateAlternateConsensus(r.getSequence, r.getStart, r.samtoolsCigar) match {
            case Some(o) => o :: consensus
            case None => consensus
          }
	} else {
	  realignedReads = r :: realignedReads
        }
      })

      if(readsToClean.length > 0 && consensus.length > 0) {

        // get reference from reads
        val (reference, refStart, refEnd) = getReferenceFromReads(reads)

        // do not check realigned reads - they must match
        val totalMismatchSumPreCleaning = readsToClean.map(sumMismatchQuality(_)).reduce(_ + _)
        
        var consensusOutcomes = List[(Int, Consensus, Map[ADAMRecord,Int])]()

        consensus.foreach(c => {
          val consensusSequence = c.insertIntoReference(reference, refStart, refEnd)

          val sweptValues = readsToClean.map(r => {
            val (qual, pos) = sweepReadOverReferenceForQuality(r.getSequence, reference, r.qualityScores.map(_.toInt))
            val originalQual = sumMismatchQuality(r)
            
            if (qual < originalQual) {
              (r, (qual, pos))
            } else {
              (r, (originalQual, -1))
            }
          })
          
          val totalQuality = sweptValues.map(_._2._1).reduce(_ + _)
          var readMappings = Map[ADAMRecord,Int]()
          sweptValues.map(kv => (kv._1, kv._2._2)).foreach(m => {
            readMappings += (m._1 -> m._2)
          })

          consensusOutcomes = (totalQuality, c, readMappings) :: consensusOutcomes
        })

        val bestConsensusTuple = consensusOutcomes.reduce ((c1: (Int, Consensus, Map[ADAMRecord, Int]), c2: (Int, Consensus, Map[ADAMRecord, Int])) => {
          if (c1._1 <= c2._1) {
            c1
          } else {
            c2
          }
        })

        val (bestConsensusMismatchSum, bestConsensus, bestMappings) = bestConsensusTuple

        if (totalMismatchSumPreCleaning.toDouble / bestConsensusMismatchSum > 5.0) {
          readsToClean.foreach(r => {

            val remapping = bestMappings(r)
            
            if (remapping != -1) {
              r.setMapq(r.getMapq + 10)
              
              r.setStart(refStart + remapping)

              val newCigar: Cigar = if (refStart + remapping >= bestConsensus.index.head && refStart + remapping <= bestConsensus.index.end) {
                                                
                val (idElement, endLength) = if(bestConsensus.index.head == bestConsensus.index.end) {
                  (new CigarElement(bestConsensus.consensus.length, CigarOperator.I),
                   r.getSequence.length - bestConsensus.consensus.length - (bestConsensus.index.head - (refStart + remapping)))
                } else {
                  (new CigarElement((bestConsensus.index.end - bestConsensus.index.head).toInt, CigarOperator.D),
                   r.getSequence.length - (bestConsensus.index.head - (refStart + remapping)))
                }

                val cigarElements = List[CigarElement](new CigarElement((refStart + remapping - bestConsensus.index.head).toInt, CigarOperator.M),
                                                       idElement,
                                                       new CigarElement(endLength.toInt, CigarOperator.M))

                new Cigar(cigarElements)
              } else {
                new Cigar(List[CigarElement](new CigarElement(r.getSequence.length, CigarOperator.M)))
              }

              r.mdTag.moveAlignment(r, newCigar)
              r.setCigar(newCigar.toString)
              // TODO: fix mismatchingPositions string
            }
          })
        }
      }
       
    readsToClean ::: realignedReads
    }
  }

  def getReferenceFromReads (reads: Seq[ADAMRecord]): (String, Long, Long) = {
    val readRefs = reads.map((r: ADAMRecord) => {
      (r.mdTag.getReference(r), r.getStart.toLong to r.end.get)
    })
      .sortBy(_._2.head)

    val ref = readRefs.foldRight[(String,Long)](("", readRefs.head._2.head))((refReads: (String, NumericRange[Long]), reference: (String, Long)) => {
      if (refReads._2.end < reference._2) {
        reference
      } else if (reference._2 >= refReads._2.head) {
        (reference._1 + refReads._1.substring((reference._2 - refReads._2.head).toInt), refReads._2.end)
      } else {
        // everyone we love just died
        ("", 0L)
      }
    })

    (ref._1, readRefs.head._2.head, ref._2)
  }

  def leftAlignIndel (cigar: Cigar): Cigar = {
    var indelPos = -1
    var pos = 0
    var indelLength = 0
    
    cigar.getCigarElements.map(elem => {
      elem.getOperator match {
        case (CigarOperator.I | CigarOperator.D) => {
          if(indelPos == -1) {
            indelPos = pos
            indelLength = elem.getLength
          } else {
            return cigar
          }
          pos += 1
        }
        case _ => pos += 1
      }
    })
    
    @tailrec def shiftIndel (cigar: Cigar, position: Int, shifts: Int): Cigar = {
      val newCigar = new Cigar(cigar.getCigarElements).moveLeft(position)

      if (shifts == 0 || !newCigar.isWellFormed(cigar.getLength)) {
        cigar
      } else {
        shiftIndel (newCigar, position, shifts - 1)
      }
    }

    if (indelPos != -1) {
      shiftIndel (cigar, indelPos, indelLength)
    } else {
      cigar
    }
  }

  def sweepReadOverReferenceForQuality (read: String, reference: String, qualities: Seq[Int]): (Int, Int) = {
        
    var qualityScores = List[(Int, Int)]()

    for (i <- 0 until (reference.length - read.length)) {
      qualityScores = (sumMismatchQualityIgnoreCigar(read, reference.substring(i, i + read.length), qualities), i) :: qualityScores
    }
    
    qualityScores.reduce ((p1: (Int, Int), p2: (Int, Int)) => {
      if (p1._1 < p2._1) {
        p1
      } else {
        p2
      }
    })
  }

  def sumMismatchQualityIgnoreCigar (read: String, reference: String, qualities: Seq[Int]): Int = {
    read.zip(reference)
      .zip(qualities)
      .filter(r => r._1._1 != r._1._2)
      .map(_._2)
      .reduce(_ + _)
  }

  def sumMismatchQuality (read: ADAMRecord): Int = {
    sumMismatchQuality (read.samtoolsCigar, read.mdTag, read.getStart, read.qualityScores.map(_.toInt))
  }

  def sumMismatchQuality (cigar: Cigar, mdTag: MdTag, start: Long, phredQuals: Seq[Int]): Int = {
    
    var referencePos = start
    var readPos = 0
    var mismatchQual = 0

    cigar.getCigarElements.foreach(cigarElement =>
      cigarElement.getOperator match {
	case CigarOperator.M =>
	  if (!mdTag.isMatch(referencePos)) {
	    mismatchQual += phredQuals (readPos)
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

    mismatchQual
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
