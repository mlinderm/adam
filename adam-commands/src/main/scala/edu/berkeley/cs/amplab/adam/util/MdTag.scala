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
package edu.berkeley.cs.amplab.adam.util

import scala.collection.immutable.NumericRange
import scala.util.matching.Regex
import net.sf.samtools.{Cigar, CigarOperator, CigarElement}
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.util.ImplicitJavaConversions._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._

object MdTagEvent extends Enumeration {
  val Match, Mismatch, Delete = Value
}

object MdTag {

  val digitPattern = new Regex("\\d+")
  val basesPattern = new Regex("[AGCTN]+")

  private def apply(mdTag: String, referenceStart: Long): MdTag = {
    new MdTag(mdTag, referenceStart)
  }

  def apply(mdTag: String): MdTag = {
    apply(mdTag, 0L)
  }

  def apply(mdTag: CharSequence, start: Long): MdTag = apply(mdTag.asInstanceOf[String], start)

  def apply(read: ADAMRecord, newCigar: Cigar): MdTag = {
    val md = new MdTag(read.getMismatchingPositions, read.getStart)
      
    md.moveAlignment(read, newCigar)
    
    md
  }

}

class MdTag(mdTagInput: String, referenceStart: Long) {

  private var matches = List[NumericRange[Long]]()
  private var mismatches = Map[Long, Char]()
  private var deletes = Map[Long, Char]()

  if (mdTagInput != null && mdTagInput.length > 0) {
    val mdTag = mdTagInput.toUpperCase
    val end = mdTag.length

    var offset = 0
    var referencePos = referenceStart

    def readMatches(errMsg: String): Unit = {
      MdTag.digitPattern.findPrefixOf(mdTag.substring(offset)) match {
        case None => throw new IllegalArgumentException(errMsg)
        case Some(s) =>
          val length = s.toInt
          if (length > 0) {
            matches ::= NumericRange(referencePos, referencePos + length, 1L)
          }
          offset += s.length
          referencePos += length
      }
    }

    readMatches("MD tag must start with a digit")

    while (offset < end) {
      val mdTagType = {
        if (mdTag.charAt(offset) == '^') {
          offset += 1
          MdTagEvent.Delete
        } else {
          MdTagEvent.Mismatch
        }
      }
      MdTag.basesPattern.findPrefixOf(mdTag.substring(offset)) match {
        case None => throw new IllegalArgumentException("Failed to find deleted or mismatched bases after a match: %s".format(mdTagInput))
        case Some(bases) =>
          mdTagType match {
            case MdTagEvent.Delete =>
              bases.foreach {
                base =>
                  deletes += (referencePos -> base)
                  referencePos += 1
              }
            case MdTagEvent.Mismatch =>
              bases.foreach {
                base =>
                  mismatches += (referencePos -> base)
                  referencePos += 1
              }
          }
          offset += bases.length
      }
      readMatches("MD tag should have matching bases after mismatched or missing bases")
    }
  }

  def isMatch(pos: Long): Boolean = {
    matches.exists(_.contains(pos))
  }

  def mismatchedBase(pos: Long): Option[Char] = {
    mismatches.get(pos)
  }

  def deletedBase(pos: Long): Option[Char] = {
    deletes.get(pos)
  }
  
  def hasMismatches(): Boolean = {
    !mismatches.isEmpty
  }

  /**
   * Given a read, returns the reference.
   *
   * @param read A read for which one desires the reference sequence.
   * @return A string corresponding to the reference overlapping this read.
   */
  def getReference (read: ADAMRecord): String = {
    getReference (read.getSequence, read.samtoolsCigar, read.getStart)
  }

  /**
   * Given a read sequence, cigar, and a reference start position, returns the reference.
   *
   * @param readSequence The base sequence of the read.
   * @param cigar The cigar for the read.
   * @param referenceFrom The starting point of this read alignment vs. the reference.
   * @return A string corresponding to the reference overlapping this read.
   */
  def getReference (readSequence: String, cigar: Cigar, referenceFrom: Long): String = {

    var referencePos = referenceFrom
    var readPos = 0
    var reference = ""
    
    // loop over all cigar elements
    cigar.getCigarElements.foreach(cigarElement => {
      cigarElement.getOperator match {
	case CigarOperator.M => {
          // if we are a match, loop over bases in element
	  for (i <- (0 until cigarElement.getLength)) {
            // if a mismatch, get from the mismatch set, else pull from read
	    if (mismatches.contains(referencePos)) {
	      reference += mismatches.get(referencePos)
	    } else {
	      reference += readSequence(readPos)
	    }

	    readPos += 1
	    referencePos += 1
	  }
	}
	case CigarOperator.D => {
          // if a delete, get from the delete pool
	  for (i <- (0 until cigarElement.getLength)) {
	    reference += deletes.get(referencePos)
	    
	    referencePos += 1
	  }
	}
	case _ => {
          // ignore inserts
	  if (cigarElement.getOperator.consumesReadBases) {
	    readPos += cigarElement.getLength
	  }
	  if (cigarElement.getOperator.consumesReferenceBases) {
	    throw new IllegalArgumentException ("Cannot handle operator: " + cigarElement.getOperator)
	  }
	}
      }
    })

    reference
  }

  /**
   * Given a single read and an updated cigar element, recalculates the MDTag.
   *
   * @param read Record for current alignment.
   * @param newCigar Realigned cigar string.
   */
  def moveAlignment (read: ADAMRecord, newCigar: Cigar) {
    
    val reference = getReference(read)
    var referencePos = 0
    var readPos = 0
    var sequence = read.getSequence

    // clear out internal sets
    deletes = deletes.empty
    mismatches = mismatches.empty
    matches = List[NumericRange[Long]]()

    // loop over cigar elements and refill sets
    newCigar.getCigarElements.foreach(cigarElement => {
      cigarElement.getOperator match {
	case CigarOperator.M => {
	  var rangeStart = 0L
	  var rangeEnd = 0L

          // dirty dancing to recalculate match sets
	  for (i <- 0 until cigarElement.getLength) {
	    if (reference(referencePos) == sequence(readPos)) {
	      rangeEnd = i.toLong
	    } else {
	      if (i != 0) {
		matches = ((rangeStart + read.getStart) to (rangeEnd + read.getStart)) :: matches
	      }

	      rangeStart = (i + 1).toLong
	    
	      mismatches += ((referencePos + read.getStart) -> reference(referencePos))
	    }

	    readPos += 1
	    referencePos += 1
	  }
	}
	case CigarOperator.D => {
	  for (i <- 0 until cigarElement.getLength) {
	    deletes += ((referencePos + read.getStart) -> reference(referencePos))
	    
	    referencePos += 1
	  }
	}
	case _ => {
	  if (cigarElement.getOperator.consumesReadBases) {
	    readPos += cigarElement.getLength
	  }
	  if (cigarElement.getOperator.consumesReferenceBases) {
	    throw new IllegalArgumentException ("Cannot handle operator: " + cigarElement.getOperator)
	  }
	}
      }
    })    
    // TODO: generate new MD string from updated MD tag.
  }

}
