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

package edu.berkeley.cs.amplab.adam.utils

import net.sf.samtools.{Cigar, CigarOperator, TextCigarCodec, CigarElement}
import edu.berkeley.cs.amplab.adam.util.ImplicitJavaConversions
import scala.annotation.tailrec

object RichCigar {

  def apply (cigar: Cigar) = {
    new RichCigar(cigar)
  }

  implicit def cigarToRichCigar (cigar: Cigar): RichCigar = new RichCigar(cigar)

}

class RichCigar (cigar: Cigar) {
  
  lazy val numElements: Int = cigar.numCigarElements

  lazy val numAlignmentBlocks: Int = {
    cigar.getCigarElements.map (element => {
      element.getOperator match {
	case CigarOperator.M => 1
	case _ => 0
      }
    }).reduce (_ + _)
  }

  def moveLeft (index: Int): Cigar = {
    var elements = List[CigarElement](numElements)

    @tailrec def moveCigarLeft (index: Int, cigarElements: List[CigarElement]): List[CigarElement] = {
      if (index == 0) {
	val elemMovedLeft = new CigarElement (cigarElements.head.getLength - 1, cigarElements.head.getOperator)
	val elemPadded = Option(list.tail.head) match {
	  case Some(o) => new CigarElement(o.getLength + 1, o.getOperator) :: list.tail.tail
	  case None => List (new CigarElement(1, CigarOperator.M))
	}
	
	elemMovedLeft :: elemPadded
      } else {
	cigarElements.head :: moveCigarLeft (index - 1, cigarElements.tail)
      }
    }

    new Cigar(moveCigarLeft (index, elements))
  }

}
