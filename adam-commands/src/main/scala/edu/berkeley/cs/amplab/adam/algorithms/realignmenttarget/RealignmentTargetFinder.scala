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

import spark.RDD
import spark.SparkContext._
import edu.berkeley.cs.amplab.adam.avro.{ADAMRecord,ADAMPileup}
import edu.berkeley.cs.amplab.adam.commands.Read2PileupProcessor
import scala.annotation.tailrec
import scala.collection.immutable.TreeSet

object RealignmentTargetFinder {
  
  def apply(rdd: RDD[ADAMRecord]): TreeSet[IndelRealignmentTarget] = {
    new RealignmentTargetFinder().findTargets (rdd)
  }
  
}

class RealignmentTargetFinder extends Serializable {

  @tailrec protected final def joinTargets (
    first: TreeSet[IndelRealignmentTarget], 
    second: TreeSet[IndelRealignmentTarget]): TreeSet[IndelRealignmentTarget] = {
    
    if (!TargetOrdering.overlap(first.last, second.head)) {
      first.union(second)
    } else {
      joinTargets (first - first.last + first.last.merge(second.head), second - second.head)
    }
  }

  def findTargets (reads: RDD[ADAMRecord]) : TreeSet[IndelRealignmentTarget] = {

    val processor = new Read2PileupProcessor

    val rods: RDD[Seq[ADAMPileup]] = reads.flatMap(processor.readToPileups(_))
      .groupBy(_.getPosition).map(_._2)

    val targetSet = rods.map(IndelRealignmentTarget(_))
      .filter(!_.isEmpty)
      .keyBy(_.getReadRange.start)
      .sortByKey()
      .map(new TreeSet()(TargetOrdering) + _._2)
      .fold(new TreeSet()(TargetOrdering))(joinTargets)

    targetSet
  }

}
