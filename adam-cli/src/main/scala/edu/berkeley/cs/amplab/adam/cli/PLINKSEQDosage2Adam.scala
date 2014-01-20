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

package edu.berkeley.cs.amplab.adam.cli

import org.kohsuke.args4j.Argument
import scala.io.Source
import org.apache.spark.Logging

object PLINKSEQDosage2Adam extends AdamCommandCompanion {
  val commandName: String = "dosage2adam"
  val commandDescription: String = "Converts a PLINK/SEQ formatted dosage file to ADAM/Parquet and writes locally or to HDFS, etc."

  def apply(cmdLine: Array[String]) = {
    new PLINKSEQDosage2Adam(Args4j[PLINKSEQDosage2AdamArgs](cmdLine))
  }
}

class PLINKSEQDosage2AdamArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "dosage", usage = "The dosage file to convert", index = 0)
  var dosageFile: String = null
  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM data", index = 1)
  var outputPath: String = null
}

class PLINKSEQDosage2Adam(args:PLINKSEQDosage2AdamArgs) extends AdamCommand with Logging {
  initLogging()

  val companion = PLINKSEQDosage2Adam
  def run() = {
    var lines = Source.fromFile(args.dosageFile).getLines

    // Is the first line a header? If so, extract the sample names. If not and if there is not
    // another map file, stop with an error.
    val first_line = lines.next
    assert(first_line.startsWith("#"), "First line needs to specify the sample names")

    val samples = first_line.substring(1).split("""\s""")

    // Check reference (using Picard Reference file access functions) and switch alleles if needed. There
    // is no guarantee that the alleles are actually ref/alt.


    // Write to parquet file

  }
}
