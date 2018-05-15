/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.NextIterator

/**
 * This is a specialized version of [[org.apache.spark.sql.execution.ShuffledRowRDD]] which is used
 * for shuffle in continuous processing.
 *
 * This RDD takes a [[ShuffleDependency]] (`dependency`),
 * an optional array of partition start indices as input arguments
 * (`specifiedPartitionStartIndices`), and (`totalShuffleNum`) which specifies
 * the total shuffle number in the job.
 */
class ContinuousShuffledRowRDD(
    dependency: ShuffleDependency[Int, InternalRow, InternalRow],
    specifiedPartitionStartIndices: Option[Array[Int]] = None, totalShuffleNum: Int)
  extends ShuffledRowRDD(dependency, specifiedPartitionStartIndices) {

  private val shuffleNumMaps = dependency.rdd.partitions.length

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val shuffledRowPartition = split.asInstanceOf[ShuffledRowRDDPartition]
    var currentEpoch: Int = shuffledRowPartition.lastEpoch
    if (currentEpoch == Int.MinValue) {
      // Init currentEpoch from localProperty for the first time calling compute method.
      // TODO: Get current epoch from epoch coordinator while task restart, also epoch is Long, we
      // should deal with it.
      currentEpoch = context.getLocalProperty(SparkEnv.START_EPOCH_KEY).toInt
    } else {
      currentEpoch += 1
    }

    // Update lastEpoch for the split.
    shuffledRowPartition.lastEpoch = currentEpoch

    // Create a ContinuousShuffleDependency which has new shuffleId based on continuous epoch
    val continuousDep = new ContinuousShuffleDependency(dependency._rdd,
      dependency, currentEpoch, totalShuffleNum, shuffleNumMaps)
    // The range of pre-shuffle partitions that we are fetching at here is
    // [startPreShufflePartitionIndex, endPreShufflePartitionIndex - 1].
    val reader =
    SparkEnv.get.shuffleManager.getReader(
      continuousDep.shuffleHandle,
      shuffledRowPartition.startPreShufflePartitionIndex,
      shuffledRowPartition.endPreShufflePartitionIndex,
      context)
    // The read method will be blocked until the shuffle data for current epoch is ready.
    val currentIterator =
      reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)

    new NextIterator[InternalRow] {
      override protected def getNext(): InternalRow = {
        if (currentIterator.hasNext) {
          currentIterator.next()
        } else {
          finished = true
          null
        }
      }

      override def close(): Unit = {
        // If there are jobs should be done after handling the shuffle data, clean/update
        // status variables for example, they can be triggered in the close method.
        logInfo(s"Finished handling shuffle data for epoch $currentEpoch with" +
          s" partitionId ${split.index} in task ${context.taskAttemptId}")
      }
    }
  }
}
