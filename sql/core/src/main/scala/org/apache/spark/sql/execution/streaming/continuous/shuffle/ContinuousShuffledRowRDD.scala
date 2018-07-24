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

package org.apache.spark.sql.execution.streaming.continuous.shuffle

import java.util.UUID

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.continuous.{ContinuousExecution, EpochTracker}
import org.apache.spark.util.ThreadUtils

case class ContinuousShuffleRowRDDPartition(
    index: Int,
    endpointName: String,
    queueSize: Int,
    numShuffleWriters: Int,
    epochIntervalMs: Long)
  extends Partition {
  // Initialized only on the executor, and only once even as we call compute() multiple times.
  lazy val (reader: ContinuousShuffleReader, endpoint) = {
    val env = SparkEnv.get.rpcEnv
    val receiver = new RPCContinuousShuffleReader(
      queueSize, numShuffleWriters, epochIntervalMs, env)
    val endpoint = env.setupEndpoint(endpointName, receiver)

    TaskContext.get().addTaskCompletionListener { ctx =>
      env.stop(endpoint)
    }
    (receiver, endpoint)
  }
  // This flag will be flipped on the executors to indicate that the threads processing
  // partitions of the write-side RDD have been started. These will run indefinitely
  // asynchronously as epochs of the coalesce RDD complete on the read side.
  private[continuous] var writersInitialized: Boolean = false
}

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
    dependency: ContinuousDependency[Int, InternalRow],
    readerQueueSize: Int) extends RDD[InternalRow](dependency.rdd.context, Nil) {

  private val shuffleNumMaps = dependency.rdd.getNumPartitions
  private val numPartitions = dependency.partitioner.numPartitions
  private val epochIntervalMs = dependency.rdd.sparkContext.getLocalProperty(
    ContinuousExecution.EPOCH_INTERVAL_KEY).toLong

  private val readerEndpointNames = (0 until numPartitions).map { i =>
    s"ContinuousShuffledRowRDD-part$i-${UUID.randomUUID()}"
  }

  override def getPartitions: Array[Partition] = {
    (0 until numPartitions).map { partIndex =>
      ContinuousShuffleRowRDDPartition(
        partIndex,
        readerEndpointNames(partIndex),
        readerQueueSize,
        shuffleNumMaps,
        epochIntervalMs)
    }.toArray
  }

  private lazy val threadPool = ThreadUtils.newDaemonFixedThreadPool(
    dependency._rdd.getNumPartitions,
    this.name)

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val part = split.asInstanceOf[ContinuousShuffleRowRDDPartition]
    if (!part.writersInitialized) {
      val rpcEnv = SparkEnv.get.rpcEnv

      // trigger lazy initialization
      part.endpoint
      val endpointRefs = readerEndpointNames.map { endpointName =>
        rpcEnv.setupEndpointRef(rpcEnv.address, endpointName)
      }

      val runnables = dependency.rdd.partitions.map { prevSplit =>
        new Runnable() {
          override def run(): Unit = {
            TaskContext.setTaskContext(context)

            val writer: ContinuousShuffleWriter = new RPCContinuousShuffleWriter(
              prevSplit.index, dependency.partitioner, endpointRefs.toArray)

            EpochTracker.initializeCurrentEpoch(
              context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong)
            while (!context.isInterrupted() && !context.isCompleted()) {
              writer.write(dependency.rdd.compute(prevSplit, context)
                .asInstanceOf[Iterator[UnsafeRow]])
              // Note that current epoch is a non-inheritable thread local, so each writer thread
              // can properly increment its own epoch without affecting the main task thread.
              EpochTracker.incrementCurrentEpoch()
            }
          }
        }
      }

      context.addTaskCompletionListener { ctx =>
        threadPool.shutdownNow()
      }

      part.writersInitialized = true

      runnables.foreach(threadPool.execute)
    }
    dependency.rdd.barrier()
    part.reader.read()
  }
}
