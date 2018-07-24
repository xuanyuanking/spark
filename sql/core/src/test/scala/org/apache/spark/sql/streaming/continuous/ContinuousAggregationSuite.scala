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

package org.apache.spark.sql.streaming.continuous

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

class ContinuousAggregationSuite extends ContinuousSuiteBase {
  import testImplicits._

  test("not enabled") {
    val ex = intercept[AnalysisException] {
      val input = ContinuousMemoryStream.singlePartition[Int]
      testStream(input.toDF().agg(max('value)), OutputMode.Complete)()
    }

    assert(ex.getMessage.contains(
      "In continuous processing mode, coalesce(1) must be called before aggregate operation"))
  }

  test("basic") {
    withSQLConf(("spark.sql.streaming.unsupportedOperationCheck", "false")) {
      val input = ContinuousMemoryStream.singlePartition[Int]

      testStream(input.toDF().agg(max('value)), OutputMode.Complete)(
        AddData(input, 0, 1, 2),
        CheckAnswer(2),
        StopStream,
        AddData(input, 3, 4, 5),
        StartStream(),
        CheckAnswer(5),
        AddData(input, -1, -2, -3),
        CheckAnswer(5))
    }
  }

  test("multiple partitions with coalesce") {
    val input = ContinuousMemoryStream[Int]

    val df = input.toDF().coalesce(1).agg(max('value))

    testStream(df, OutputMode.Complete)(
      AddData(input, 0, 1, 2),
      CheckAnswer(2),
      StopStream,
      AddData(input, 3, 4, 5),
      StartStream(),
      CheckAnswer(5),
      AddData(input, -1, -2, -3),
      CheckAnswer(5))
  }

  test("multiple partitions with coalesce - multiple transformations") {
    val input = ContinuousMemoryStream[Int]

    // We use a barrier to make sure predicates both before and after coalesce work
    val df = input.toDF()
      .select('value as 'copy, 'value)
      .where('copy =!= 1)
      .planWithBarrier
      .coalesce(1)
      .where('copy =!= 2)
      .agg(max('value))

    testStream(df, OutputMode.Complete)(
      AddData(input, 0, 1, 2),
      CheckAnswer(0),
      StopStream,
      AddData(input, 3, 4, 5),
      StartStream(),
      CheckAnswer(5),
      AddData(input, -1, -2, -3),
      CheckAnswer(5))
  }

  test("multiple partitions with multiple coalesce") {
    val input = ContinuousMemoryStream[Int]

    val df = input.toDF()
      .coalesce(1)
      .planWithBarrier
      .coalesce(1)
      .select('value as 'copy, 'value)
      .agg(max('value))

    testStream(df, OutputMode.Complete)(
      AddData(input, 0, 1, 2),
      CheckAnswer(2),
      StopStream,
      AddData(input, 3, 4, 5),
      StartStream(),
      CheckAnswer(5),
      AddData(input, -1, -2, -3),
      CheckAnswer(5))
  }

  test("repeated restart") {
    withSQLConf(("spark.sql.streaming.unsupportedOperationCheck", "false")) {
      val input = ContinuousMemoryStream.singlePartition[Int]

      testStream(input.toDF().agg(max('value)), OutputMode.Complete)(
        AddData(input, 0, 1, 2),
        CheckAnswer(2),
        StopStream,
        StartStream(),
        StopStream,
        StartStream(),
        StopStream,
        StartStream(),
        AddData(input, 0),
        CheckAnswer(2),
        AddData(input, 5),
        CheckAnswer(5))
    }
  }

  test("test simple repartition") {
    val input = ContinuousMemoryStream[Int]
    val df = input.toDF()

    testStream(df.repartition(2))(
      StartStream(),
      AddData(input, 0, 1),
      CheckAnswer(0, 1),
      StopStream,
      AddData(input, 2, 3),
      StartStream(),
      CheckAnswer(0, 1, 2, 3),
      StopStream)
  }

  test("test continuous shuffle") {
    val input = ContinuousMemoryStream[Int]

    val df = input.toDF()
      .repartition(2)
      .planWithBarrier
      .select('value as 'copy, 'value)
      .agg(max('value))

    testStream(df, OutputMode.Complete)(
      AddData(input, 0, 1, 2),
      CheckAnswer(2),
      StopStream,
      AddData(input, 3, 4, 5),
      StartStream(),
      CheckAnswer(5),
      AddData(input, -1, -2, -3),
      CheckAnswer(5))
  }

  test("test continuous shuffle - groupBy") {
    val inputData = ContinuousMemoryStream[Int]
    val agg = inputData.toDF().select('value as 'copy, 'value).groupBy("value").count()

    testStream(agg, OutputMode.Complete)(
      AddData(inputData, 1, 2),
      StartStream(),
      CheckAnswer((1, 1), (2, 1)))
//      StopStream,
//      AddData(inputData, (3, 0), (2, 0)),
//      StartStream(additionalConfs = Map(SQLConf.SHUFFLE_PARTITIONS.key -> "5")),
//      CheckAnswer((1, 1), (2, 2), (3, 1)),
//      StopStream,
//      AddData(inputData, (3, 0), (1, 0)),
//      StartStream(additionalConfs = Map(SQLConf.SHUFFLE_PARTITIONS.key -> "1")),
//      CheckAnswer((1, 2), (2, 2), (3, 2)))
  }
}
