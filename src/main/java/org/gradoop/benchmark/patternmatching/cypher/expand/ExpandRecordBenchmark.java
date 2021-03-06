/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.benchmark.patternmatching.cypher.expand;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;


@SuppressWarnings("ALL")
public class ExpandRecordBenchmark {
  /**
   * Run with
   *
   * run -c org.gradoop.benchmark.patternmatching.cypher.expand.ExpandRecordBenchmark gradoop.jar inputPath lowerBound upperBound ObjectReuse{true|false}
   *
   * @param args arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    String inputPath = args[0];
    Integer lowerBound = Integer.parseInt(args[1]);
    Integer upperBound = Integer.parseInt(args[2]);
    boolean objectReuse = Boolean.parseBoolean(args[3]);

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    if(objectReuse) {
      env.getConfig().enableObjectReuse();
    }

    DataSet<ExpandRecord> res =
      new ExpandWithExpandRecord(readEdges(env, inputPath), lowerBound, upperBound).evaluate();

    System.out.println(res.count());
  }

  //------------------------------------------------------------------------------------------------
  // Helper
  //------------------------------------------------------------------------------------------------

  private static DataSet<Tuple3<Long, Long, Long>> readEdges(ExecutionEnvironment env, String inputPath) throws Exception {
    return env.readCsvFile(inputPath)
      .ignoreComments("#")
      .fieldDelimiter("\t")
      .types(Long.class, Long.class, Long.class);
  }
}
