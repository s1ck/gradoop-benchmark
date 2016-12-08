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

package org.gradoop.benchmark.patternmatching.cypher.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.benchmark.patternmatching.cypher.join.records.EmbeddingRecord;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings("ALL")
public class EmbeddingRecordBenchmark {

  public static class JoinEmbeddingRecords
    implements JoinFunction<EmbeddingRecord, EmbeddingRecord, EmbeddingRecord> {

    private final List<Integer> joinColumnsRight;

    @SuppressWarnings("unchecked")
    public JoinEmbeddingRecords(List<Integer> joinColumnsRight) {
      this.joinColumnsRight = joinColumnsRight;
    }

    @Override
    public EmbeddingRecord join(EmbeddingRecord first, EmbeddingRecord second) throws Exception {
      byte[] data = new byte[first.data.length + second.data.length];
      System.arraycopy(first.data, 0, data, 0, first.data.length);
      System.arraycopy(second.data, 0, data, first.data.length, second.data.length);

      return new EmbeddingRecord(data, first.size()+second.size());
    }
  }

  public static class EmbeddingRecordKeyExtractor implements KeySelector<EmbeddingRecord, String> {
    private final List<Integer> columns;
  
    public EmbeddingRecordKeyExtractor(List<Integer> columns) {
      this.columns = columns;
    }
  
    @Override
    public String getKey(EmbeddingRecord value) throws Exception {
      String res = "";
      for(int i : columns) {
        res += "|" + value.getId(i);
      }
      return res;
    }
  }

  /**
   * Run with
   *
   * run -c org.gradoop.examples.FlinkGenericTupleTest gradoop.jar inputPath q[0-n] ObjectReuse{true|false}
   *
   * @param args arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    String inputPath = args[0];
    String query = args[1];
    boolean objectReuse = Boolean.parseBoolean(args[2]);

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    if(objectReuse) {
      env.getConfig().enableObjectReuse();
    }

    performQuery(readEdges(env, inputPath), query);
  }

  //------------------------------------------------------------------------------------------------
  // Helper
  //------------------------------------------------------------------------------------------------

  private static  void performQuery(DataSet<EmbeddingRecord> edges, String query) throws
    Exception {
    switch (query) {
    case "q0" : q0(edges).count(); break;
    case "q1" : q1(edges).count(); break;
    case "q2" : q2(edges).count(); break;
    case "q3" : q3(edges).count(); break;
    case "q4" : q4(edges).count(); break;
    case "q5" : q5(edges).count(); break;
    default: throw new IllegalArgumentException(query + " not implemented");
    }
  }

  private static DataSet<EmbeddingRecord> readEdges(ExecutionEnvironment env, String inputPath) throws Exception {
    return env.readCsvFile(inputPath)
      .ignoreComments("#")
      .fieldDelimiter("\t")
      .types(Long.class, Long.class, Long.class)
      .map(
        (MapFunction<Tuple3<Long, Long, Long>,EmbeddingRecord>) t -> {
          EmbeddingRecord edge = new EmbeddingRecord();
          edge.add(t.f0);
          edge.add(t.f1);
          edge.add(t.f2);
          return edge;
        }
      ).returns(EmbeddingRecord.class);
  }

  //------------------------------------------------------------------------------------------------
  // Queries
  //------------------------------------------------------------------------------------------------

  /**
   * Query 0: (a)-->(b)-->(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param  id type
   * @return query result
   */
  public static DataSet<EmbeddingRecord> q0(DataSet<EmbeddingRecord> edges) {
    return edges.join(edges)
      .where(new EmbeddingRecordKeyExtractor(Arrays.asList(2))).equalTo(new EmbeddingRecordKeyExtractor(Arrays.asList(0)))
      .with(new JoinEmbeddingRecords(Arrays.asList(0)));
  }

  /**
   * Query 1: (a)-->(b)-->(c)-->(d)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param  id type
   * @return query result
   */
  public static  DataSet<EmbeddingRecord> q1(DataSet<EmbeddingRecord> edges) {
    return edges.join(edges)
      .where(new EmbeddingRecordKeyExtractor(Arrays.asList(2))).equalTo(new EmbeddingRecordKeyExtractor(Arrays.asList(0)))
      .with(new JoinEmbeddingRecords(Arrays.asList(0)))
      .join(edges)
      .where(new EmbeddingRecordKeyExtractor(Arrays.asList(4))).equalTo(new EmbeddingRecordKeyExtractor(Arrays.asList(0)))
      .with(new JoinEmbeddingRecords(Arrays.asList(0)));
  }

  /**
   * Query 2: (a)-->(b)-->(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param  id type
   * @return query result
   */
  public static  DataSet<EmbeddingRecord> q2(DataSet<EmbeddingRecord> edges) {
    return edges.join(edges)
      .where(new EmbeddingRecordKeyExtractor(Arrays.asList(2,0))).equalTo(new EmbeddingRecordKeyExtractor(Arrays.asList(0,2)))
      .with(new JoinEmbeddingRecords(Arrays.asList(0, 2)));
  }

  /**
   * Query 3: (a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param  id type
   * @return query result
   */
  public static  DataSet<EmbeddingRecord> q3(DataSet<EmbeddingRecord> edges) {
    return edges.join(edges)
      .where(new EmbeddingRecordKeyExtractor(Arrays.asList(2,0))).equalTo(new EmbeddingRecordKeyExtractor(Arrays.asList(0,2)))
      .with(new JoinEmbeddingRecords(Arrays.asList(0)))
      .join(edges)
      .where(new EmbeddingRecordKeyExtractor(Arrays.asList(0))).equalTo(new EmbeddingRecordKeyExtractor(Arrays.asList(2)))
      .with(new JoinEmbeddingRecords(Arrays.asList(0)));
  }

  /**
   * Query 4: (d)-->(a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param  id type
   * @return query result
   */
  public static  DataSet<EmbeddingRecord> q4(DataSet<EmbeddingRecord> edges) {
    return edges.join(edges)
      .where(new EmbeddingRecordKeyExtractor(Arrays.asList(2,0))).equalTo(new EmbeddingRecordKeyExtractor(Arrays.asList(0,2)))
      .with(new JoinEmbeddingRecords(Arrays.asList(0, 2)))
      .join(edges)
      .where(new EmbeddingRecordKeyExtractor(Arrays.asList(0))).equalTo(new EmbeddingRecordKeyExtractor(Arrays.asList(2)))
      .with(new JoinEmbeddingRecords(Arrays.asList(0)))
      .join(edges)
      .where(new EmbeddingRecordKeyExtractor(Arrays.asList(0))).equalTo(new EmbeddingRecordKeyExtractor(Arrays.asList(2)))
      .with(new JoinEmbeddingRecords(Arrays.asList(2)));
  }

  /**
   * Query 5: (a)-->(b)-->(c)<--(d)<--(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param  id type
   * @return query result
   */
  public static  DataSet<EmbeddingRecord> q5(DataSet<EmbeddingRecord> edges) {

    DataSet<EmbeddingRecord> abc =  edges.join(edges)
      .where(new EmbeddingRecordKeyExtractor(Arrays.asList(2))).equalTo(new EmbeddingRecordKeyExtractor(Arrays.asList(0)))
      .with(new JoinEmbeddingRecords(Arrays.asList(0)));

    // self join
    return abc.join(abc)
      .where(new EmbeddingRecordKeyExtractor(Arrays.asList(0,4))).equalTo(new EmbeddingRecordKeyExtractor(Arrays.asList(0,4)))
      .with(new JoinEmbeddingRecords(Arrays.asList(0, 4)));
  }
}
