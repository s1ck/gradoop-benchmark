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
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings("ALL")
public class RecordBenchmark {

  public static class JoinRecords<K>
    implements JoinFunction<Record, Record, Record> {

    private final List<Integer> joinColumnsRight;

    @SuppressWarnings("unchecked")
    public JoinRecords(List<Integer> joinColumnsRight) {
      this.joinColumnsRight = joinColumnsRight;
    }

    @Override
    public Record join(Record first, Record second) throws Exception {
      Record result = new Record(first.getNumFields() + second.getNumFields() - joinColumnsRight.size());

      first.copyTo(result);

      int i = first.getNumFields();
      for(int j = 0; j < second.getNumFields(); j++) {
        if (!joinColumnsRight.contains(j)) {
          int[] src = new int[]{j};
          int[] tgt = new int[]{i++};
          result.copyFrom(second,src,tgt);
        }
      }
      return result;
    }
  }

  public static class RecordKeyExtractor implements KeySelector<Record, String> {
    private final List<Integer> columns;
  
    public RecordKeyExtractor(List<Integer> columns) {
      this.columns = columns;
    }
  
    @Override
    public String getKey(Record value) throws Exception {
      String res = "";
      for(int i : columns) {
        res += value.getField(i, LongValue.class).toString();
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

  private static <K> void performQuery(DataSet<Record> edges, String query) throws
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

  private static DataSet<Record> readEdges(ExecutionEnvironment env, String inputPath) throws Exception {
    return env.readCsvFile(inputPath)
      .ignoreComments("#")
      .fieldDelimiter("\t")
      .types(Long.class, Long.class, Long.class)
      .map(
        (MapFunction<Tuple3<Long, Long, Long>,Record>) t -> {
          Record edge = new Record(3);
          edge.setField(0,new LongValue(t.f0));
          edge.setField(1,new LongValue(t.f1));
          edge.setField(2,new LongValue(t.f2));
          return edge;
        }
      ).returns(Record.class);
  }

  //------------------------------------------------------------------------------------------------
  // Queries
  //------------------------------------------------------------------------------------------------

  /**
   * Query 0: (a)-->(b)-->(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static DataSet<Record> q0(DataSet<Record> edges) {
    return edges.join(edges)
      .where(new RecordKeyExtractor(Arrays.asList(2))).equalTo(new RecordKeyExtractor(Arrays.asList(0)))
      .with(new JoinRecords(Arrays.asList(0)));
  }

  /**
   * Query 1: (a)-->(b)-->(c)-->(d)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<Record> q1(DataSet<Record> edges) {
    return edges.join(edges)
      .where(new RecordKeyExtractor(Arrays.asList(2))).equalTo(new RecordKeyExtractor(Arrays.asList(0)))
      .with(new JoinRecords<K>(Arrays.asList(0)))
      .join(edges)
      .where(new RecordKeyExtractor(Arrays.asList(4))).equalTo(new RecordKeyExtractor(Arrays.asList(0)))
      .with(new JoinRecords<K>(Arrays.asList(0)));
  }

  /**
   * Query 2: (a)-->(b)-->(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<Record> q2(DataSet<Record> edges) {
    return edges.join(edges)
      .where(new RecordKeyExtractor(Arrays.asList(2,0))).equalTo(new RecordKeyExtractor(Arrays.asList(0,2)))
      .with(new JoinRecords<K>(Arrays.asList(0, 2)));
  }

  /**
   * Query 3: (a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<Record> q3(DataSet<Record> edges) {
    return edges.join(edges)
      .where(new RecordKeyExtractor(Arrays.asList(2,0))).equalTo(new RecordKeyExtractor(Arrays.asList(0,2)))
      .with(new JoinRecords<K>(Arrays.asList(0)))
      .join(edges)
      .where(new RecordKeyExtractor(Arrays.asList(0))).equalTo(new RecordKeyExtractor(Arrays.asList(2)))
      .with(new JoinRecords<K>(Arrays.asList(0)));
  }

  /**
   * Query 4: (d)-->(a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<Record> q4(DataSet<Record> edges) {
    return edges.join(edges)
      .where(new RecordKeyExtractor(Arrays.asList(2,0))).equalTo(new RecordKeyExtractor(Arrays.asList(0,2)))
      .with(new JoinRecords<K>(Arrays.asList(0, 2)))
      .join(edges)
      .where(new RecordKeyExtractor(Arrays.asList(0))).equalTo(new RecordKeyExtractor(Arrays.asList(2)))
      .with(new JoinRecords<K>(Arrays.asList(0)))
      .join(edges)
      .where(new RecordKeyExtractor(Arrays.asList(0))).equalTo(new RecordKeyExtractor(Arrays.asList(2)))
      .with(new JoinRecords<K>(Arrays.asList(2)));
  }

  /**
   * Query 5: (a)-->(b)-->(c)<--(d)<--(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<Record> q5(DataSet<Record> edges) {

    DataSet<Record> abc =  edges.join(edges)
      .where(new RecordKeyExtractor(Arrays.asList(2))).equalTo(new RecordKeyExtractor(Arrays.asList(0)))
      .with(new JoinRecords<K>(Arrays.asList(0)));

    // self join
    return abc.join(abc)
      .where(new RecordKeyExtractor(Arrays.asList(0,4))).equalTo(new RecordKeyExtractor(Arrays.asList(0,4)))
      .with(new JoinRecords<K>(Arrays.asList(0, 4)));
  }
}
