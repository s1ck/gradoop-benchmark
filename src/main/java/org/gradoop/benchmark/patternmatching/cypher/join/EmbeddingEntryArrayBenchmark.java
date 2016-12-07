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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.benchmark.patternmatching.cypher.join.embeddings.EmbeddingEntry;
import org.gradoop.benchmark.patternmatching.cypher.join.embeddings.IdEntry;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings("ALL")
public class EmbeddingEntryArrayBenchmark {

  public static class JoinLists<K>
    implements JoinFunction<EmbeddingEntry[], EmbeddingEntry[], EmbeddingEntry[]> {

    private final List<Integer> joinColumnsRight;

    @SuppressWarnings("unchecked")
    public JoinLists(List<Integer> joinColumnsRight) {
      this.joinColumnsRight = joinColumnsRight;
    }

    @Override
    public EmbeddingEntry[] join(EmbeddingEntry[] first, EmbeddingEntry[] second) throws Exception {
      EmbeddingEntry[] result =
        new EmbeddingEntry[first.length + second.length - joinColumnsRight.size()];

      System.arraycopy(first,0,result,0,first.length);

      int i = first.length;
      // take all except join fields from the second embedding
      for (int j = 0; j < second.length; j++) {
        if (!joinColumnsRight.contains(j)) {
          result[i++] = second[j];
        }
      }
      return result;
    }
  }

  public static class EmbeddingEntryKeyExtractor implements KeySelector<EmbeddingEntry[], String> {
    private final List<Integer> columns;

    public EmbeddingEntryKeyExtractor(List<Integer> columns) {
      this.columns = columns;
    }

    @Override
    public String getKey(EmbeddingEntry[] value) throws Exception {
      String res = "";
      for(int i : columns) {
        res += "|" + Arrays.toString(value[i].getId());
      }
      return res;
    }
  }

  /**
   * Run with
   *
   * run -c org.gradoop.examples.FlinkGenericTupleTest gradoop.jar [long|gradoopid] inputPath q[0-n] ObjectReuse{true|false}
   *
   * @param args arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    String idType = args[0].toLowerCase();
    String inputPath = args[1];
    String query = args[2];
    boolean objectReuse = Boolean.parseBoolean(args[3]);

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    if(objectReuse) {
      env.getConfig().enableObjectReuse();
    }

    if (idType.equals("long")) {
      performQuery(readLongTypeEdges(env, inputPath), query);
    } else if (idType.equals("gradoopid")) {
      performQuery(readGradoopIdEdges(env, inputPath), query);
    }
  }

  //------------------------------------------------------------------------------------------------
  // Helper
  //------------------------------------------------------------------------------------------------

  private static <K> void performQuery(DataSet<EmbeddingEntry[]> edges, String query) throws Exception {
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

  private static DataSet<EmbeddingEntry[]> readLongTypeEdges(ExecutionEnvironment env, 
    String inputPath) throws Exception {
    return env.readCsvFile(inputPath)
      .ignoreComments("#")
      .fieldDelimiter("\t")
      .types(Long.class, Long.class, Long.class)
      .map(new MapFunction<Tuple3<Long, Long, Long>, EmbeddingEntry[]>() {
        @Override
        public EmbeddingEntry[] map(Tuple3<Long, Long, Long> t) throws Exception {
          return new EmbeddingEntry[] {
            new IdEntry(t.f0), new IdEntry(t.f1), new IdEntry(t.f2)
          };  
        }
      });
  }

  private static DataSet<EmbeddingEntry[]> readGradoopIdEdges(ExecutionEnvironment env,
    String inputPath) {
    return env.readTextFile(inputPath)
      .map((MapFunction<String, EmbeddingEntry[]>) line -> {
        String[] tokens = line.split("\t");
          return new EmbeddingEntry[] {
            new IdEntry(GradoopId.fromString(tokens[0])),
            new IdEntry(GradoopId.fromString(tokens[1])),
            new IdEntry(GradoopId.fromString(tokens[2]))
          };
      }).returns(new TypeHint<EmbeddingEntry[]>(){}.getTypeInfo());
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
  public static <K> DataSet<EmbeddingEntry[]> q0(DataSet<EmbeddingEntry[]> edges) {
    return edges.join(edges)
      .where(new EmbeddingEntryKeyExtractor(Arrays.asList(2)))
      .equalTo(new EmbeddingEntryKeyExtractor(Arrays.asList(0)))
      .with(new JoinLists<K>(Arrays.asList(0)));
  }

  /**
   * Query 1: (a)-->(b)-->(c)-->(d)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<EmbeddingEntry[]> q1(DataSet<EmbeddingEntry[]> edges) {
    return edges
      .join(edges)
        .where(new EmbeddingEntryKeyExtractor(Arrays.asList(2)))
        .equalTo(new EmbeddingEntryKeyExtractor(Arrays.asList(0)))
        .with(new JoinLists<K>(Arrays.asList(0)))
      .join(edges)
        .where(new EmbeddingEntryKeyExtractor(Arrays.asList(4)))
        .equalTo(new EmbeddingEntryKeyExtractor(Arrays.asList(0)))
        .with(new JoinLists<K>(Arrays.asList(0)));
  }

  /**
   * Query 2: (a)-->(b)-->(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<EmbeddingEntry[]> q2(DataSet<EmbeddingEntry[]> edges) {
    return edges
      .join(edges)
        .where(new EmbeddingEntryKeyExtractor(Arrays.asList(2,0)))
        .equalTo(new EmbeddingEntryKeyExtractor(Arrays.asList(0,2)))
        .with(new JoinLists<K>(Arrays.asList(0, 2)));
  }

  /**
   * Query 3: (a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<EmbeddingEntry[]> q3(DataSet<EmbeddingEntry[]> edges) {
    return edges
      .join(edges)
        .where(new EmbeddingEntryKeyExtractor(Arrays.asList(2,0)))
        .equalTo(new EmbeddingEntryKeyExtractor(Arrays.asList(0,2)))
        .with(new JoinLists<K>(Arrays.asList(0)))
      .join(edges)
        .where(new EmbeddingEntryKeyExtractor(Arrays.asList(0)))
        .equalTo(new EmbeddingEntryKeyExtractor(Arrays.asList(2)))
        .with(new JoinLists<K>(Arrays.asList(0)));
  }

  /**
   * Query 4: (d)-->(a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<EmbeddingEntry[]> q4(DataSet<EmbeddingEntry[]> edges) {
    return edges
      .join(edges)
        .where(new EmbeddingEntryKeyExtractor(Arrays.asList(2,0)))
        .equalTo(new EmbeddingEntryKeyExtractor(Arrays.asList(0,2)))
        .with(new JoinLists<K>(Arrays.asList(0, 2)))
      .join(edges)
        .where(new EmbeddingEntryKeyExtractor(Arrays.asList(0)))
        .equalTo(new EmbeddingEntryKeyExtractor(Arrays.asList(2)))
        .with(new JoinLists<K>(Arrays.asList(0)))
      .join(edges)
        .where(new EmbeddingEntryKeyExtractor(Arrays.asList(0)))
        .equalTo(new EmbeddingEntryKeyExtractor(Arrays.asList(2)))
        .with(new JoinLists<K>(Arrays.asList(2)));
  }

  /**
   * Query 5: (a)-->(b)-->(c)<--(d)<--(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<EmbeddingEntry[]> q5(DataSet<EmbeddingEntry[]> edges) {

    DataSet<EmbeddingEntry[]> abc =  edges
      .join(edges)
        .where(new EmbeddingEntryKeyExtractor(Arrays.asList(2)))
        .equalTo(new EmbeddingEntryKeyExtractor(Arrays.asList(0)))
        .with(new JoinLists<K>(Arrays.asList(0)));

    // self join
    return abc
      .join(abc)
        .where(new EmbeddingEntryKeyExtractor(Arrays.asList(0,4)))
        .equalTo(new EmbeddingEntryKeyExtractor(Arrays.asList(0,4)))
        .with(new JoinLists<K>(Arrays.asList(0, 4)));
  }
}