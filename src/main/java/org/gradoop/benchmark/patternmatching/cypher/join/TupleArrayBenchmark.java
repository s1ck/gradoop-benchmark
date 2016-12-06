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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings("ALL")
public class TupleArrayBenchmark {

  public static class IdEntry<K> extends Tuple1<K> {
    public IdEntry(K value0) {
      super(value0);
    }

    public IdEntry() {
    }

    public K getId() {
      return f0;
    }

    @Override
    public String toString() {
      return f0.toString();
    }
  }

  public static class JoinLists<K>
    implements JoinFunction<IdEntry<K>[], IdEntry<K>[], IdEntry<K>[]> {

    private final List<Integer> joinColumnsRight;

    @SuppressWarnings("unchecked")
    public JoinLists(List<Integer> joinColumnsRight) {
      this.joinColumnsRight = joinColumnsRight;
    }

    @Override
    public IdEntry<K>[] join(IdEntry<K>[] first, IdEntry<K>[] second) throws Exception {
      IdEntry<K>[] result = (IdEntry<K>[]) Array.newInstance(
        first.getClass().getComponentType(),
        first.length + second.length - joinColumnsRight.size()
      );

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

  public static class TupleArrayKeyExtractor<K> implements KeySelector<IdEntry<K>[], String> {
    private final List<Integer> columns;

    public TupleArrayKeyExtractor(List<Integer> columns) {
      this.columns = columns;
    }

    @Override
    public String getKey(IdEntry<K>[] value) throws Exception {
      String res = "";
      for(int i : columns) {
        res += "|" + value[i].getId().toString();
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
      performQuery(readBasicTypeEdges(env, inputPath, Long.class), query);
    } else if (idType.equals("gradoopid")) {
      performQuery(readGradoopIdEdges(env, inputPath), query);
    }
  }

  //------------------------------------------------------------------------------------------------
  // Helper
  //------------------------------------------------------------------------------------------------

  private static <K> void performQuery(DataSet<IdEntry<K>[]> edges, String query) throws Exception {
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

  private static <K> DataSet<IdEntry<K>[]> readBasicTypeEdges(ExecutionEnvironment env,
    String inputPath, Class<K> keyClazz) throws Exception {
    return env.readCsvFile(inputPath)
      .ignoreComments("#")
      .fieldDelimiter("\t")
      .types(keyClazz, keyClazz, keyClazz)
      .map(new MapFunction<Tuple3<K, K, K>, IdEntry<K>[]>() {
        @Override
        public IdEntry<K>[] map(Tuple3<K, K, K> t) throws Exception {
          IdEntry<K>[] edge = (IdEntry<K>[]) new IdEntry[3];
          edge[0] = new IdEntry(t.f0);
          edge[1] = new IdEntry(t.f1);
          edge[2] = new IdEntry(t.f2);
          return edge;
        }
      });
  }

  private static DataSet<IdEntry<GradoopId>[]> readGradoopIdEdges(ExecutionEnvironment env,
    String inputPath) {
    return env.readTextFile(inputPath)
      .map((MapFunction<String, IdEntry<GradoopId>[]>) line -> {
        String[] tokens = line.split("\t");
        IdEntry<GradoopId>[] edge = (IdEntry<GradoopId>[]) new IdEntry[3];
        edge[0] = new IdEntry<>(GradoopId.fromString(tokens[0]));
        edge[1] = new IdEntry<>(GradoopId.fromString(tokens[1]));
        edge[2] = new IdEntry<>(GradoopId.fromString(tokens[2]));
        return edge;
      }).returns(new TypeHint<IdEntry<GradoopId>[]>(){}.getTypeInfo());
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
  public static <K> DataSet<IdEntry<K>[]> q0(DataSet<IdEntry<K>[]> edges) {
    return edges.join(edges)
      .where(new TupleArrayKeyExtractor<K>(Arrays.asList(2)))
      .equalTo(new TupleArrayKeyExtractor<K>(Arrays.asList(0)))
      .with(new JoinLists<K>(Arrays.asList(0)));
  }

  /**
   * Query 1: (a)-->(b)-->(c)-->(d)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<IdEntry<K>[]> q1(DataSet<IdEntry<K>[]> edges) {
    return edges
      .join(edges)
        .where(new TupleArrayKeyExtractor<K>(Arrays.asList(2)))
        .equalTo(new TupleArrayKeyExtractor<K>(Arrays.asList(0)))
        .with(new JoinLists<K>(Arrays.asList(0)))
      .join(edges)
        .where(new TupleArrayKeyExtractor<K>(Arrays.asList(4)))
        .equalTo(new TupleArrayKeyExtractor<K>(Arrays.asList(0)))
        .with(new JoinLists<K>(Arrays.asList(0)));
  }

  /**
   * Query 2: (a)-->(b)-->(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<IdEntry<K>[]> q2(DataSet<IdEntry<K>[]> edges) {
    return edges
      .join(edges)
        .where(new TupleArrayKeyExtractor<K>(Arrays.asList(2,0)))
        .equalTo(new TupleArrayKeyExtractor<K>(Arrays.asList(0,2)))
        .with(new JoinLists<K>(Arrays.asList(0, 2)));
  }

  /**
   * Query 3: (a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<IdEntry<K>[]> q3(DataSet<IdEntry<K>[]> edges) {
    return edges
      .join(edges)
        .where(new TupleArrayKeyExtractor<K>(Arrays.asList(2,0)))
        .equalTo(new TupleArrayKeyExtractor<K>(Arrays.asList(0,2)))
        .with(new JoinLists<K>(Arrays.asList(0)))
      .join(edges)
        .where(new TupleArrayKeyExtractor<K>(Arrays.asList(0)))
        .equalTo(new TupleArrayKeyExtractor<K>(Arrays.asList(2)))
        .with(new JoinLists<K>(Arrays.asList(0)));
  }

  /**
   * Query 4: (d)-->(a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<IdEntry<K>[]> q4(DataSet<IdEntry<K>[]> edges) {
    return edges
      .join(edges)
        .where(new TupleArrayKeyExtractor<K>(Arrays.asList(2,0)))
        .equalTo(new TupleArrayKeyExtractor<K>(Arrays.asList(0,2)))
        .with(new JoinLists<K>(Arrays.asList(0, 2)))
      .join(edges)
        .where(new TupleArrayKeyExtractor<K>(Arrays.asList(0)))
        .equalTo(new TupleArrayKeyExtractor<K>(Arrays.asList(2)))
        .with(new JoinLists<K>(Arrays.asList(0)))
      .join(edges)
        .where(new TupleArrayKeyExtractor<K>(Arrays.asList(0)))
        .equalTo(new TupleArrayKeyExtractor<K>(Arrays.asList(2)))
        .with(new JoinLists<K>(Arrays.asList(2)));
  }

  /**
   * Query 5: (a)-->(b)-->(c)<--(d)<--(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<IdEntry<K>[]> q5(DataSet<IdEntry<K>[]> edges) {

    DataSet<IdEntry<K>[]> abc =  edges
      .join(edges)
        .where(new TupleArrayKeyExtractor<K>(Arrays.asList(2)))
        .equalTo(new TupleArrayKeyExtractor<K>(Arrays.asList(0)))
        .with(new JoinLists<K>(Arrays.asList(0)));

    // self join
    return abc
      .join(abc)
        .where(new TupleArrayKeyExtractor<K>(Arrays.asList(0,4)))
        .equalTo(new TupleArrayKeyExtractor<K>(Arrays.asList(0,4)))
        .with(new JoinLists<K>(Arrays.asList(0, 4)));
  }
}