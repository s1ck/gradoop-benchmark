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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings("ALL")
public class ListBenchmark {

  public static class JoinLists<K>
    implements JoinFunction<List<K>, List<K>, List<K>> {

    private final List<Integer> joinColumnsRight;

    @SuppressWarnings("unchecked")
    public JoinLists(List<Integer> joinColumnsRight) {
      this.joinColumnsRight = joinColumnsRight;
    }

    @Override
    public List<K> join(List<K> first, List<K> second) throws Exception {
      // take all except join fields from the second embedding
      for (int j = 0; j < second.size(); j++) {
        if (!joinColumnsRight.contains(j)) {
          first.add(second.get(j));
        }
      }
      return first;
    }
  }

  public static class ListKeyExtractor<K> implements KeySelector<List<K>, String> {
    private final List<Integer> columns;

    public ListKeyExtractor(List<Integer> columns) {
      this.columns = columns;
    }

    @Override
    public String getKey(List<K> value) throws Exception {
      String res = "";
      for(int i : columns) {
        res += value.get(i).toString();
      }
      return res;
    }
  }

  /**
   * Run with
   *
   * run -c org.gradoop.examples.FlinkGenericTupleTest gradoop.jar [long|gradoopid] inputPath q[0-n]
   *
   * @param args arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    String idType = args[0].toLowerCase();
    String inputPath = args[1];
    String query = args[2];

    if (idType.equals("long")) {
      performQuery(readBasicTypeEdges(inputPath, Long.class), query, BasicTypeInfo.LONG_TYPE_INFO);
    } else if (idType.equals("gradoopid")) {
      performQuery(readGradoopIdEdges(inputPath), query, TypeExtractor.getForClass(GradoopId.class));
    }
  }

  //------------------------------------------------------------------------------------------------
  // Helper
  //------------------------------------------------------------------------------------------------

  private static <K> void performQuery(DataSet<List<K>> edges, String query, TypeInformation<K> typeInfo) throws
    Exception {
    switch (query) {
    case "q0" : q0(edges, typeInfo).count(); break;
    case "q1" : q1(edges, typeInfo).count(); break;
    case "q2" : q2(edges, typeInfo).count(); break;
    case "q3" : q3(edges, typeInfo).count(); break;
    case "q4" : q4(edges, typeInfo).count(); break;
    case "q5" : q5(edges, typeInfo).count(); break;
    default: throw new IllegalArgumentException(query + " not implemented");
    }
  }

  private static <K> DataSet<List<K>> readBasicTypeEdges(String inputPath, Class<K> keyClazz) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    return env.readCsvFile(inputPath)
      .ignoreComments("#")
      .fieldDelimiter("\t")
      .types(keyClazz, keyClazz, keyClazz)
      .map(
        (MapFunction<Tuple3<K,K,K>,List<K>>) t -> Lists.newArrayList(t.f0, t.f1, t.f2)
      ).returns(new TypeHint<List<K>>(){}.getTypeInfo());
  }

  private static DataSet<List<GradoopId>> readGradoopIdEdges(String inputPath) {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    TypeInformation<GradoopId> idType = TypeExtractor.getForClass(GradoopId.class);
    TypeInformation<Tuple3<GradoopId, GradoopId, GradoopId>> tupleType = new TupleTypeInfo<>(idType, idType, idType);

    return env.readTextFile(inputPath)
      .map((MapFunction<String, List<GradoopId>>) line -> {
        String[] tokens = line.split("\t");
        return Lists.newArrayList(GradoopId.fromString(tokens[0]), GradoopId.fromString(tokens[1]), GradoopId.fromString(tokens[2]));
      });
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
  public static <K> DataSet<List<K>> q0(DataSet<List<K>> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(new ListKeyExtractor<K>(Arrays.asList(2))).equalTo(new ListKeyExtractor<K>(Arrays.asList(0)))
      .with(new JoinLists<K>(Arrays.asList(0)));
  }

  /**
   * Query 1: (a)-->(b)-->(c)-->(d)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<List<K>> q1(DataSet<List<K>> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(new ListKeyExtractor<K>(Arrays.asList(2))).equalTo(new ListKeyExtractor<K>(Arrays.asList(0)))
      .with(new JoinLists<K>(Arrays.asList(0)))
      .join(edges)
      .where(new ListKeyExtractor<K>(Arrays.asList(4))).equalTo(new ListKeyExtractor<K>(Arrays.asList(0)))
      .with(new JoinLists<K>(Arrays.asList(0)));
  }

  /**
   * Query 2: (a)-->(b)-->(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<List<K>> q2(DataSet<List<K>> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(new ListKeyExtractor<K>(Arrays.asList(2,0))).equalTo(new ListKeyExtractor<K>(Arrays.asList(0,2)))
      .with(new JoinLists<K>(Arrays.asList(0, 2)));
  }

  /**
   * Query 3: (a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<List<K>> q3(DataSet<List<K>> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(new ListKeyExtractor<K>(Arrays.asList(2,0))).equalTo(new ListKeyExtractor<K>(Arrays.asList(0,2)))
      .with(new JoinLists<K>(Arrays.asList(0)))
      .join(edges)
      .where(new ListKeyExtractor<K>(Arrays.asList(0))).equalTo(new ListKeyExtractor<K>(Arrays.asList(2)))
      .with(new JoinLists<K>(Arrays.asList(0)));
  }

  /**
   * Query 4: (d)-->(a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<List<K>> q4(DataSet<List<K>> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(new ListKeyExtractor<K>(Arrays.asList(2,0))).equalTo(new ListKeyExtractor<K>(Arrays.asList(0,2)))
      .with(new JoinLists<K>(Arrays.asList(0, 2)))
      .join(edges)
      .where(new ListKeyExtractor<K>(Arrays.asList(0))).equalTo(new ListKeyExtractor<K>(Arrays.asList(2)))
      .with(new JoinLists<K>(Arrays.asList(0)))
      .join(edges)
      .where(new ListKeyExtractor<K>(Arrays.asList(0))).equalTo(new ListKeyExtractor<K>(Arrays.asList(2)))
      .with(new JoinLists<K>(Arrays.asList(2)));
  }

  /**
   * Query 5: (a)-->(b)-->(c)<--(d)<--(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<List<K>> q5(DataSet<List<K>> edges, TypeInformation<K> typeInfo) {

    DataSet<List<K>> abc =  edges.join(edges)
      .where(new ListKeyExtractor<K>(Arrays.asList(2))).equalTo(new ListKeyExtractor<K>(Arrays.asList(0)))
      .with(new JoinLists<K>(Arrays.asList(0)));

    // self join
    return abc.join(abc)
      .where(new ListKeyExtractor<K>(Arrays.asList(0,4))).equalTo(new ListKeyExtractor<K>(Arrays.asList(0,4)))
      .with(new JoinLists<K>(Arrays.asList(0, 4)));
  }
}
