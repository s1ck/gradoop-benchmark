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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.id.GradoopId;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings("ALL")
public class ArrayBenchmark {

  public static class JoinLists<K>
    implements JoinFunction<K[], K[], K[]> {

    private final List<Integer> joinColumnsRight;

    @SuppressWarnings("unchecked")
    public JoinLists(List<Integer> joinColumnsRight) {
      this.joinColumnsRight = joinColumnsRight;
    }

    @Override
    public K[] join(K[] first, K[] second) throws Exception {
      K[] result = (K[]) Array.newInstance(
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

  public static class ArrayKeyExtractor<K> implements KeySelector<K[], String> {
    private final List<Integer> columns;

    public ArrayKeyExtractor(List<Integer> columns) {
      this.columns = columns;
    }

    @Override
    public String getKey(K[] value) throws Exception {
      String res = "";
      for(int i : columns) {
        res += "|" + value[i].toString();
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
      performQuery(readBasicTypeEdges(env, inputPath, Long.class), query, BasicTypeInfo.LONG_TYPE_INFO);
    } else if (idType.equals("gradoopid")) {
      performQuery(readGradoopIdEdges(env, inputPath), query, TypeExtractor.getForClass(GradoopId.class));
    }
  }

  //------------------------------------------------------------------------------------------------
  // Helper
  //------------------------------------------------------------------------------------------------

  private static <K> void performQuery(DataSet<K[]> edges, String query, TypeInformation<K> typeInfo) throws
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

  private static <K> DataSet<K[]> readBasicTypeEdges(ExecutionEnvironment env, String inputPath, Class<K> keyClazz) throws Exception {
    return env.readCsvFile(inputPath)
      .ignoreComments("#")
      .fieldDelimiter("\t")
      .types(keyClazz, keyClazz, keyClazz)
      .map(new MapFunction<Tuple3<K, K, K>, K[]>() {
        @Override
        public K[] map(Tuple3<K, K, K> t) throws Exception {
          K[] edge = (K[]) Array.newInstance(keyClazz,3);
          edge[0] = t.f0;
          edge[1] = t.f1;
          edge[2] = t.f2;
          return edge;
        }
      });
  }

  private static DataSet<GradoopId[]> readGradoopIdEdges(ExecutionEnvironment env, String inputPath) {
    return env.readTextFile(inputPath)
      .map((MapFunction<String, GradoopId[]>) line -> {
        String[] tokens = line.split("\t");
        return new GradoopId[]{GradoopId.fromString(tokens[0]), GradoopId.fromString(tokens[1]), GradoopId.fromString(tokens[2])};
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
  public static <K> DataSet<K[]> q0(DataSet<K[]> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(new ArrayKeyExtractor<K>(Arrays.asList(2))).equalTo(new ArrayKeyExtractor<K>(Arrays.asList(0)))
      .with(new JoinLists<K>(Arrays.asList(0)));
  }

  /**
   * Query 1: (a)-->(b)-->(c)-->(d)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<K[]> q1(DataSet<K[]> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(new ArrayKeyExtractor<K>(Arrays.asList(2))).equalTo(new ArrayKeyExtractor<K>(Arrays.asList(0)))
      .with(new JoinLists<K>(Arrays.asList(0)))
      .join(edges)
      .where(new ArrayKeyExtractor<K>(Arrays.asList(4))).equalTo(new ArrayKeyExtractor<K>(Arrays.asList(0)))
      .with(new JoinLists<K>(Arrays.asList(0)));
  }

  /**
   * Query 2: (a)-->(b)-->(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<K[]> q2(DataSet<K[]> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(new ArrayKeyExtractor<K>(Arrays.asList(2,0))).equalTo(new ArrayKeyExtractor<K>(Arrays.asList(0,2)))
      .with(new JoinLists<K>(Arrays.asList(0, 2)));
  }

  /**
   * Query 3: (a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<K[]> q3(DataSet<K[]> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(new ArrayKeyExtractor<K>(Arrays.asList(2,0))).equalTo(new ArrayKeyExtractor<K>(Arrays.asList(0,2)))
      .with(new JoinLists<K>(Arrays.asList(0)))
      .join(edges)
      .where(new ArrayKeyExtractor<K>(Arrays.asList(0))).equalTo(new ArrayKeyExtractor<K>(Arrays.asList(2)))
      .with(new JoinLists<K>(Arrays.asList(0)));
  }

  /**
   * Query 4: (d)-->(a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<K[]> q4(DataSet<K[]> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(new ArrayKeyExtractor<K>(Arrays.asList(2,0))).equalTo(new ArrayKeyExtractor<K>(Arrays.asList(0,2)))
      .with(new JoinLists<K>(Arrays.asList(0, 2)))
      .join(edges)
      .where(new ArrayKeyExtractor<K>(Arrays.asList(0))).equalTo(new ArrayKeyExtractor<K>(Arrays.asList(2)))
      .with(new JoinLists<K>(Arrays.asList(0)))
      .join(edges)
      .where(new ArrayKeyExtractor<K>(Arrays.asList(0))).equalTo(new ArrayKeyExtractor<K>(Arrays.asList(2)))
      .with(new JoinLists<K>(Arrays.asList(2)));
  }

  /**
   * Query 5: (a)-->(b)-->(c)<--(d)<--(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<K[]> q5(DataSet<K[]> edges, TypeInformation<K> typeInfo) {

    DataSet<K[]> abc =  edges.join(edges)
      .where(new ArrayKeyExtractor<K>(Arrays.asList(2))).equalTo(new ArrayKeyExtractor<K>(Arrays.asList(0)))
      .with(new JoinLists<K>(Arrays.asList(0)));

    // self join
    return abc.join(abc)
      .where(new ArrayKeyExtractor<K>(Arrays.asList(0,4))).equalTo(new ArrayKeyExtractor<K>(Arrays.asList(0,4)))
      .with(new JoinLists<K>(Arrays.asList(0, 4)));
  }
}
