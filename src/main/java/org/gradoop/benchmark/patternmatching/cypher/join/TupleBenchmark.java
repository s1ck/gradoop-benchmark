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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.benchmark.gradoopid.GradoopIdTuple4;

import java.util.Arrays;
import java.util.List;

public class TupleBenchmark {

  public static class JoinTuples<K, T1 extends Tuple, T2 extends Tuple, T3 extends Tuple>
    implements JoinFunction<T1, T2, T3> {

    private final T3 result;

    private final List<Integer> joinColumnsRight;

    @SuppressWarnings("unchecked")
    public JoinTuples(int outputArity, List<Integer> joinColumnsRight) {
      this.joinColumnsRight = joinColumnsRight;

      switch(outputArity) {
      case 1:
        result = (T3) new Tuple1<K>();
        break;
      case 2:
        result = (T3) new Tuple2<K, K>();
        break;
      case 3:
        result = (T3) new Tuple3<K, K, K>();
        break;
      case 4:
        result = (T3) new Tuple4<K, K, K, K>();
        break;
      case 5:
        result = (T3) new Tuple5<K, K, K, K, K>();
        break;
      case 6:
        result = (T3) new Tuple6<K, K, K, K, K, K>();
        break;
      case 7:
        result = (T3) new Tuple7<K, K, K, K, K, K, K>();
        break;
      case 8:
        result = (T3) new Tuple8<K, K, K, K, K, K, K, K>();
        break;
      default: throw new IndexOutOfBoundsException();
      }
    }

    @Override
    public T3 join(T1 first, T2 second) throws Exception {
      int i;
      // take all fields from the first embedding
      for (i = 0; i < first.getArity(); i++) {
        result.setField(first.getField(i), i);
      }
      // take all except join fields from the second embedding
      for (int j = 0; j < second.getArity(); j++) {
        if (!joinColumnsRight.contains(j)) {
          result.setField(second.getField(j), i++);
        }
      }
      return result;
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
      performQuery(readBasicTypeEdges(inputPath, Long.class, env), query, BasicTypeInfo.LONG_TYPE_INFO);
    } else if (idType.equals("gradoopid")) {
      performQuery(readGradoopIdTupleEdges(inputPath, env), query, TupleTypeInfo.of(GradoopIdTuple4.class));
    }
  }

  //------------------------------------------------------------------------------------------------
  // Helper
  //------------------------------------------------------------------------------------------------

  private static <K> void performQuery(DataSet<Tuple3<K, K, K>> edges, String query, TypeInformation<K> typeInfo) throws
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

  private static <K> DataSet<Tuple3<K, K, K>> readBasicTypeEdges(String inputPath, Class<K> keyClazz, ExecutionEnvironment env) throws Exception {
    return env.readCsvFile(inputPath)
      .ignoreComments("#")
      .fieldDelimiter("\t")
      .types(keyClazz, keyClazz, keyClazz);
  }

  private static DataSet<Tuple3<GradoopIdTuple4, GradoopIdTuple4, GradoopIdTuple4>> readGradoopIdTupleEdges(String inputPath, ExecutionEnvironment env) {

    return env.readTextFile(inputPath)
      .map(new MapFunction<String, Tuple3<GradoopIdTuple4, GradoopIdTuple4, GradoopIdTuple4>>() {
        @Override
        public Tuple3<GradoopIdTuple4, GradoopIdTuple4, GradoopIdTuple4> map(String line) throws
          Exception {
          String[] tokens = line.split("\t");
          return Tuple3
            .of(GradoopIdTuple4.fromString(tokens[0]), GradoopIdTuple4.fromString(tokens[1]),
              GradoopIdTuple4

                .fromString(tokens[2]));
        }
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
  public static <K> DataSet<Tuple5<K, K, K, K, K>> q0(DataSet<Tuple3<K, K, K>> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(2).equalTo(0)
      .with(new JoinTuples<K, Tuple3<K, K, K>, Tuple3<K, K, K>, Tuple5<K, K, K, K, K>>(5, Arrays.asList(0)))
      .returns(new TupleTypeInfo<>(typeInfo, typeInfo, typeInfo, typeInfo, typeInfo))
      .withForwardedFieldsFirst("f0;f1;f2")
      .withForwardedFieldsSecond("f1->f3;f2->f4");
  }

  /**
   * Query 1: (a)-->(b)-->(c)-->(d)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<Tuple7<K, K, K, K, K, K, K>> q1(DataSet<Tuple3<K, K, K>> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(2).equalTo(0)
      .with(new JoinTuples<K, Tuple3<K, K, K>, Tuple3<K, K, K>, Tuple5<K, K, K, K, K>>(5, Arrays.asList(0)))
      .returns(new TupleTypeInfo<>(typeInfo, typeInfo, typeInfo, typeInfo, typeInfo))
      .withForwardedFieldsFirst("f0;f1;f2")
      .withForwardedFieldsSecond("f1->f3;f2->f4")
      .join(edges)
      .where(4).equalTo(0)
      .with(new JoinTuples<K, Tuple5<K, K, K, K, K>, Tuple3<K, K, K>, Tuple7<K, K, K, K, K, K, K>>(7, Arrays.asList(0)))
      .returns(new TupleTypeInfo<>(typeInfo, typeInfo, typeInfo, typeInfo, typeInfo, typeInfo, typeInfo))
      .withForwardedFieldsFirst("f0;f1;f2;f3;f4")
      .withForwardedFieldsSecond("f1->f5;f2->f6");
  }

  /**
   * Query 2: (a)-->(b)-->(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<Tuple4<K, K, K, K>> q2(DataSet<Tuple3<K, K, K>> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(2, 0).equalTo(0, 2)
      .with(new JoinTuples<K, Tuple3<K, K, K>, Tuple3<K, K, K>, Tuple4<K, K, K, K>>(4, Arrays.asList(0, 2)))
      .returns(new TupleTypeInfo<>(typeInfo, typeInfo, typeInfo, typeInfo))
      .withForwardedFieldsFirst("f0;f1;f2")
      .withForwardedFieldsSecond("f1->f3");
  }

  /**
   * Query 3: (a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<Tuple6<K, K, K, K, K, K>> q3(DataSet<Tuple3<K, K, K>> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(2, 0).equalTo(0, 2)
      .with(new JoinTuples<K, Tuple3<K, K, K>, Tuple3<K, K, K>, Tuple4<K, K, K, K>>(4, Arrays.asList(0, 2)))
      .returns(new TupleTypeInfo<>(typeInfo, typeInfo, typeInfo, typeInfo))
      .withForwardedFieldsFirst("f0;f1;f2")
      .withForwardedFieldsSecond("f1->f3")
      .join(edges)
      .where(0).equalTo(2)
      .with(new JoinTuples<K, Tuple4<K, K, K, K>, Tuple3<K, K, K>, Tuple6<K, K, K, K, K, K>>(6, Arrays.asList(0)))
      .returns(new TupleTypeInfo<>(typeInfo, typeInfo, typeInfo, typeInfo, typeInfo, typeInfo))
      .withForwardedFieldsFirst("f0;f1;f2;f3")
      .withForwardedFieldsSecond("f0->f4;f1->f5");
  }

  /**
   * Query 4: (d)-->(a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<Tuple8<K, K, K, K, K, K, K, K>> q4(DataSet<Tuple3<K, K, K>> edges, TypeInformation<K> typeInfo) {
    return edges.join(edges)
      .where(2, 0).equalTo(0, 2)
      .with(new JoinTuples<K, Tuple3<K, K, K>, Tuple3<K, K, K>, Tuple4<K, K, K, K>>(4, Arrays.asList(0, 2)))
      .returns(new TupleTypeInfo<>(typeInfo, typeInfo, typeInfo, typeInfo))
      .withForwardedFieldsFirst("f0;f1;f2")
      .withForwardedFieldsSecond("f1->f3")
      .join(edges)
      .where(0).equalTo(2)
      .with(new JoinTuples<K, Tuple4<K, K, K, K>, Tuple3<K, K, K>, Tuple6<K, K, K, K, K, K>>(6, Arrays.asList(0)))
      .returns(new TupleTypeInfo<>(typeInfo, typeInfo, typeInfo, typeInfo, typeInfo, typeInfo))
      .withForwardedFieldsFirst("f0;f1;f2;f3")
      .withForwardedFieldsSecond("f0->f4;f1->f5")
      .join(edges)
      .where(0).equalTo(2)
      .with(new JoinTuples<K, Tuple6<K, K, K, K, K, K>, Tuple3<K, K, K>, Tuple8<K, K, K, K, K, K, K, K>>(8, Arrays.asList(2)))
      .returns(new TupleTypeInfo<>(typeInfo, typeInfo, typeInfo, typeInfo, typeInfo, typeInfo, typeInfo, typeInfo))
      .withForwardedFieldsFirst("f0;f1;f2;f3;f4;f5")
      .withForwardedFieldsSecond("f0->f6;f1->f7");
  }

  /**
   * Query 5: (a)-->(b)-->(c)<--(d)<--(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @param <K> id type
   * @return query result
   */
  public static <K> DataSet<Tuple8<K, K, K, K, K, K, K, K>> q5(DataSet<Tuple3<K, K, K>> edges, TypeInformation<K> typeInfo) {

    DataSet<Tuple5<K, K, K, K, K>> abc =  edges.join(edges)
      .where(2).equalTo(0)
      .with(new JoinTuples<K, Tuple3<K, K, K>, Tuple3<K, K, K>, Tuple5<K, K, K, K, K>>(5, Arrays.asList(0)))
      .returns(new TupleTypeInfo<>(typeInfo, typeInfo, typeInfo, typeInfo, typeInfo))
      .withForwardedFieldsFirst("f0;f1;f2")
      .withForwardedFieldsSecond("f1->f3;f2->f4");

    // self join
    return abc.join(abc)
      .where(0, 4).equalTo(0, 4)
      .with(new JoinTuples<K, Tuple5<K, K, K, K, K>, Tuple5<K, K, K, K, K>, Tuple8<K, K, K, K, K, K, K, K>>(8, Arrays.asList(0, 4)))
      .returns(new TupleTypeInfo<>(typeInfo, typeInfo, typeInfo, typeInfo, typeInfo, typeInfo, typeInfo, typeInfo))
      .withForwardedFieldsFirst("f0;f1;f2;f3;f4")
      .withForwardedFieldsSecond("f1->f5;f2->f6;f3->f7");
  }
}
