package org.gradoop.benchmark;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.s1ck.ldbc.LDBCToFlink;
import org.s1ck.ldbc.tuples.LDBCEdge;
import org.s1ck.ldbc.tuples.LDBCVertex;

public class LDBCConverter implements ProgramDescription {

  public static void main(String[] args) throws Exception {
    String inputPath = args[0];
    String outputPath = args[1];

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    final LDBCToFlink ldbcToFlink = new LDBCToFlink(inputPath, env);

    ldbcToFlink.getVertices()
      .map(new MapFunction<LDBCVertex, Tuple2<String, Integer>>() {
        public Tuple2<String, Integer> map(LDBCVertex ldbcVertex) throws
          Exception {
          return new Tuple2<String, Integer>(ldbcVertex.getLabel(), 1);
        }
      })
      .union(ldbcToFlink.getEdges().map(
        new MapFunction<LDBCEdge, Tuple2<String, Integer>>() {
          public Tuple2<String, Integer> map(LDBCEdge ldbcEdge) throws
            Exception {
            return new Tuple2<String, Integer>(ldbcEdge.getLabel(), 1);
          }
        }))
      .groupBy(0)
      .sum(1)
      .print();
  }

  public String getDescription() {
    return this.getClass().getName();
  }
}
