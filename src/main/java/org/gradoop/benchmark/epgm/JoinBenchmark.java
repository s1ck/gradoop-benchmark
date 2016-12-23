package org.gradoop.benchmark.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.benchmark.epgm.tuples.ToEdgeByteArrayTuple;
import org.gradoop.benchmark.epgm.tuples.ToEdgePartialByteArrayTuple;
import org.gradoop.benchmark.epgm.tuples.ToEdgeTuple;
import org.gradoop.benchmark.epgm.tuples.ToVertexByteArrayTuple;
import org.gradoop.benchmark.epgm.tuples.ToVertexPartialByteArrayTuple;
import org.gradoop.benchmark.epgm.tuples.ToVertexTuple;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Performs
 *
 * (vertices |><| edges ON vId = sourceId) |><| vertices on targetId = vId
 *
 * on different graph representations.
 */
public class JoinBenchmark {

  public static void main(String[] args) throws Exception {

    String setting = args[0].toLowerCase();
    String inputPath = args[1];

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    long count;

    switch (setting) {
    case "pojo" : {
      count = runWithEPGMPojos(inputPath, env);
      break;
    }
    case "tuple" : {
      count = runWithEPGMTuples(inputPath, new ToVertexTuple(), new ToEdgeTuple(), env);
      break;
    }
    case "tuple_bytearray" : {
      count = runWithEPGMTuples(inputPath, new ToVertexByteArrayTuple(), new ToEdgeByteArrayTuple(), env);
      break;
    }
    case "tuple_partial_bytearray" : {
      count = runWithEPGMTuples(inputPath, new ToVertexPartialByteArrayTuple(), new ToEdgePartialByteArrayTuple(), env);
    }
    break;
    case "long" : {
      count = runWithIdOnly(inputPath,
        (s -> Tuple1.of(Long.parseLong(s.f0))),
        (s -> Tuple3.of(Long.parseLong(s.f0), Long.parseLong(s.f1), Long.parseLong(s.f2))),
        Long.class, env);
      break;
    }
    case "gradoopid" : {
      count = runWithIdOnly(inputPath,
        (s -> Tuple1.of(GradoopId.fromString(s.f0))),
        (s -> Tuple3.of(GradoopId.fromString(s.f0), GradoopId.fromString(s.f1), GradoopId.fromString(s.f2))),
        GradoopId.class, env);
      break;
    }
    default: throw new IllegalArgumentException("unsupported setting: " + setting);
    }

    long netRuntime = env.getLastJobExecutionResult().getNetRuntime();

    System.out.printf("computed %d triples in %d ms", count, netRuntime);
  }

  private static long runWithEPGMPojos(String inputPath, ExecutionEnvironment env) throws Exception {

    DataSource jsonDataSource = new JSONDataSource(
      inputPath + "graphs.json",
      inputPath + "nodes.json",
      inputPath + "edges.json",
      GradoopFlinkConfig.createConfig(env));

    DataSet<Vertex> vertices = jsonDataSource.getLogicalGraph().getVertices();
    DataSet<Edge> edges = jsonDataSource.getLogicalGraph().getEdges();

    DataSet<Tuple2<Tuple2<Vertex, Edge>, Vertex>> triples = vertices
      .join(edges)
      .where(new Id<>()).equalTo(new SourceId<>())
      .join(vertices)
      .where("f1.targetId").equalTo(new Id<>());

    return triples.count();
  }

  private static <T1 extends Tuple4, T2 extends Tuple6> long runWithEPGMTuples(String inputPath,
    MapFunction<Vertex, T1> vertexMapFunction,
    MapFunction<Edge, T2> edgeMapFunction,
    ExecutionEnvironment env) throws Exception {

    DataSource jsonDataSource = new JSONDataSource(
      inputPath + "graphs.json",
      inputPath + "nodes.json",
      inputPath + "edges.json",
      GradoopFlinkConfig.createConfig(env));

    DataSet<T1> vertices = jsonDataSource.getLogicalGraph().getVertices().map(vertexMapFunction);

    DataSet<T2> edges = jsonDataSource.getLogicalGraph().getEdges().map(edgeMapFunction);

    DataSet<Tuple2<Tuple2<T1, T2>, T1>> triples = vertices
      .join(edges)
      .where(0).equalTo(1)
      .join(vertices)
      .where("f1.f2").equalTo(0);

    return triples.count();
  }

  private static <T> long runWithIdOnly(String inputPath,
    MapFunction<Tuple1<String>, Tuple1<T>> vertexMapFunction,
    MapFunction<Tuple3<String, String, String>, Tuple3<T, T, T>> edgeMapFunction,
    Class<T> clazz,
    ExecutionEnvironment env) throws Exception {

    TypeInformation<T> typeInfo = TypeExtractor.getForClass(clazz);

    DataSet<Tuple1<T>> vertices = env
      .readCsvFile(inputPath + "vertices")
      .types(String.class)
      .map(vertexMapFunction)
      .returns(new TupleTypeInfo<>(typeInfo));
    DataSet<Tuple3<T, T, T>> edges = env
      .readCsvFile(inputPath + "edges")
      .types(String.class, String.class, String.class)
      .map(edgeMapFunction)
      .returns(new TupleTypeInfo<>(typeInfo, typeInfo, typeInfo));

    DataSet<Tuple2<Tuple2<Tuple1<T>, Tuple3<T, T, T>>, Tuple1<T>>> triples = vertices
      .join(edges)
      .where(0).equalTo(1)
      .join(vertices)
      .where("f0.f0").equalTo(0);

    return triples.count();
  }
}
