package org.gradoop.benchmark.patternmatching.cypher.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.benchmark.patternmatching.cypher.join.embeddings.Embedding;
import org.gradoop.benchmark.patternmatching.cypher.join.embeddings.IdEntry;

import java.util.Arrays;
import java.util.List;

/**
 * Created by max on 08.12.16.
 */
@SuppressWarnings("ALL")
public class EmbeddingBenchmark {
  public static class JoinEmbeddings implements JoinFunction<Embedding, Embedding, Embedding> {
    private final List<Integer> joinColumnsRight;

    @SuppressWarnings("unchecked")
    public JoinEmbeddings(List<Integer> joinColumnsRight) {
      this.joinColumnsRight = joinColumnsRight;
    }

    @Override
    public Embedding join(Embedding first, Embedding second) throws Exception {
      Embedding result = new Embedding(first.size() + second.size() - joinColumnsRight.size());

      for (int i=0; i< first.size(); i++) {
        result.add(first.get(i));
      }

      // take all except join fields from the second embedding
      for (int j = 0; j < second.size(); j++) {
        if (!joinColumnsRight.contains(j)) {
          result.add(second.get(j));
        }
      }
      return result;
    }
  }

  public static class EmbeddingKeyExtractor implements KeySelector<Embedding, String> {
    private final List<Integer> columns;

    public EmbeddingKeyExtractor(List<Integer> columns) {
      this.columns = columns;
    }

    @Override
    public String getKey(Embedding value) throws Exception {
      String res = "";
      for(int i : columns) {
        res += "|" + value.get(i).getId();
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

  private static  void performQuery(DataSet<Embedding> edges, String query) throws
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

  private static DataSet<Embedding> readEdges(ExecutionEnvironment env, String inputPath) throws Exception {
    return env.readCsvFile(inputPath)
      .ignoreComments("#")
      .fieldDelimiter("\t")
      .types(Long.class, Long.class, Long.class)
      .map(
        (MapFunction<Tuple3<Long, Long, Long>,Embedding>) t -> {
          Embedding edge = new Embedding(3);
          edge.add(new IdEntry(t.f0));
          edge.add(new IdEntry(t.f1));
          edge.add(new IdEntry(t.f2));
          return edge;
        }
      ).returns(Embedding.class);
  }

  //------------------------------------------------------------------------------------------------
  // Queries
  //------------------------------------------------------------------------------------------------

  /**
   * Query 0: (a)-->(b)-->(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @return query result
   */
  public static DataSet<Embedding> q0(DataSet<Embedding> edges) {
    return edges.join(edges)
      .where(new EmbeddingKeyExtractor(Arrays.asList(2)))
      .equalTo(new EmbeddingKeyExtractor(Arrays.asList(0)))
      .with(new JoinEmbeddings(Arrays.asList(0)));
  }

  /**
   * Query 1: (a)-->(b)-->(c)-->(d)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @return query result
   */
  public static  DataSet<Embedding> q1(DataSet<Embedding> edges) {
    return edges.join(edges)
      .where(new EmbeddingKeyExtractor(Arrays.asList(2)))
      .equalTo(new EmbeddingKeyExtractor(Arrays.asList(0)))
      .with(new JoinEmbeddings(Arrays.asList(0)))

      .join(edges)
      .where(new EmbeddingKeyExtractor(Arrays.asList(4)))
      .equalTo(new EmbeddingKeyExtractor(Arrays.asList(0)))
      .with(new JoinEmbeddings(Arrays.asList(0)));
  }

  /**
   * Query 2: (a)-->(b)-->(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @return query result
   */
  public static  DataSet<Embedding> q2(DataSet<Embedding> edges) {
    return edges.join(edges)
      .where(new EmbeddingKeyExtractor(Arrays.asList(2,0)))
      .equalTo(new EmbeddingKeyExtractor(Arrays.asList(0,2)))
      .with(new JoinEmbeddings(Arrays.asList(0, 2)));
  }

  /**
   * Query 3: (a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @return query result
   */
  public static  DataSet<Embedding> q3(DataSet<Embedding> edges) {
    return edges.join(edges)
      .where(new EmbeddingKeyExtractor(Arrays.asList(2,0)))
      .equalTo(new EmbeddingKeyExtractor(Arrays.asList(0,2)))
      .with(new JoinEmbeddings(Arrays.asList(0)))
      .join(edges)
      .where(new EmbeddingKeyExtractor(Arrays.asList(0)))
      .equalTo(new EmbeddingKeyExtractor(Arrays.asList(2)))
      .with(new JoinEmbeddings(Arrays.asList(0)));
  }

  /**
   * Query 4: (d)-->(a)-->(b)-->(a)<--(c)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @return query result
   */
  public static  DataSet<Embedding> q4(DataSet<Embedding> edges) {
    return edges.join(edges)
      .where(new EmbeddingKeyExtractor(Arrays.asList(2,0))).equalTo(new EmbeddingKeyExtractor(Arrays.asList(0,2)))
      .with(new JoinEmbeddings(Arrays.asList(0, 2)))
      .join(edges)
      .where(new EmbeddingKeyExtractor(Arrays.asList(0))).equalTo(new EmbeddingKeyExtractor(Arrays.asList(2)))
      .with(new JoinEmbeddings(Arrays.asList(0)))
      .join(edges)
      .where(new EmbeddingKeyExtractor(Arrays.asList(0))).equalTo(new EmbeddingKeyExtractor(Arrays.asList(2)))
      .with(new JoinEmbeddings(Arrays.asList(2)));
  }

  /**
   * Query 5: (a)-->(b)-->(c)<--(d)<--(a)
   *
   * @param edges triple data set (sourceId, edgeId, targetId)
   * @return query result
   */
  public static  DataSet<Embedding> q5(DataSet<Embedding> edges) {

    DataSet<Embedding> abc =  edges.join(edges)
      .where(new EmbeddingKeyExtractor(Arrays.asList(2))).equalTo(new EmbeddingKeyExtractor(Arrays.asList(0)))
      .with(new JoinEmbeddings(Arrays.asList(0)));

    // self join
    return abc.join(abc)
      .where(new EmbeddingKeyExtractor(Arrays.asList(0,4))).equalTo(new EmbeddingKeyExtractor(Arrays.asList(0,4)))
      .with(new JoinEmbeddings(Arrays.asList(0, 4)));
  }
}
