package org.gradoop.benchmark.patternmatching.cypher.expand;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.gradoop.benchmark.patternmatching.cypher.join.embeddings.Embedding;
import org.gradoop.benchmark.patternmatching.cypher.join.embeddings.IdEntry;


public class ExpandWithEmbedding {
  private DataSet<Tuple3<Long, Long, Long>> candidateEdges;
  private final int lowerBound;
  private final int upperBound;


  public ExpandWithEmbedding(DataSet<Tuple3<Long, Long, Long>> candidateEdges, int lowerBound, int upperBound) {
    this.candidateEdges = candidateEdges;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }


  public void evaluate() throws Exception {
    DataSet<Embedding> initialWorkingSet = preprocess();

    DataSet<Embedding> iterationResults = iterate(initialWorkingSet);

    System.out.println(postProcess(iterationResults).count());
  }

  private DataSet<Embedding> preprocess() {
    return candidateEdges.map(edge -> new Embedding(Lists.newArrayList(
      new IdEntry(edge.f0),
      new IdEntry(edge.f1),
      new IdEntry(edge.f2)
    ))).returns(Embedding.class);
  }

  private DataSet<Embedding> iterate(DataSet<Embedding> initialWorkingSet) {

    IterativeDataSet<Embedding> iteration = initialWorkingSet.iterate(upperBound);

    DataSet<Embedding> nextWorkingSet = iteration
      .filter(new FilterPreviousEmbedding())
      .join(candidateEdges)
        .where(new EmbeddingKeySelector())
        .equalTo(0)
        .with((embedding, edge) -> {
          Embedding newEmbedding = new Embedding(embedding.getEntries());
          newEmbedding.add(new IdEntry(edge.f0));
          newEmbedding.add(new IdEntry(edge.f1));
          newEmbedding.add(new IdEntry(edge.f2));
          return newEmbedding;
        }).returns(Embedding.class);

    DataSet<Embedding> solutionSet = nextWorkingSet.union(iteration);

    return iteration.closeWith(solutionSet, nextWorkingSet);
  }


  private DataSet<Embedding> postProcess(DataSet<Embedding> iterationResults) {
    return iterationResults.filter(new FilterLowerBound(lowerBound));
  }


  private class EmbeddingKeySelector implements KeySelector<Embedding, Long> {
    @Override
    public Long getKey(Embedding value) throws Exception {
      return value.get(value.size() -1).getId();
    }
  }

  private class FilterPreviousEmbedding extends RichFilterFunction<Embedding> {
    /**
     * super step
     */
    private int currentIteration;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      currentIteration = getIterationRuntimeContext().getSuperstepNumber() * 3;
    }

    @Override
    public boolean filter(Embedding embedding) {
      return embedding.size() >= currentIteration;
    }
  }

  private class FilterLowerBound extends RichFilterFunction<Embedding> {
    private int minSize;

    public FilterLowerBound(int lowerBound) {
      this.minSize = lowerBound * 3;
    }

    @Override
    public boolean filter(Embedding embedding) {
      return embedding.size() >= minSize;
    }
  }
}
