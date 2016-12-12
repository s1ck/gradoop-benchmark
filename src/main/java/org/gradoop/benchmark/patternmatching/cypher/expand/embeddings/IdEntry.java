package org.gradoop.benchmark.patternmatching.cypher.expand.embeddings;

public class IdEntry implements EmbeddingEntry {
  private long id;

  public IdEntry(Long id) {
    this.id = id;
  }

  public IdEntry() {
  }

  @Override
  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }
}
