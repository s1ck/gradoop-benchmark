package org.gradoop.benchmark.patternmatching.cypher.join.embeddings;

import java.util.ArrayList;
import java.util.List;


public class Embedding {
  private List<EmbeddingEntry> entries;

  public Embedding(List<EmbeddingEntry> entries) {
    this.entries = entries;
  }

  public Embedding(int size) {
    entries = new ArrayList<>(size);
  }

  public Embedding() {
  }

  public EmbeddingEntry get(int column) {
    return entries.get(column);
  }

  public List<EmbeddingEntry> getEntries() {
    return entries;
  }

  public void setEntries(List<EmbeddingEntry> entries) {
    this.entries = entries;
  }

  public void add(EmbeddingEntry entry) {
    entries.add(entry);
  }

  public int size() {
    return entries.size();
  }
}
