package org.gradoop.benchmark.patternmatching.cypher.join.embeddings;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.joining;


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

  public List<EmbeddingEntry> copyEntries() {
    return new ArrayList<>(entries);
  }

  public void add(EmbeddingEntry entry) {
    entries.add(entry);
  }

  public int size() {
    return entries.size();
  }

  @Override
  public String toString() {
    return "[" + entries.stream().map(x -> String.valueOf(x.getId())).collect(joining(", ")) + "]";
  }
}
