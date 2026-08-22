package io.akka.haystack.domain;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A deterministic order used to tie-break which of several equally-runnable components goes
 * next (SPEC-001 R8, §4.2).
 *
 * <p>Ported from {@code _topological_sort} (source: {@code core/pipeline/base.py:1497-1518}):
 * every component is assigned a rank by collapsing each strongly-connected component (every
 * cycle) into one node and topologically sorting that condensation, so all components in the
 * same cycle rank equally. The source uses a plain (non-lexicographic) topological sort for
 * this step and a separately lexicographic one for a graph with no cycle at all; this port
 * always breaks ties — both within one rank and, going further than the source specifies,
 * between independent ranks with no dependency between them — by name, which exactly matches
 * the source whenever the graph is acyclic (its own lexicographic branch) and is this port's
 * own deterministic choice, not the source's, only when two different cycles or chains are
 * mutually independent (§4.2 — the source leaves that order to `networkx`'s internal node
 * iteration, which is not a rule to copy, only a fact to not depend on).
 */
final class TopologicalOrder {
  private final Map<String, Integer> rankByComponent;
  private final List<String> orderedNames;

  private TopologicalOrder(Map<String, Integer> rankByComponent, List<String> orderedNames) {
    this.rankByComponent = rankByComponent;
    this.orderedNames = orderedNames;
  }

  /** Every component name, in this order's rank (then name) order. */
  List<String> orderedNames() {
    return orderedNames;
  }

  static TopologicalOrder of(PipelineGraph graph) {
    List<String> names = new ArrayList<>(graph.components().keySet());
    Map<String, List<String>> adjacency = new HashMap<>();
    for (String n : names) {
      adjacency.put(n, new ArrayList<>());
    }
    for (Connection c : graph.connections()) {
      adjacency.get(c.fromComponent()).add(c.toComponent());
    }

    List<Set<String>> sccs = tarjanStronglyConnectedComponents(names, adjacency);
    Map<String, Integer> sccIndexByComponent = new HashMap<>();
    for (int i = 0; i < sccs.size(); i++) {
      for (String member : sccs.get(i)) {
        sccIndexByComponent.put(member, i);
      }
    }

    List<Set<Integer>> condensedAdjacency = new ArrayList<>();
    for (int i = 0; i < sccs.size(); i++) {
      condensedAdjacency.add(new HashSet<>());
    }
    for (Connection c : graph.connections()) {
      int from = sccIndexByComponent.get(c.fromComponent());
      int to = sccIndexByComponent.get(c.toComponent());
      if (from != to) {
        condensedAdjacency.get(from).add(to);
      }
    }

    List<String> sccRepresentative = sccs.stream().map(scc -> scc.stream().sorted().findFirst().orElseThrow()).toList();
    List<Integer> condensationOrder =
        topologicalSortCondensation(sccs.size(), condensedAdjacency, sccRepresentative);
    Map<Integer, Integer> rankBySccIndex = new HashMap<>();
    for (int rank = 0; rank < condensationOrder.size(); rank++) {
      rankBySccIndex.put(condensationOrder.get(rank), rank);
    }

    Map<String, Integer> rankByComponent = new HashMap<>();
    for (String n : names) {
      rankByComponent.put(n, rankBySccIndex.get(sccIndexByComponent.get(n)));
    }
    List<String> orderedNames =
        names.stream()
            .sorted(Comparator.<String>comparingInt(rankByComponent::get).thenComparing(Comparator.naturalOrder()))
            .toList();
    return new TopologicalOrder(rankByComponent, orderedNames);
  }

  /** Picks, from {@code candidates}, the one this order ranks first (rank, then name). */
  String earliest(List<String> candidates) {
    return candidates.stream()
        .min(Comparator.<String>comparingInt(rankByComponent::get).thenComparing(Comparator.naturalOrder()))
        .orElseThrow();
  }

  private static List<Set<String>> tarjanStronglyConnectedComponents(
      List<String> names, Map<String, List<String>> adjacency) {
    Map<String, Integer> index = new HashMap<>();
    Map<String, Integer> lowlink = new HashMap<>();
    Set<String> onStack = new HashSet<>();
    Deque<String> stack = new ArrayDeque<>();
    List<Set<String>> result = new ArrayList<>();
    int[] counter = {0};

    for (String n : names) {
      if (!index.containsKey(n)) {
        strongConnect(n, adjacency, index, lowlink, onStack, stack, result, counter);
      }
    }
    return result;
  }

  private static void strongConnect(
      String v,
      Map<String, List<String>> adjacency,
      Map<String, Integer> index,
      Map<String, Integer> lowlink,
      Set<String> onStack,
      Deque<String> stack,
      List<Set<String>> result,
      int[] counter) {
    index.put(v, counter[0]);
    lowlink.put(v, counter[0]);
    counter[0]++;
    stack.push(v);
    onStack.add(v);

    for (String w : adjacency.get(v)) {
      if (!index.containsKey(w)) {
        strongConnect(w, adjacency, index, lowlink, onStack, stack, result, counter);
        lowlink.put(v, Math.min(lowlink.get(v), lowlink.get(w)));
      } else if (onStack.contains(w)) {
        lowlink.put(v, Math.min(lowlink.get(v), index.get(w)));
      }
    }

    if (lowlink.get(v).equals(index.get(v))) {
      Set<String> scc = new HashSet<>();
      String w;
      do {
        w = stack.pop();
        onStack.remove(w);
        scc.add(w);
      } while (!w.equals(v));
      result.add(scc);
    }
  }

  /**
   * Kahn's algorithm, but among all currently-available (zero-remaining-in-degree) nodes it
   * always picks the one whose representative name sorts first — a lexicographic topological
   * sort of the condensation, so the result is fully deterministic and, for an acyclic graph
   * (where every node is its own trivial SCC), is exactly {@code
   * networkx.lexicographical_topological_sort} (source's own acyclic-case algorithm).
   */
  private static List<Integer> topologicalSortCondensation(
      int nodeCount, List<Set<Integer>> adjacency, List<String> representative) {
    int[] inDegree = new int[nodeCount];
    for (Set<Integer> targets : adjacency) {
      for (int t : targets) {
        inDegree[t]++;
      }
    }
    TreeSet<Integer> available =
        new TreeSet<>(Comparator.<Integer, String>comparing(representative::get).thenComparingInt(i -> i));
    for (int i = 0; i < nodeCount; i++) {
      if (inDegree[i] == 0) {
        available.add(i);
      }
    }
    List<Integer> order = new ArrayList<>();
    while (!available.isEmpty()) {
      int n = available.pollFirst();
      order.add(n);
      for (int t : adjacency.get(n)) {
        if (--inDegree[t] == 0) {
          available.add(t);
        }
      }
    }
    return order;
  }
}
