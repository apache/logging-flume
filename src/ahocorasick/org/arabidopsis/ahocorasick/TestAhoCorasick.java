/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.arabidopsis.ahocorasick;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import junit.framework.TestCase;

/**
 * Junit test cases for AhoCorasick.
 */
@SuppressWarnings("unchecked")
public class TestAhoCorasick extends TestCase {

  private AhoCorasick tree;

  public void setUp() {
    this.tree = new AhoCorasick();
  }

  public void testConstruction() {
    tree.add("hello".getBytes(), "hello".getBytes());
    tree.add("hi".getBytes(), "hi".getBytes());
    tree.prepare();

    State s0 = tree.getRoot();
    State s1 = s0.get((byte) 'h');
    State s2 = s1.get((byte) 'e');
    State s3 = s2.get((byte) 'l');
    State s4 = s3.get((byte) 'l');
    State s5 = s4.get((byte) 'o');
    State s6 = s1.get((byte) 'i');
    
    assertTrue(s6 != null);

    assertEquals(s0, s1.getFail());
    assertEquals(s0, s2.getFail());
    assertEquals(s0, s3.getFail());
    assertEquals(s0, s4.getFail());
    assertEquals(s0, s5.getFail());
    assertEquals(s0, s6.getFail());

    assertEquals(0, s0.getOutputs().size());
    assertEquals(0, s1.getOutputs().size());
    assertEquals(0, s2.getOutputs().size());
    assertEquals(0, s3.getOutputs().size());
    assertEquals(0, s4.getOutputs().size());
    assertEquals(1, s5.getOutputs().size());
    assertEquals(1, s6.getOutputs().size());    
  }

  public void testExample() {
    tree.add("he".getBytes(), "he".getBytes());
    tree.add("she".getBytes(), "she".getBytes());
    tree.add("his".getBytes(), "his".getBytes());
    tree.add("hers".getBytes(), "hers".getBytes());
    assertEquals(10, tree.getRoot().size());
    tree.prepare(); // after prepare, we can't call size()
    State s0 = tree.getRoot();
    State s1 = s0.get((byte) 'h');
    State s2 = s1.get((byte) 'e');

    State s3 = s0.get((byte) 's');
    State s4 = s3.get((byte) 'h');
    State s5 = s4.get((byte) 'e');

    State s6 = s1.get((byte) 'i');
    State s7 = s6.get((byte) 's');

    State s8 = s2.get((byte) 'r');
    State s9 = s8.get((byte) 's');

    assertEquals(s0, s1.getFail());
    assertEquals(s0, s2.getFail());
    assertEquals(s0, s3.getFail());
    assertEquals(s0, s6.getFail());
    assertEquals(s0, s8.getFail());

    assertEquals(s1, s4.getFail());
    assertEquals(s2, s5.getFail());
    assertEquals(s3, s7.getFail());
    assertEquals(s3, s9.getFail());

    assertEquals(0, s1.getOutputs().size());
    assertEquals(0, s3.getOutputs().size());
    assertEquals(0, s4.getOutputs().size());
    assertEquals(0, s6.getOutputs().size());
    assertEquals(0, s8.getOutputs().size());
    assertEquals(1, s2.getOutputs().size());
    assertEquals(1, s7.getOutputs().size());
    assertEquals(1, s9.getOutputs().size());
    assertEquals(2, s5.getOutputs().size());
  }

  public void testStartSearchWithSingleResult() {
    tree.add("apple".getBytes(), "apple".getBytes());
    tree.prepare();
    SearchResult result = tree.startSearch("washington cut the apple tree"
        .getBytes());
    assertEquals(1, result.getOutputs().size());
    assertEquals("apple", new String((byte[]) result.getOutputs().iterator()
        .next()));
    assertEquals(24, result.getLastIndex());
    assertEquals(null, tree.continueSearch(result));
  }

  public void testStartSearchWithAdjacentResults() {
    tree.add("john".getBytes(), "john".getBytes());
    tree.add("jane".getBytes(), "jane".getBytes());
    tree.prepare();
    SearchResult firstResult = tree.startSearch("johnjane".getBytes());
    SearchResult secondResult = tree.continueSearch(firstResult);
    assertEquals(null, tree.continueSearch(secondResult));
  }

  public void testStartSearchOnEmpty() {
    tree.add("cipher".getBytes(), Integer.valueOf(0));
    tree.add("zip".getBytes(), Integer.valueOf(1));
    tree.add("nought".getBytes(), Integer.valueOf(2));
    tree.prepare();
    SearchResult result = tree.startSearch("".getBytes());
    assertEquals(null, result);
  }

  public void testMultipleOutputs() {
    tree.add("x".getBytes(), "x");
    tree.add("xx".getBytes(), "xx");
    tree.add("xxx".getBytes(), "xxx");
    tree.prepare();

    SearchResult result = tree.startSearch("xxx".getBytes());
    assertEquals(1, result.getLastIndex());
    assertEquals(new HashSet(Arrays.asList(new String[] { "x" })), result
        .getOutputs());

    result = tree.continueSearch(result);
    assertEquals(2, result.getLastIndex());
    assertEquals(new HashSet(Arrays.asList(new String[] { "xx", "x" })), result
        .getOutputs());

    result = tree.continueSearch(result);
    assertEquals(3, result.getLastIndex());
    assertEquals(new HashSet(Arrays.asList(new String[] { "xxx", "xx", "x" })),
        result.getOutputs());

    assertEquals(null, tree.continueSearch(result));
  }

  public void testIteratorInterface() {
    tree.add("moo".getBytes(), "moo");
    tree.add("one".getBytes(), "one");
    tree.add("on".getBytes(), "on");
    tree.add("ne".getBytes(), "ne");
    tree.prepare();
    Iterator iter = tree.search("one moon ago".getBytes());

    assertTrue(iter.hasNext());
    SearchResult r = (SearchResult) iter.next();
    assertEquals(new HashSet(Arrays.asList(new String[] { "on" })), r
        .getOutputs());
    assertEquals(2, r.getLastIndex());

    assertTrue(iter.hasNext());
    r = (SearchResult) iter.next();
    assertEquals(new HashSet(Arrays.asList(new String[] { "one", "ne" })), r
        .getOutputs());
    assertEquals(3, r.getLastIndex());

    assertTrue(iter.hasNext());
    r = (SearchResult) iter.next();
    assertEquals(new HashSet(Arrays.asList(new String[] { "moo" })), r
        .getOutputs());
    assertEquals(7, r.getLastIndex());

    assertTrue(iter.hasNext());
    r = (SearchResult) iter.next();
    assertEquals(new HashSet(Arrays.asList(new String[] { "on" })), r
        .getOutputs());
    assertEquals(8, r.getLastIndex());

    assertFalse(iter.hasNext());

    try {
      iter.next();
      fail();
    } catch (NoSuchElementException e) {
    }

  }

  public void largerTextExample() {
    String text = "The ga3 mutant of Arabidopsis is a gibberellin-responsive dwarf. We present data showing that the ga3-1 mutant is deficient in ent-kaurene oxidase activity, the first cytochrome P450-mediated step in the gibberellin biosynthetic pathway. By using a combination of conventional map-based cloning and random sequencing we identified a putative cytochrome P450 gene mapping to the same location as GA3. Relative to the progenitor line, two ga3 mutant alleles contained single base changes generating in-frame stop codons in the predicted amino acid sequence of the P450. A genomic clone spanning the P450 locus complemented the ga3-2 mutant. The deduced GA3 protein defines an additional class of cytochrome P450 enzymes. The GA3 gene was expressed in all tissues examined, RNA abundance being highest in inflorescence tissue.";
    String[] terms = { "microsome", "cytochrome", "cytochrome P450 activity",
        "gibberellic acid biosynthesis", "GA3", "cytochrome P450",
        "oxygen binding", "AT5G25900.1", "protein", "RNA", "gibberellin",
        "Arabidopsis", "ent-kaurene oxidase activity", "inflorescence",
        "tissue", };
    for (int i = 0; i < terms.length; i++) {
      tree.add(terms[i].getBytes(), terms[i]);
    }
    tree.prepare();

    Set termsThatHit = new HashSet();
    for (Iterator iter = tree.search(text.getBytes()); iter.hasNext();) {
      SearchResult result = (SearchResult) iter.next();
      termsThatHit.addAll(result.getOutputs());
    }
    assertEquals(new HashSet(Arrays.asList(new String[] { "cytochrome", "GA3",
        "cytochrome P450", "protein", "RNA", "gibberellin", "Arabidopsis",
        "ent-kaurene oxidase activity", "inflorescence", "tissue", })),
        termsThatHit);

  }
}
