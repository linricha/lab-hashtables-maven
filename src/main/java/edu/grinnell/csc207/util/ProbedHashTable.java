package edu.grinnell.csc207.util;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Random;
import java.util.function.BiConsumer;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A simple implementation of probed hash tables.
 *
 * @author Your Name Here
 * @author Your Name Here
 * @author Samuel A. Rebelsky
 *
 * @param <K>
 *   The type of keys.
 * @param <V>
 *   The type of values.
 */
public class ProbedHashTable<K, V> implements HashTable<K, V> {

  // +-------+-----------------------------------------------------------
  // | Notes |
  // +-------+

  /*
   * Our hash table is stored as an array of key/value pairs. Because of the
   * design of Java arrays, we declare that as type Object[] and cast whenever
   * we remove an element. (SamR needs to find a better way to deal with this
   * issue; using ArrayLists doesn't seem like the best idea.)
   *
   * We use linear probing to handle collisions. (Well, we *will* use linear
   * probing, once the table is finished.)
   *
   * We expand the hash table when the load factor is greater than LOAD_FACTOR
   * (see constants below).
   *
   * Since some combinations of data and hash function may lead to a situation
   * in which we get a surprising relationship between values (e.g., all the
   * hash values are 0 mod 32), when expanding the hash table, we incorporate a
   * random number. (Is this likely to make a big difference? Who knows. But
   * it's likely to be fun.)
   *
   * For experimentation and such, we allow the client to supply a Reporter that
   * is used to report behind-the-scenes work, such as calls to expand the
   * table.
   *
   * Bugs to squash.
   *
   * [X] Doesn't check for repeated keys in set.
   *
   * [X] Doesn't check for matching key in get.
   *
   * [X] Doesn't handle collisions.
   *
   * [?] The `expand` method is not completely implemented.
   *
   * [X] The `remove` method is not implemented.
   *
   * Features to add.
   *
   * [ ] A full implementation of `containsKey`.
   *
   * [X] An iterator.
   */

  // +-----------+-------------------------------------------------------
  // | Constants |
  // +-----------+

  /**
   * The load factor for expanding the table.
   */
  static final double LOAD_FACTOR = 0.5;

  /**
   * The offset to use in linear probes. (We choose a prime because that helps
   * ensure that we cover all of the spaces.)
   */
  static final int PROBE_OFFSET = 17;

  // +--------+----------------------------------------------------------
  // | Fields |
  // +--------+

  /**
   * The number of values currently stored in the hash table. We use this to
   * determine when to expand the hash table.
   */
  int size = 0;

  /**
   * The array that we use to store the key/value pairs. (We use an array,
   * rather than an ArrayList, because we want to control expansion.)
   */
  Object[] pairs;

  /**
   * An optional reporter to let us observe what the hash table is doing.
   */
  Reporter reporter;

  /**
   * Do we report basic calls?
   */
  boolean REPORT_BASIC_CALLS = false;

  /**
   * Our helpful random number generator, used primarily when expanding the size
   * of the table..
   */
  Random rand;

  // +--------------+----------------------------------------------------
  // | Constructors |
  // +--------------+

  /**
   * Create a new hash table.
   */
  public ProbedHashTable() {
    this.rand = new Random();
    this.clear();
    this.reporter = null;
  } // ProbedHashTable

  /**
   * Create a new hash table that reports activities using a reporter.
   *
   * @param report
   *   The object used to report activities.
   */
  public ProbedHashTable(Reporter report) {
    this();
    this.reporter = report;
  } // ProbedHashTable(Reporter)

  // +-------------------+-------------------------------------------
  // | SimpleMap methods |
  // +-------------------+

  boolean isNullKey(K key) {
    if (key == null) {
      return true;
    } // if
    return false;
  }

  /**
   * Determine if the hash table contains a particular key.
   *
   * @param key
   *   The key to look for.
   *
   * @return true if the key is in the table and false otherwise.
   */
  @Override
  public boolean containsKey(K key) {
    if (isNullKey(key)) {
      return false;
    }

    boolean hasKey = false;
    int findIndex = find(key);

    if (this.pairs[findIndex] != null) { // found key
      hasKey = true;
    } // if
  
    return hasKey;
  } // containsKey(K)

  /**
   * Apply a function to each key/value pair in the table.
   *
   * @param action
   *   The function to apply. Takes a key and a value as parameters.
   */
  public void forEach(BiConsumer<? super K, ? super V> action) {
    for (Pair<K, V> pair : this) {
      action.accept(pair.key(), pair.value());
    } // for
  } // forEach(BiConsumer)

  /**
   * Get the value for a particular key.
   *
   * @param key
   *   The key to search for.
   *
   * @return the corresponding value.
   *
   * @throws IndexOutOfBoundsException
   *   when the key is not in the table.
   */
  @Override
  public V get(K key) {
    if (isNullKey(key)) {
      throw new IndexOutOfBoundsException("Invalid key: null");
    } // if

    int index = find(key);
    @SuppressWarnings("unchecked")
    Pair<K, V> pair = (Pair<K, V>) pairs[index];
    if (pair == null || !(pair.key().equals(key))) {
      if (REPORT_BASIC_CALLS && (reporter != null)) {
        reporter.report("get(" + key + ") failed");
      } // if reporter != null
      throw new IndexOutOfBoundsException("Invalid key: " + key);
    } else {
      if (REPORT_BASIC_CALLS && (reporter != null)) {
        reporter.report("get(" + key + ") => " + pair.value());
      } // if reporter != null
      return pair.value();
    } // get
  } // get(K)

  /**
   * Iterate the keys in some order.
   *
   * @return
   *   An iterator for the keys.
   */
  public Iterator<K> keys() {
    return MiscUtils.transform(this.iterator(), (pair) -> pair.key());
  } // keys()

  /**
   * Remove a key/value pair.
   *
   * @param key
   *   The key of the key/value pair.
   *
   * @return
   *   The corresponding value.
   */
  @Override
  @SuppressWarnings("unchecked") // this ok since this.pairs should contain Pair<K, V>
  public V remove(K key) {
    if (isNullKey(key)) {
      return null;
    } // if

    ConcurrentLinkedQueue<Pair <Integer, Pair <K, V>>> queue = new ConcurrentLinkedQueue<Pair <Integer, Pair <K, V>>>();
    V value = null;

    int removeIndex = find(key);
    int pattern = Math.abs(key.hashCode()) % this.pairs.length;


    //key at index with similar mod value as key
    int similarKeyIndex = (removeIndex + PROBE_OFFSET) % this.pairs.length;


    // key can't be found, i.e. it does not exist
    if (this.pairs[removeIndex] == null) {
      return null;
    } else { // key is found
      value = ((Pair<K , V>) this.pairs[removeIndex]).value(); // store value
      this.pairs[removeIndex] = null; // remove
    } // if/else

    // Stops when loops back or reaches end the pattern of where the pairs are
    // Stores pairs of similar pattern after similarKeyIndex
    while (this.pairs[similarKeyIndex] != null) {
      
      K searchedKey = ((Pair<K , V>) this.pairs[similarKeyIndex]).key();
      int calcPattern = Math.abs(searchedKey.hashCode()) % this.pairs.length;
      if (calcPattern == pattern) { // match pattern
        queue.add(new Pair<Integer, Pair<K, V>>(similarKeyIndex, (Pair<K , V>) this.pairs[similarKeyIndex]));
      }

      similarKeyIndex = (similarKeyIndex + PROBE_OFFSET) % this.pairs.length; // go to next potential index
    } // while


    // relocate/shift all pairs with key of pattern back by one pattern.
    int relocateIndex = removeIndex;

    while (!queue.isEmpty()) {
      int patternedIndex = queue.peek().key();
      Pair<K, V> relocatePair = queue.poll().value();

      this.pairs[relocateIndex] = relocatePair;
      relocateIndex = patternedIndex;
    } // while

    // end of pattern should be null since removed pair with key, which belongs to the pattern.
    this.pairs[relocateIndex] = null;
    this.size--;

    return value;
  } // remove(K)

  /**
   * Set a value.
   *
   * @param key
   *   The key to set.
   * @param value
   *   The value to set.
   *
   * @return the prior value for the key, if there was one, or null.
   */
  @SuppressWarnings("unchecked")
  public V set(K key, V value) {
    if (isNullKey(key) || value == null) {
      return null; // cannot set key or value as null since null key doesn't work and null return is being used to say not there.
    } // if

    V result = null;
    // If there are too many entries, expand the table.
    if (this.size > (this.pairs.length * LOAD_FACTOR)) {
      expand();
    } // if there are too many entries
    // Find out where the key belongs and put the pair there.
    int index = find(key);
    if (this.pairs[index] != null) {
      result = ((Pair<K, V>) this.pairs[index]).value();
    } else { // is null
      this.size++; // since will add a new entry
    } // if/else
    this.pairs[index] = new Pair<K, V>(key, value);
    // Report activity, if appropriate
    if (REPORT_BASIC_CALLS && (reporter != null)) {
      reporter.report("pairs[" + index + "] = " + key + ":" + value);
    } // if reporter != null
    
    // this.size++;

    // And we're done
    return result;
  } // set(K, V)

  /**
   * Get the size of the dictionary - the number of key/value pairs
   * stored in the dictionary.
   *
   * @return the number of key/value pairs in the dictionary.
   */
  @Override
  public int size() {
    return this.size;
  } // size()

  /**
   * Iterate the values in some order.
   *
   * @return an iterator for the values.
   */
  public Iterator<V> values() {
    return MiscUtils.transform(this.iterator(), (pair) -> pair.value());
  } // values()

  // +------------------+--------------------------------------------
  // | Iterator methods |
  // +------------------+

  /**
   * Iterate the key/value pairs in some order.
   *
   * @return an iterator for the key/value pairs.
   */
  public Iterator<Pair<K, V>> iterator() {
    return new Iterator<Pair<K, V>>() {

      int currentIndex = 0;

      public boolean hasNext() {
        for (int i = currentIndex; i < pairs.length; i++) {
          if (pairs[i] != null) { // has next pair
            return true;
          } // if
        } // for
        return false;
      } // hasNext()

      @SuppressWarnings("unchecked")
      public Pair<K, V> next() {
        if (hasNext()) {
          while (pairs[currentIndex] == null) {
            currentIndex++;
          } // while
          currentIndex++;
          return (Pair <K, V>) pairs[currentIndex - 1];
        } // if
        
        return null;
      } // next()
    }; // new Iterator
  } // iterator()

  // +-------------------+-------------------------------------------
  // | HashTable methods |
  // +-------------------+

  /**
   * Clear the whole table.
   */
  @Override
  public void clear() {
    this.pairs = new Object[41];
    this.size = 0;
  } // clear()

  /**
   * Dump the hash table.
   *
   * @param pen
   *   Where to dump the table.
   */
  @Override
  public void dump(PrintWriter pen) {
    pen.print("{");
    int printed = 0; // Number of elements printed
    for (int i = 0; i < this.pairs.length; i++) {
      @SuppressWarnings("unchecked")
      Pair<K, V> pair = (Pair<K, V>) this.pairs[i];
      if (pair != null) {
        pen.print(i + ":" + pair.key() + "(" + pair.key().hashCode() + "):"
            + pair.value());
        if (++printed < this.size) {
          pen.print(", ");
        } // if
      } // if the current element is not null
    } // for
    pen.println("}");
  } // dump(PrintWriter)

  // +------+------------------------------------------------------------
  // | Misc |
  // +------+

  /**
   * Should we report basic calls? Intended mostly for tracing.
   *
   * @param report
   *   Use true if you want basic calls reported and false otherwise.
   */
  public void reportBasicCalls(boolean report) {
    REPORT_BASIC_CALLS = report;
  } // reportBasicCalls

  // +---------+---------------------------------------------------------
  // | Helpers |
  // +---------+

  /**
   * Expand the size of the table.
   */
  @SuppressWarnings("unchecked") // safe
  void expand() { // doesn't this also mess with set and get since increased size? so diff positions if redone?
    // Figure out the size of the new table.
    int newSize = 2 * this.pairs.length + rand.nextInt(10);

    //check that size is not multiple of Probe Offset
    if (newSize % PROBE_OFFSET == 0) {
      newSize++; // Under assumption that probe_offset is not 1 / prime
    } // if

    if (REPORT_BASIC_CALLS && (reporter != null)) {
      reporter.report("Expanding to " + newSize + " elements.");
    } // if reporter != null

    Object[] oldPairs = this.pairs;
    
    this.pairs = new Object[newSize];
    this.size = 0;
    

    // Move all the values from the old table to their appropriate 
    // location in the new table.
    for (int i = 0; i < oldPairs.length; i++) {
      
      if (oldPairs[i] != null) { // pair here at i
        Pair<K, V> movePair = (Pair <K, V>) oldPairs[i];
        set(movePair.key(), movePair.value());
      } // if
    } // for

  } // expand()

  /**
   * Find the index of the entry with a given key. If there is no such entry,
   * return the index of an entry we can use to store that key.
   *
   * @param key
   *   The key we're searching for. Key should not be null.
   *
   * @return the aforementioned index.
   */
  @SuppressWarnings("unchecked")
  int find(K key) {

    boolean found = false;

    int guessedIndex = Math.abs(key.hashCode()) % this.pairs.length;
    int realIndex = guessedIndex;


    // We know that pairs contains the object pair<K,V>
    // If key at guessed index is not the correct key, find the correct index.
    while (!found) {

      // space is null / traversed path correctly and could not find key
      // i.e. key does not exist since key follows this same path to set it but null reached first
      if (this.pairs[guessedIndex] == null) {
        realIndex = guessedIndex;
        found = true;
      } else if (((Pair<K, V>) this.pairs[guessedIndex]).key().equals(key)) {// key found
        realIndex = guessedIndex;
        found = true;
      } else { // check new index for key or empty space (to possible put new key / check if key can be found)
        guessedIndex = (guessedIndex + PROBE_OFFSET) % this.pairs.length;
      } // if/else-if/else
    }

    return realIndex;
  } // find(K)

} // class ProbedHashTable<K, V>

