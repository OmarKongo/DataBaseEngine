package DataBaseEngine.DB;

import java.lang.*;
import java.lang.reflect.Array;
import java.util.*;
import java.io.*;

@SuppressWarnings({ "serial" })
public class bplustree<K extends Comparable<K>> implements Serializable {
	private Class<K> clazz;
	int m;
	InternalNode root;
	LeafNode firstLeaf;

	/* ~~~~~~~~~~~~~~~~ HELPER FUNCTIONS ~~~~~~~~~~~~~~~~ */
	public String toString() {
		LeafNode n = this.firstLeaf;

		return n + "";
	}

	/**
	 * This method performs a standard binary search on a sorted
	 * DictionaryPair[] and returns the index of the dictionary pair
	 * with target key t if found. Otherwise, this method returns a negative
	 * value.
	 * 
	 * @param dps: list of dictionary pairs sorted by key within leaf node
	 * @param t:   target key value of dictionary pair being searched for
	 * @return index of the target value if found, else a negative value
	 */
	private int binarySearch(DictionaryPair[] dps, int numPairs, K key) {

		return Arrays.binarySearch(dps, 0, numPairs, new DictionaryPair(key, null), null);
	}

	/**
	 * This method starts at the root of the B+ tree and traverses down the
	 * tree via key comparisons to the corresponding leaf node that holds 'key'
	 * within its dictionary.
	 * 
	 * @param key: the unique key that lies within the dictionary of a LeafNode
	 *             object
	 * @return the LeafNode object that contains the key within its dictionary
	 */
	private LeafNode findLeafNode(K key) {

		// Initialize keys and index variable
		K[] keys = this.root.keys;
		int i;

		// Find next node on path to appropriate leaf node
		for (i = 0; i < this.root.degree - 1; i++) {
			if (key.compareTo(keys[i]) < 0) {
				break;
			}

		}

		/*
		 * Return node if it is a LeafNode object,
		 * otherwise repeat the search function a level down
		 */
		Node child = this.root.childPointers[i];
		if (child instanceof bplustree.LeafNode) {
			return (LeafNode) child;
		} else {
			return findLeafNode((InternalNode) child, key);
		}
	}

	private LeafNode findLeafNode(InternalNode node, K key) {

		// Initialize keys and index variable
		K[] keys = node.keys;
		int i;

		// Find next node on path to appropriate leaf node
		for (i = 0; i < node.degree - 1; i++) {
			if (key.compareTo(keys[i]) < 0) {
				break;
			}
		}

		/*
		 * Return node if it is a LeafNode object,
		 * otherwise repeat the search function a level down
		 */
		Node childNode = node.childPointers[i];
		if (childNode instanceof bplustree.LeafNode) {
			return (LeafNode) childNode;
		} else {
			return findLeafNode((InternalNode) node.childPointers[i], key);
		}
	}

	/**
	 * Given a list of pointers to Node objects, this method returns the index of
	 * the pointer that points to the specified 'node' LeafNode object.
	 * 
	 * @param pointers: a list of pointers to Node objects
	 * @param node:     a specific pointer to a LeafNode
	 * @return (int) index of pointer in list of pointers
	 */
	private int findIndexOfPointer(Node[] pointers, LeafNode node) {
		int i;
		for (i = 0; i < pointers.length; i++) {
			if (pointers[i] == node) {
				break;
			}
		}
		return i;
	}

	/**
	 * This is a simple method that returns the midpoint (or lower bound
	 * depending on the context of the method invocation) of the max degree m of
	 * the B+ tree.
	 * 
	 * @return (int) midpoint/lower bound
	 */
	private int getMidpoint() {
		return (int) Math.ceil((this.m + 1) / 2.0) - 1;
	}

	/**
	 * Given a deficient InternalNode in, this method remedies the deficiency
	 * through borrowing and merging.
	 * 
	 * @param in: a deficient InternalNode
	 */
	private void handleDeficiency(InternalNode in) {

		InternalNode sibling;
		InternalNode parent = in.parent;

		// Remedy deficient root node
		if (this.root == in) {
			for (int i = 0; i < in.childPointers.length; i++) {
				if (in.childPointers[i] != null) {
					if (in.childPointers[i] instanceof bplustree.InternalNode) {
						this.root = (InternalNode) in.childPointers[i];
						this.root.parent = null;
					} else if (in.childPointers[i] instanceof bplustree.LeafNode) {
						this.root = null;
					}
				}
			}
		}

		// Borrow:
		else if (in.leftSibling != null && in.leftSibling.isLendable()) {
			sibling = in.leftSibling;
		} else if (in.rightSibling != null && in.rightSibling.isLendable()) {
			sibling = in.rightSibling;

			// Copy 1 key and pointer from sibling (atm just 1 key)
			K borrowedKey = sibling.keys[0];
			Node pointer = sibling.childPointers[0];

			// Copy root key and pointer into parent
			in.keys[in.degree - 1] = parent.keys[0];
			in.childPointers[in.degree] = pointer;

			// Copy borrowedKey into root
			parent.keys[0] = borrowedKey;

			// Delete key and pointer from sibling
			sibling.removePointer(0);
			Arrays.sort(sibling.keys);
			sibling.removePointer(0);
			shiftDown(in.childPointers, 1);
		}

		// Merge:
		else if (in.leftSibling != null && in.leftSibling.isMergeable()) {

		} else if (in.rightSibling != null && in.rightSibling.isMergeable()) {
			sibling = in.rightSibling;

			// Copy rightmost key in parent to beginning of sibling's keys &
			// delete key from parent
			sibling.keys[sibling.degree - 1] = parent.keys[parent.degree - 2];
			Arrays.sort(sibling.keys, 0, sibling.degree);
			parent.keys[parent.degree - 2] = null;

			// Copy in's child pointer over to sibling's list of child pointers
			for (int i = 0; i < in.childPointers.length; i++) {
				if (in.childPointers[i] != null) {
					sibling.prependChildPointer(in.childPointers[i]);
					in.childPointers[i].parent = sibling;
					in.removePointer(i);
				}
			}

			// Delete child pointer from grandparent to deficient node
			parent.removePointer(in);

			// Remove left sibling
			sibling.leftSibling = in.leftSibling;
		}

		// Handle deficiency a level up if it exists
		if (parent != null && parent.isDeficient()) {
			handleDeficiency(parent);
		}
	}

	/**
	 * This is a simple method that determines if the B+ tree is empty or not.
	 * 
	 * @return a boolean indicating if the B+ tree is empty or not
	 */
	private boolean isEmpty() {
		return firstLeaf == null;
	}

	/**
	 * This method performs a standard linear search on a sorted
	 * DictionaryPair[] and returns the index of the first null entry found.
	 * Otherwise, this method returns a -1. This method is primarily used in
	 * place of binarySearch() when the target t = null.
	 * 
	 * @param dps: list of dictionary pairs sorted by key within leaf node
	 * @return index of the target value if found, else -1
	 */
	private int linearNullSearch(DictionaryPair[] dps) {
		for (int i = 0; i < dps.length; i++) {
			if (dps[i] == null) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * This method performs a standard linear search on a list of Node[] pointers
	 * and returns the index of the first null entry found. Otherwise, this
	 * method returns a -1. This method is primarily used in place of
	 * binarySearch() when the target t = null.
	 * 
	 * @param pointers: list of Node[] pointers
	 * @return index of the target value if found, else -1
	 */
	private int linearNullSearch(Node[] pointers) {
		for (int i = 0; i < pointers.length; i++) {
			if (pointers[i] == null) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * This method is used to shift down a set of pointers that are prepended
	 * by null values.
	 * 
	 * @param pointers: the list of pointers that are to be shifted
	 * @param amount:   the amount by which the pointers are to be shifted
	 */
	private void shiftDown(Node[] pointers, int amount) {
		Node[] newPointers = new bplustree.Node[this.m + 1];
		for (int i = amount; i < pointers.length; i++) {
			newPointers[i - amount] = pointers[i];
		}
		pointers = newPointers;
	}

	/**
	 * This is a specialized sorting method used upon lists of DictionaryPairs
	 * that may contain interspersed null values.
	 * 
	 * @param dictionary: a list of DictionaryPair objects
	 */
	private void sortDictionary(DictionaryPair[] dictionary) {
		Arrays.sort(dictionary, new Comparator<DictionaryPair>() {
			@Override
			public int compare(DictionaryPair o1, DictionaryPair o2) {
				if (o1 == null && o2 == null) {
					return 0;
				}
				if (o1 == null) {
					return 1;
				}
				if (o2 == null) {
					return -1;
				}
				return o1.compareTo(o2);
			}
		});
	}

	/**
	 * This method modifies the InternalNode 'in' by removing all pointers within
	 * the childPointers after the specified split. The method returns the removed
	 * pointers in a list of their own to be used when constructing a new
	 * InternalNode sibling.
	 * 
	 * @param in:    an InternalNode whose childPointers will be split
	 * @param split: the index at which the split in the childPointers begins
	 * @return a Node[] of the removed pointers
	 */
	private Node[] splitChildPointers(InternalNode in, int split) {

		Node[] pointers = in.childPointers;
		Node[] halfPointers = new bplustree.Node[this.m + 1];

		// Copy half of the values into halfPointers while updating original keys
		for (int i = split + 1; i < pointers.length; i++) {
			halfPointers[i - split - 1] = pointers[i];
			in.removePointer(i);
		}

		return halfPointers;
	}

	/**
	 * This method splits a single dictionary into two dictionaries where all
	 * dictionaries are of equal length, but each of the resulting dictionaries
	 * holds half of the original dictionary's non-null values. This method is
	 * primarily used when splitting a node within the B+ tree. The dictionary of
	 * the specified LeafNode is modified in place. The method returns the
	 * remainder of the DictionaryPairs that are no longer within ln's dictionary.
	 * 
	 * @param ln:    list of DictionaryPairs to be split
	 * @param split: the index at which the split occurs
	 * @return DictionaryPair[] of the two split dictionaries
	 */
	private DictionaryPair[] splitDictionary(LeafNode ln, int split) {

		DictionaryPair[] dictionary = ln.dictionary;

		/*
		 * Initialize two dictionaries that each hold half of the original
		 * dictionary values
		 */
		DictionaryPair[] halfDict = new bplustree.DictionaryPair[this.m];

		// Copy half of the values into halfDict
		for (int i = split; i < dictionary.length; i++) {
			halfDict[i - split] = dictionary[i];
			ln.delete(i);
		}

		return halfDict;
	}

	/**
	 * When an insertion into the B+ tree causes an overfull node, this method
	 * is called to remedy the issue, i.e. to split the overfull node. This method
	 * calls the sub-methods of splitKeys() and splitChildPointers() in order to
	 * split the overfull node.
	 * 
	 * @param in: an overfull InternalNode that is to be split
	 */
	private void splitInternalNode(InternalNode in) {

		// Acquire parent
		InternalNode parent = in.parent;

		// Split keys and pointers in half
		int midpoint = getMidpoint();
		K newParentKey = in.keys[midpoint];
		K[] halfKeys = splitKeys(in.keys, midpoint);
		Node[] halfPointers = splitChildPointers(in, midpoint);

		// Change degree of original InternalNode in
		in.degree = linearNullSearch(in.childPointers);

		// Create new sibling internal node and add half of keys and pointers
		InternalNode sibling = new InternalNode(this.m, halfKeys, halfPointers);
		for (Node pointer : halfPointers) {
			if (pointer != null) {
				pointer.parent = sibling;
			}
		}

		// Make internal nodes siblings of one another
		sibling.rightSibling = in.rightSibling;
		if (sibling.rightSibling != null) {
			sibling.rightSibling.leftSibling = sibling;
		}
		in.rightSibling = sibling;
		sibling.leftSibling = in;

		if (parent == null) {

			// Create new root node and add midpoint key and pointers
			K[] keys = createKeyArray();
			keys[0] = newParentKey;
			InternalNode newRoot = new InternalNode(this.m, keys);
			newRoot.appendChildPointer(in);
			newRoot.appendChildPointer(sibling);
			this.root = newRoot;

			// Add pointers from children to parent
			in.parent = newRoot;
			sibling.parent = newRoot;

		} else {

			// Add key to parent
			parent.keys[parent.degree - 1] = newParentKey;
			Arrays.sort(parent.keys, 0, parent.degree);

			// Set up pointer to new sibling
			int pointerIndex = parent.findIndexOfPointer(in) + 1;
			parent.insertChildPointer(sibling, pointerIndex);
			sibling.parent = parent;
		}
	}

	/**
	 * This method modifies a list of Integer-typed objects that represent keys
	 * by removing half of the keys and returning them in a separate Integer[].
	 * This method is used when splitting an InternalNode object.
	 * 
	 * @param keys:  a list of Integer objects
	 * @param split: the index where the split is to occur
	 * @return Integer[] of removed keys
	 */
	private K[] splitKeys(K[] keys, int split) {

		K[] halfKeys = createKeyArray();

		// Remove split-indexed value from keys
		keys[split] = null;

		// Copy half of the values into halfKeys while updating original keys
		for (int i = split + 1; i < keys.length; i++) {
			halfKeys[i - (split + 1)] = keys[i];
			keys[i] = null;
		}

		return halfKeys;
	}

	@SuppressWarnings("unchecked")
	private K[] createKeyArray() {
		return (K[]) Array.newInstance(clazz, this.m);
	}

	/* ~~~~~~~~~~~~~~~~ API: DELETE, INSERT, SEARCH, UPDATE ~~~~~~~~~~~~~~~~ */

	/**
	 * Given a dictionaryPair that contains a subset of values of search method.
	 * It deletes the values within the dictionaryPair's value array
	 * 
	 * @param dp: dictionaryPair to delete with.
	 */
	public ArrayList<String> delete(K key, ArrayList<String> pageNames) {

		DictionaryPair dp = new DictionaryPair(key, pageNames);

		if (isEmpty()) {

			/* Flow of execution goes here when B+ tree has no dictionary pairs */
			System.err.println("Invalid Delete: The B+ tree is currently empty.");

		}
		// Get leaf node and attempt to find index of key to delete
		LeafNode ln = (this.root == null) ? this.firstLeaf : findLeafNode(dp.key);
		int dpIndex = binarySearch(ln.dictionary, ln.numPairs, key);

		if (dpIndex < 0) {

			/* Flow of execution goes here when key is absent in B+ tree */
			System.err.println("Invalid Delete: Key unable to be found.");

		}

		DictionaryPair targetDP = ln.dictionary[dpIndex];

		// delete specific values for given key if dp.value is null
		if (dp.value != null) {
			// PageNames to be returned by method
			ArrayList<String> pagesHadKey = new ArrayList<>();
			// delete relevant pages from dictionaryPair
			for (String o : dp.value) {
				int filterValue = targetDP.value.indexOf(o);
				if (filterValue > -1) {
					targetDP.value.remove(filterValue);
					pagesHadKey.add(o);
				} else {
					System.out.println("Key: " + key + " does not contain " + o);
				}
			}

			if (!targetDP.value.isEmpty())
				return pagesHadKey;

		}

		// Successfully delete the dictionary pair
		DictionaryPair deletedDP = ln.delete(dpIndex);

		// Check for deficiencies
		if (ln.isDeficient()) {

			LeafNode sibling;
			InternalNode parent = ln.parent;

			// Borrow: First, check the left sibling, then the right sibling
			if (ln.leftSibling != null &&
					ln.leftSibling.parent == ln.parent &&
					ln.leftSibling.isLendable()) {

				sibling = ln.leftSibling;
				DictionaryPair borrowedDP = sibling.dictionary[sibling.numPairs - 1];

				/*
				 * Insert borrowed dictionary pair, sort dictionary,
				 * and delete dictionary pair from sibling
				 */
				ln.insert(borrowedDP);
				sortDictionary(ln.dictionary);
				sibling.delete(sibling.numPairs - 1);

				// Update key in parent if necessary
				int pointerIndex = findIndexOfPointer(parent.childPointers, ln);
				if (borrowedDP.key.compareTo(parent.keys[pointerIndex - 1]) < 0) {
					parent.keys[pointerIndex - 1] = ln.dictionary[0].key;
				}

			} else if (ln.rightSibling != null &&
					ln.rightSibling.parent == ln.parent &&
					ln.rightSibling.isLendable()) {

				sibling = ln.rightSibling;
				DictionaryPair borrowedDP = sibling.dictionary[0];

				/*
				 * Insert borrowed dictionary pair, sort dictionary,
				 * and delete dictionary pair from sibling
				 */
				ln.insert(borrowedDP);
				sibling.delete(0);
				sortDictionary(sibling.dictionary);

				// Update key in parent if necessary
				int pointerIndex = findIndexOfPointer(parent.childPointers, ln);
				if (borrowedDP.key.compareTo(parent.keys[pointerIndex]) >= 0) {
					parent.keys[pointerIndex] = sibling.dictionary[0].key;
				}

			}

			// Merge: First, check the left sibling, then the right sibling
			else if (ln.leftSibling != null &&
					ln.leftSibling.parent == ln.parent &&
					ln.leftSibling.isMergeable()) {

				sibling = ln.leftSibling;
				int pointerIndex = findIndexOfPointer(parent.childPointers, ln);

				// Remove key and child pointer from parent
				parent.removeKey(pointerIndex - 1);
				parent.removePointer(ln);

				// Update sibling pointer
				sibling.rightSibling = ln.rightSibling;

				// Check for deficiencies in parent
				if (parent.isDeficient()) {
					handleDeficiency(parent);
				}

			} else if (ln.rightSibling != null &&
					ln.rightSibling.parent == ln.parent &&
					ln.rightSibling.isMergeable()) {

				sibling = ln.rightSibling;
				int pointerIndex = findIndexOfPointer(parent.childPointers, ln);

				// Remove key and child pointer from parent
				parent.removeKey(pointerIndex);
				parent.removePointer(pointerIndex);

				// Update sibling pointer
				sibling.leftSibling = ln.leftSibling;
				if (sibling.leftSibling == null) {
					firstLeaf = sibling;
				}

				if (parent.isDeficient()) {
					handleDeficiency(parent);
				}
			}

		} else if (this.root == null && this.firstLeaf.numPairs == 0) {

			/*
			 * Flow of execution goes here when the deleted dictionary
			 * pair was the only pair within the tree
			 */

			// Set first leaf as null to indicate B+ tree is empty
			this.firstLeaf = null;

		} else {

			/*
			 * The dictionary of the LeafNode object may need to be
			 * sorted after a successful delete
			 */
			sortDictionary(ln.dictionary);

		}

		return deletedDP.value;
	}

	/**
	 * Given an integer key and floating point value, this method inserts a
	 * dictionary pair accordingly into the B+ tree.
	 * 
	 * @param key:   an integer key to be used in the dictionary pair
	 * @param value: a floating point number to be used in the dictionary pair
	 */
	public void insert(K key, ArrayList<String> pageNames) {
		System.out.println("--------Page Names-------- " + pageNames.toString());
		DictionaryPair dp = new DictionaryPair(key, pageNames);

		if (isEmpty()) {

			/* Flow of execution goes here only when first insert takes place */

			// Create leaf node as first node in B plus tree (root is null)
			LeafNode ln = new LeafNode(this.m, dp);

			// Set as first leaf node (can be used later for in-order leaf traversal)
			this.firstLeaf = ln;

		} else {

			// Find leaf node to insert into
			LeafNode ln = (this.root == null) ? this.firstLeaf : findLeafNode(dp.key);

			int indexOfEqualKey = binarySearch(ln.dictionary, ln.numPairs, key);
			if (indexOfEqualKey >= 0) {

				ln.dictionary[indexOfEqualKey].value.addAll(dp.value);
			}
			// Insert into leaf node fails if node becomes overfull
			else if (!ln.insert(dp)) {

				// Sort all the dictionary pairs with the included pair to be inserted
				ln.dictionary[ln.numPairs] = dp;
				ln.numPairs++;
				sortDictionary(ln.dictionary);

				// Split the sorted pairs into two halves
				int midpoint = getMidpoint();
				DictionaryPair[] halfDict = splitDictionary(ln, midpoint);

				if (ln.parent == null) {

					/* Flow of execution goes here when there is 1 node in tree */

					// Create internal node to serve as parent, use dictionary midpoint key
					K[] parent_keys = createKeyArray();
					parent_keys[0] = halfDict[0].key;
					InternalNode parent = new InternalNode(this.m, parent_keys);
					ln.parent = parent;
					parent.appendChildPointer(ln);

				} else {

					/* Flow of execution goes here when parent exists */

					// Add new key to parent for proper indexing
					K newParentKey = halfDict[0].key;
					ln.parent.keys[ln.parent.degree - 1] = newParentKey;
					Arrays.sort(ln.parent.keys, 0, ln.parent.degree);
				}

				// Create new LeafNode that holds the other half
				LeafNode newLeafNode = new LeafNode(this.m, halfDict, ln.parent);

				// Update child pointers of parent node
				int pointerIndex = ln.parent.findIndexOfPointer(ln) + 1;
				ln.parent.insertChildPointer(newLeafNode, pointerIndex);

				// Make leaf nodes siblings of one another
				newLeafNode.rightSibling = ln.rightSibling;
				if (newLeafNode.rightSibling != null) {
					newLeafNode.rightSibling.leftSibling = newLeafNode;
				}
				ln.rightSibling = newLeafNode;
				newLeafNode.leftSibling = ln;

				if (this.root == null) {

					// Set the root of B+ tree to be the parent
					this.root = ln.parent;

				} else {

					/*
					 * If parent is overfull, repeat the process up the tree,
					 * until no deficiencies are found
					 */
					InternalNode in = ln.parent;
					while (in != null) {
						if (in.isOverfull()) {
							splitInternalNode(in);
						} else {
							break;
						}
						in = in.parent;
					}
				}
			}
		}
	}

	/**
	 * Given a key, this method returns the value associated with the key
	 * within a dictionary pair that exists inside the B+ tree.
	 * 
	 * @param key: the key to be searched within the B+ tree
	 * @return the floating point value associated with the key within the B+ tree
	 */
	public ArrayList<String> search(K key) {

		// Empty ArrayList if search doesn't find anything
		ArrayList<String> returnEmpty = new ArrayList<>();
		// If B+ tree is completely empty, simply return null
		if (isEmpty()) {
			System.err.println("This key doesn't exist");
			return returnEmpty;
		}

		// Find leaf node that holds the dictionary key
		LeafNode ln = (this.root == null) ? this.firstLeaf : findLeafNode(key);

		// Perform binary search to find index of key within dictionary
		DictionaryPair[] dps = ln.dictionary;
		int index = binarySearch(dps, ln.numPairs, key);

		// If index negative, the key doesn't exist in B+ tree
		if (index < 0) {
			System.err.println("This key doesn't exist");
			return returnEmpty;
		} else {
			return dps[index].value;
		}
	}

	public ArrayList<String> rangeSearch(K key, String strarrOperator) {
		// what if there are duplicates and multiple pages for a particular key
		// Instantiate array to hold values
		ArrayList<String> values = new ArrayList<String>();
		LeafNode currNode = (this.root == null) ? this.firstLeaf : findLeafNode(key);
		// Create a new LinkedHashSet
		Set<String> set = new LinkedHashSet<>();

		// Iterate through the doubly linked list of leaves
		switch (strarrOperator) {
			case ">":
			case ">=": {
				int countIter = 0;
				while (currNode != null) {

					// Iterate through the dictionary of each node
					DictionaryPair dps[] = currNode.dictionary;
					System.out.println(dps.length + " length of dict");
					System.out.println(dps[0] + " Brolosy?");
					System.out.println(dps[1] + " Brolosy 1 ?");
					for (DictionaryPair dp : dps) {

						/*
						 * Stop searching the dictionary once a null value is encountered
						 * as this the indicates the end of non-null values
						 */
						if (dp == null) {
							break;
						}

						// Include value if its key fits within the provided range
						if (strarrOperator.equals(">") && countIter == 0) {
							countIter++;
							continue;
						}
						// value is the array containing the pages (where dups may occur)
						// values now contains all pages which include tuples > | >= the key
						System.out.println("values before: " + values);
						if (!(values.containsAll(dp.value))) {
							values.addAll(dp.value);
						}
						System.out.println("values after: " + values);
						countIter++;

					}
					/*
					 * Update the current node to be the right sibling,
					 * leaf traversal is from left to right
					 */

					currNode = currNode.rightSibling;
				}
				break;
			}

			case "<":
			case "<=": {
				int countIter = 0;
				while (currNode != null) {

					// Iterate through the dictionary of each node
					DictionaryPair dps[] = currNode.dictionary;
					for (DictionaryPair dp : dps) {

						/*
						 * Stop searching the dictionary once a null value is encountered
						 * as this the indicates the end of non-null values
						 */
						if (dp == null) {
							break;
						}

						// Include value if its key fits within the provided range
						if (strarrOperator.equals("<") && countIter == 0) {
							countIter++;
							continue;
						}
						// value is the array containing the pages (where dups may occur)
						// values now contains all pages which include tuples > | >= the key
						if (!(values.containsAll(dp.value)))
							values.addAll(dp.value);
						countIter++;

					}
					/*
					 * Update the current node to be the left sibling,
					 * leaf traversal is from right to left
					 */
					currNode = currNode.leftSibling;
				}
			}
				break;
		}
		// Add the elements to set
		set.addAll(values);

		// Clear the list
		values.clear();

		// add the elements of set
		// with no duplicates to the list
		values.addAll(set);
		return values;

	}

	/**
	 * This method traverses the doubly linked list of the B+ tree and records
	 * all values whose associated keys are within the range specified by
	 * lowerBound and upperBound.
	 * 
	 * @param lowerBound: (int) the lower bound of the range
	 * @param upperBound: (int) the upper bound of the range
	 * @return an ArrayList<Double> that holds all values of dictionary pairs
	 *         whose keys are within the specified range
	 */
	public ArrayList<String> search(K lowerBound, K upperBound) {

		// Instantiate Double array to hold values
		ArrayList<String> values = new ArrayList<String>();

		// Iterate through the doubly linked list of leaves
		LeafNode currNode = this.firstLeaf;
		while (currNode != null) {

			// Iterate through the dictionary of each node
			DictionaryPair dps[] = currNode.dictionary;
			for (DictionaryPair dp : dps) {

				/*
				 * Stop searching the dictionary once a null value is encountered
				 * as this the indicates the end of non-null values
				 */
				if (dp == null) {
					break;
				}

				// Include value if its key fits within the provided range
				if ((dp.key.compareTo(lowerBound) >= 0) && (dp.key.compareTo(upperBound) <= 0)) {
					values.addAll(dp.value);
				}
			}
			/*
			 * Update the current node to be the right sibling,
			 * leaf traversal is from left to right
			 */
			currNode = currNode.rightSibling;
		}

		return values;
	}

	/**
	 * This method takes a DictionaryPair and map the values to another key
	 * 
	 * @param oldDP: The dictionaryPair of key and specific pages that are to be
	 *               changed
	 */
	public void update(K oldKey, ArrayList<String> pageNamesToMove, K newKey) {

		ArrayList<String> values = this.delete(oldKey, pageNamesToMove);

		this.insert(newKey, values);
	}

	/**
	 * Constructor
	 * 
	 * @param m: the order (fanout) of the B+ tree
	 */
	public bplustree(Class<K> clazz, int m) {
		this.clazz = clazz;
		this.m = m;
		this.root = null;
	}

	/**
	 * This class represents a general node within the B+ tree and serves as a
	 * superclass of InternalNode and LeafNode.
	 */
	public class Node implements Serializable {
		InternalNode parent;
	}

	/**
	 * This class represents the internal nodes within the B+ tree that traffic
	 * all search/insert/delete operations. An internal node only holds keys; it
	 * does not hold dictionary pairs.
	 */
	private class InternalNode extends Node implements Serializable {
		int maxDegree;
		int minDegree;
		int degree;
		InternalNode leftSibling;
		InternalNode rightSibling;
		K[] keys;
		Node[] childPointers;

		public String toString() {
			String res = "";
			for (int i = 0; i < this.keys.length; i++) {
				res = res + this.keys[i] + "      ";
			}

			return res;
		}

		/**
		 * This method appends 'pointer' to the end of the childPointers
		 * instance variable of the InternalNode object. The pointer can point to
		 * an InternalNode object or a LeafNode object since the formal
		 * parameter specifies a Node object.
		 * 
		 * @param pointer: Node pointer that is to be appended to the
		 *                 childPointers list
		 */
		private void appendChildPointer(Node pointer) {
			this.childPointers[degree] = pointer;
			this.degree++;
		}

		/**
		 * Given a Node pointer, this method will return the index of where the
		 * pointer lies within the childPointers instance variable. If the pointer
		 * can't be found, the method returns -1.
		 * 
		 * @param pointer: a Node pointer that may lie within the childPointers
		 *                 instance variable
		 * @return the index of 'pointer' within childPointers, or -1 if
		 *         'pointer' can't be found
		 */
		private int findIndexOfPointer(Node pointer) {
			for (int i = 0; i < childPointers.length; i++) {
				if (childPointers[i] == pointer) {
					return i;
				}
			}
			return -1;
		}

		/**
		 * Given a pointer to a Node object and an integer index, this method
		 * inserts the pointer at the specified index within the childPointers
		 * instance variable. As a result of the insert, some pointers may be
		 * shifted to the right of the index.
		 * 
		 * @param pointer: the Node pointer to be inserted
		 * @param index:   the index at which the insert is to take place
		 */
		private void insertChildPointer(Node pointer, int index) {
			for (int i = degree - 1; i >= index; i--) {
				childPointers[i + 1] = childPointers[i];
			}
			this.childPointers[index] = pointer;
			this.degree++;
		}

		/**
		 * This simple method determines if the InternalNode is deficient or not.
		 * An InternalNode is deficient when its current degree of children falls
		 * below the allowed minimum.
		 * 
		 * @return a boolean indicating whether the InternalNode is deficient
		 *         or not
		 */
		private boolean isDeficient() {
			return this.degree < this.minDegree;
		}

		/**
		 * This simple method determines if the InternalNode is capable of
		 * lending one of its dictionary pairs to a deficient node. An InternalNode
		 * can give away a dictionary pair if its current degree is above the
		 * specified minimum.
		 * 
		 * @return a boolean indicating whether or not the InternalNode has
		 *         enough dictionary pairs in order to give one away.
		 */
		private boolean isLendable() {
			return this.degree > this.minDegree;
		}

		/**
		 * This simple method determines if the InternalNode is capable of being
		 * merged with. An InternalNode can be merged with if it has the minimum
		 * degree of children.
		 * 
		 * @return a boolean indicating whether or not the InternalNode can be
		 *         merged with
		 */
		private boolean isMergeable() {
			return this.degree == this.minDegree;
		}

		/**
		 * This simple method determines if the InternalNode is considered overfull,
		 * i.e. the InternalNode object's current degree is one more than the
		 * specified maximum.
		 * 
		 * @return a boolean indicating if the InternalNode is overfull
		 */
		private boolean isOverfull() {
			return this.degree == maxDegree + 1;
		}

		/**
		 * Given a pointer to a Node object, this method inserts the pointer to
		 * the beginning of the childPointers instance variable.
		 * 
		 * @param pointer: the Node object to be prepended within childPointers
		 */
		private void prependChildPointer(Node pointer) {
			for (int i = degree - 1; i >= 0; i--) {
				childPointers[i + 1] = childPointers[i];
			}
			this.childPointers[0] = pointer;
			this.degree++;
		}

		/**
		 * This method sets keys[index] to null. This method is used within the
		 * parent of a merging, deficient LeafNode.
		 * 
		 * @param index: the location within keys to be set to null
		 */
		private void removeKey(int index) {
			this.keys[index] = null;
		}

		/**
		 * This method sets childPointers[index] to null and additionally
		 * decrements the current degree of the InternalNode.
		 * 
		 * @param index: the location within childPointers to be set to null
		 */
		private void removePointer(int index) {
			this.childPointers[index] = null;
			this.degree--;
		}

		/**
		 * This method removes 'pointer' from the childPointers instance
		 * variable and decrements the current degree of the InternalNode. The
		 * index where the pointer node was assigned is set to null.
		 * 
		 * @param pointer: the Node pointer to be removed from childPointers
		 */
		private void removePointer(Node pointer) {
			for (int i = 0; i < childPointers.length; i++) {
				if (childPointers[i] == pointer) {
					this.childPointers[i] = null;
				}
			}
			this.degree--;
		}

		/**
		 * Constructor
		 * 
		 * @param m:    the max degree of the InternalNode
		 * @param keys: the list of keys that InternalNode is initialized with
		 */
		private InternalNode(int m, K[] keys) {
			this.maxDegree = m;
			this.minDegree = (int) Math.ceil(m / 2.0);
			this.degree = 0;
			this.keys = keys;
			this.childPointers = new bplustree.Node[this.maxDegree + 1];
		}

		/**
		 * Constructor
		 * 
		 * @param m:        the max degree of the InternalNode
		 * @param keys:     the list of keys that InternalNode is initialized with
		 * @param pointers: the list of pointers that InternalNode is initialized with
		 */
		private InternalNode(int m, K[] keys, Node[] pointers) {
			this.maxDegree = m;
			this.minDegree = (int) Math.ceil(m / 2.0);
			this.degree = linearNullSearch(pointers);
			this.keys = keys;
			this.childPointers = pointers;
		}
	}

	/**
	 * This class represents the leaf nodes within the B+ tree that hold
	 * dictionary pairs. The leaf node has no children. The leaf node has a
	 * minimum and maximum number of dictionary pairs it can hold, as specified
	 * by m, the max degree of the B+ tree. The leaf nodes form a doubly linked
	 * list that, i.e. each leaf node has a left and right sibling
	 */
	public class LeafNode extends Node implements Serializable {
		int maxNumPairs;
		int minNumPairs;
		int numPairs;
		LeafNode leftSibling;
		LeafNode rightSibling;
		// why an array list because a node may hold multiple keys
		DictionaryPair[] dictionary;

		public String toString() {
			String res = "";
			LeafNode n = this;
			LeafNode x = n.leftSibling;
			LeafNode y = n.rightSibling;

			for (int i = 0; i < n.dictionary.length; i++)
				res = res + n.dictionary[i] + "      ";

			return res;
		}

		/**
		 * Given an index, this method sets the dictionary pair at that index
		 * within the dictionary to null.
		 * 
		 * @param index: the location within the dictionary to be set to null
		 * @return DictionaryPair just deleted
		 */
		public DictionaryPair delete(int index) {

			// get DictionaryPair
			DictionaryPair dp = this.dictionary[index];

			// Delete dictionary pair from leaf
			this.dictionary[index] = null;

			// Decrement numPairs
			numPairs--;

			return dp;
		}

		/**
		 * This method attempts to insert a dictionary pair within the dictionary
		 * of the LeafNode object. If it succeeds, numPairs increments, the
		 * dictionary is sorted, and the boolean true is returned. If the method
		 * fails, the boolean false is returned.
		 * 
		 * @param dp: the dictionary pair to be inserted
		 * @return a boolean indicating whether or not the insert was successful
		 */
		public boolean insert(DictionaryPair dp) {
			if (this.isFull()) {

				/* Flow of execution goes here when numPairs == maxNumPairs */

				return false;
			} else {

				// Insert dictionary pair, increment numPairs, sort dictionary
				this.dictionary[numPairs] = dp;
				numPairs++;
				Arrays.sort(this.dictionary, 0, numPairs);

				return true;
			}
		}

		/**
		 * This simple method determines if the LeafNode is deficient, i.e.
		 * the numPairs within the LeafNode object is below minNumPairs.
		 * 
		 * @return a boolean indicating whether or not the LeafNode is deficient
		 */
		public boolean isDeficient() {
			return numPairs < minNumPairs;
		}

		/**
		 * This simple method determines if the LeafNode is full, i.e. the
		 * numPairs within the LeafNode is equal to the maximum number of pairs.
		 * 
		 * @return a boolean indicating whether or not the LeafNode is full
		 */
		public boolean isFull() {
			return numPairs == maxNumPairs;
		}

		/**
		 * This simple method determines if the LeafNode object is capable of
		 * lending a dictionary pair to a deficient leaf node. The LeafNode
		 * object can lend a dictionary pair if its numPairs is greater than
		 * the minimum number of pairs it can hold.
		 * 
		 * @return a boolean indicating whether or not the LeafNode object can
		 *         give a dictionary pair to a deficient leaf node
		 */
		public boolean isLendable() {
			return numPairs > minNumPairs;
		}

		/**
		 * This simple method determines if the LeafNode object is capable of
		 * being merged with, which occurs when the number of pairs within the
		 * LeafNode object is equal to the minimum number of pairs it can hold.
		 * 
		 * @return a boolean indicating whether or not the LeafNode object can
		 *         be merged with
		 */
		public boolean isMergeable() {
			return numPairs == minNumPairs;
		}

		/**
		 * Constructor
		 * 
		 * @param m:  order of B+ tree that is used to calculate maxNumPairs and
		 *            minNumPairs
		 * @param dp: first dictionary pair insert into new node
		 */
		public LeafNode(int m, DictionaryPair dp) {
			this.maxNumPairs = m - 1;
			this.minNumPairs = (int) (Math.ceil(m / 2) - 1);
			this.dictionary = new bplustree.DictionaryPair[m];
			this.numPairs = 0;
			this.insert(dp);
		}

		/**
		 * Constructor
		 * 
		 * @param dps:    list of DictionaryPair objects to be immediately inserted
		 *                into new LeafNode object
		 * @param m:      order of B+ tree that is used to calculate maxNumPairs and
		 *                minNumPairs
		 * @param parent: parent of newly created child LeafNode
		 */
		public LeafNode(int m, DictionaryPair[] dps, InternalNode parent) {
			this.maxNumPairs = m - 1;
			this.minNumPairs = (int) (Math.ceil(m / 2) - 1);
			this.dictionary = dps;
			this.numPairs = linearNullSearch(dps);
			this.parent = parent;
		}
	}

	/**
	 * This class represents a dictionary pair that is to be contained within the
	 * leaf nodes of the B+ tree. The class implements the Comparable interface
	 * so that the DictionaryPair objects can be sorted later on.
	 */
	public class DictionaryPair implements Comparable<DictionaryPair>, Serializable {
		private K key;
		ArrayList<String> value;

		public String toString() {
			return "KEY : " + key + "  VALUE :" + value;
		}

		/**
		 * Constructor
		 * 
		 * @param key:   the key of the key-value pair
		 * @param value: the value of the key-value pair
		 */
		public DictionaryPair(K key, ArrayList<String> value) {
			this.key = key;
			this.value = value;
		}

		public K getKey() {
			return key;
		}

		/**
		 * This is a method that allows comparisons to take place between
		 * DictionaryPair objects in order to sort them later on
		 * 
		 * @param o
		 * @return
		 */
		@Override
		public int compareTo(DictionaryPair o) {
			return this.key.compareTo(o.getKey());
		}

	}
}