package DataBaseEngine.DB;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

public class Table implements Serializable {
	// Is it correct that only the pageFileName are persistent?

	/**
	 * 
	 */
	private static final long serialVersionUID = 9059644501928761439L;
	/**
	 * 
	 */
	// private static final long serialVersionUID = 1L;
	private Hashtable<String, Pair> props;
	private Vector<Page> pages;
	private String strTableName;
	private String strClusteringKeyColumn;
	// tranisent since already in metadata?
	private transient Hashtable<String, String> htblColNameType;
	private int pagesCounter;

	public Table() {

	}

	public Table(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) {
		pagesCounter = 1;
		pages = new Vector<Page>();
		props = new Hashtable<String, Pair>();
		this.strTableName = strTableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.htblColNameType = htblColNameType;

	}

	public Hashtable<String, Pair> getProps() {
		return props;
	}

	public Vector<Page> getPages() {
		return pages;
	}

	public int getPagesCounter() {
		return pagesCounter;
	}

	public void setPagesCounter(int pagesCounter) {
		this.pagesCounter = pagesCounter;
	}

	public String getStrTableName() {
		return strTableName;
	}

	public void setStrTableName(String strTableName) {
		this.strTableName = strTableName;
	}

	public String getStrClusteringKeyColumn() {
		return strClusteringKeyColumn;
	}

	public void setStrClusteringKeyColumn(String strClusteringKeyColumn) {
		this.strClusteringKeyColumn = strClusteringKeyColumn;
	}

	public Hashtable<String, String> getHtblColNameType() {
		return htblColNameType;
	}

	public static void addTable(String tableName, String pK, Hashtable<String, String> htblColNameType,
			String filePath) {
		try {
			checkDataType(htblColNameType, filePath, tableName);
			CSVWriter writer = new CSVWriter(new FileWriter(filePath, true));
			Enumeration<String> types = htblColNameType.elements();
			Enumeration<String> keys = htblColNameType.keys();
			String[] line = new String[6];

			while (types.hasMoreElements()) {
				line = insertLine(line, tableName, keys.nextElement(), types.nextElement(), pK);
				writer.writeNext(line);
			}
			writer.flush();
			writer.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	public String addIndex(String tableName, String column, String indexName, String filePath) {
		String type = "";
		try {

			CSVReader reader2 = new CSVReaderBuilder(new FileReader(filePath))
					.withSkipLines(0)
					.build();
			List<String[]> allElements = reader2.readAll();
			CSVWriter writer = new CSVWriter(new FileWriter(filePath));

			boolean flag = false;
			for (String[] nextRecord : allElements) {
				if (nextRecord[0].equals(tableName) & nextRecord[1].equals(column)) {
					if (nextRecord[4].equals("") & nextRecord[5].equals("")) {
						nextRecord[4] = indexName;
						nextRecord[5] = "B+ tree";
						type = nextRecord[2];
						writer.writeAll(allElements);
						writer.flush();
						flag = true;
						break;
					} else
						throw new DBAppException("Index already created ");

				}

			}

			if (!flag)
				throw new DBAppException("Invalid Table or Column");

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return type;

	}

	public static void checkDataType(Hashtable<String, String> htblColNameType, String filePath, String tableName)
			throws DBAppException, IOException {
		for (String s : htblColNameType.values()) {
			if (!(s.equals("java.lang.Integer") || s.equals("java.lang.String") || s.equals("java.lang.Double"))) {
				throw new DBAppException("Column DataType invalid");
			}
		}
		CSVReader csvReader = null;
		try {
			csvReader = new CSVReaderBuilder(new FileReader(filePath))
					.withSkipLines(1)
					.build();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] nextRecord;
		boolean flag = false;

		// this loop iterate over the csv file untill the table is founded
		// if it's founded then an exception will be thrown
		while ((nextRecord = csvReader.readNext()) != null) {

			if (nextRecord[0].equals(tableName))
				throw new DBAppException("Duplicate Table");

			else
				continue;

		}
	}

	public static String[] insertLine(String[] line, String tableName, String name, String type, String primaryKey) {

		line[0] = tableName;
		line[1] = name;
		line[2] = type;
		if (primaryKey.equals(name))
			line[3] = "True";
		else
			line[3] = "False";
		line[4] = null;
		line[5] = null;

		return line;
	}

	public static Hashtable<String, String> checkData(String tableName, Hashtable<String, Object> data, String filePath)
			throws Exception {

		Enumeration<Object> values = data.elements();
		Enumeration<String> keys = data.keys();

		ArrayList<Hashtable<String, String>> res = outputIndicies(tableName, filePath);
		Hashtable<String, String> indexes = res.get(0);
		Hashtable<String, String> collector = res.get(1);

		// this loop checks the entered data and throws exception if anything mismatches
		// the original attributes
		while (keys.hasMoreElements()) {
			String key = keys.nextElement();
			if (collector.containsKey(key)) {
				if (!(values.nextElement().getClass().equals(Class.forName(collector.get(key)))))
					throw new DBAppException("Mismatch type");

			} else{
				throw new Exception("mismatch key  " + key);}

		}
		return indexes;
	}

	public static ArrayList<Hashtable<String, String>> outputIndicies(String tableName, String filePath)
			throws Exception {
		ArrayList<Hashtable<String, String>> res = new ArrayList<Hashtable<String, String>>();
		String[] nextRecord;
		boolean flag = false;
		Hashtable<String, String> indexes = new Hashtable<String, String>();
		Hashtable<String, String> collector = new Hashtable<String, String>();
		CSVReader csvReader = new CSVReaderBuilder(new FileReader(filePath))
				.withSkipLines(0)
				.build();
		// this loop iterate over the csv file untill the table is founded
		// if it's not founded then an exception will be thrown
		while ((nextRecord = csvReader.readNext()) != null) {

			if (nextRecord[0].equals(tableName)) {
				flag = true;
				break;
			} else
				continue;
		}
		if (!flag)
			throw new Exception("Invalid Table");

		// this loop insert all the original attributes of the table inside collector
		// hashtable
		while (nextRecord != null && nextRecord[0].equals(tableName)) {
			collector.put(nextRecord[1], nextRecord[2]);

			if (nextRecord[4] != "")
				indexes.put(nextRecord[1], nextRecord[4]);
			nextRecord = csvReader.readNext();

		}
		res.add(indexes);
		res.add(collector);
		return res;
	}

	public String getPkType(String filePath) throws Exception {
		String tableName = this.getStrTableName();
		String pk = this.getStrClusteringKeyColumn();

		CSVReader csvReader = new CSVReaderBuilder(new FileReader(filePath))
				.withSkipLines(1)
				.build();
		String[] nextRecord;

		// this loop iterate over the csv file untill the table is founded
		// if it's not founded then an exception will be thrown
		while ((nextRecord = csvReader.readNext()) != null) {

			if (nextRecord[0].equals(tableName) & nextRecord[1].equals(pk)) {
				pk = nextRecord[2];
				break;
			} else
				continue;
		}
		return pk;

	}

	public String setNameForpage() {
		int counter = this.getPagesCounter();
		String pageName = this.getStrTableName() + counter;
		this.setPagesCounter(++counter);
		return pageName;
	}

	public static Object baseType(Object key) {

		if ((key instanceof String)) {
			return "";
		} else if (key instanceof Double)
			return 0.0;
		else
			return 0;

	}

	public static bplustree btreeType(String type) {
		bplustree btree = null;

		if (type.equals("java.lang.Integer"))
			btree = new bplustree<Integer>(Integer.class, 3);

		else if (type.equals("java.lang.String"))
			btree = new bplustree<String>(String.class, 3);

		else
			btree = new bplustree<Double>(Double.class, 3);
		return btree;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void addInBtree(Tuple T, Page p, Hashtable<String, String> indexes, boolean firstInsert) {
		ArrayList<String> pages = new ArrayList<String>();
		for (String key : indexes.keySet()) {
			bplustree btree = Deserialize.Index(indexes.get(key));
			// System.out.println(indexes.get(key));
			pages.add(p.getName());
			Object attr = T.getAttributesInTuple().get(key);
			btree.insert((Comparable) attr, pages);
			if (firstInsert)
				System.out.println(attr + " added to " + key + " index");
			Serialize.Index(btree, indexes.get(key));
			pages.clear();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void deleteFromIndex(Tuple T, Page p, Hashtable<String, String> indexes, boolean insert) {
		String pageName = null;
		ArrayList<String> pages = new ArrayList<String>();
		for (String key : indexes.keySet()) {
			bplustree btree = Deserialize.Index(indexes.get(key));
			pageName = p.getName();
			pages.add(pageName);
			Object attr = T.getAttributesInTuple().get(key);
			btree.delete((Comparable) attr, pages);
			if (!insert)
				System.err.println(attr + " deleted from " + key + " index");
			Serialize.Index(btree, indexes.get(key));
			pages.clear();
		}
	}

	public Table insertIntoTable(Tuple T, Hashtable<String, String> indexes) throws Exception {
		String tableName = this.getStrTableName();
		Table table = Deserialize.Table(tableName);
		// System.out.println(table.getProps());
		Page p = null;
		String pageName;
		boolean flag = false;
		Object maxKeyInPreviousPage;// this attribute connect the pages with each other maxLastpage = minNextPage
		// Base Case

		if (table.getPages().size() == 0) {
			p = new Page(table.setNameForpage());
			p.getPageProp().put(p.getName(), new Pair(baseType(T.getPK())));
			// table.getProps().put(p.getName(),new Pair(baseType(T.getPK())));

			T.addTuple(p);
			System.out.println("KEY : " + T.getPK() + " added to " + p.getName());
			p = p.updateMax();
			table.getPages().add(p);
			table.getProps().put(p.getName(), p.getPageProp().get(p.getName()));
			Serialize.Table(table, tableName);
			addInBtree(T, p, indexes, true);

			pageName = p.getName();
			Serialize.Page(p, pageName);

		}

		else { // Search for the right page to insert in
				// table = Deserialize.Table(tableName);
			int index = table.binarySearch(T.getPK(), true);
			pageName = table.getPages().elementAt(index).getName();
			p = Deserialize.Page(pageName);
			// p = table.getPages().elementAt(index);
			if (p.tupleFounded(T.getPK()))
				throw new DBAppException("Duplicate Tuple");
			p = T.addTuple(p);
			System.out.println("KEY : " + T.getPK() + " added to " + p.getName());
			if (!p.overFlowed()) {
				p = p.updateMax();

				table.getProps().put(p.getName(), p.getPageProp().get(p.getName()));
				Serialize.Table(table, tableName);
				addInBtree(T, p, indexes, true);
				Serialize.Page(p, pageName);

				// ADDED SUCCESSFULLY
			} else {
				// OVERFLOW ON PAGE BUT STILL ADDED
				System.out.println("Hi, from solution: "+p.getName());
				addInBtree(T, p, indexes, true);
				T = p.getTuplesInPage().remove(p.getMaxCount());
				deleteFromIndex(T, p, indexes, true);
				p = p.updateMax();
				table.getProps().put(p.getName(), p.getPageProp().get(p.getName()));
				Serialize.Table(table, tableName);
				pageName = p.getName();
				maxKeyInPreviousPage = p.getTuplesInPage().lastElement().getPK();
				Serialize.Page(p, pageName);

				// OVERFLOW ON THE LAST PAGE
				if (index == table.getPages().size() - 1) { // if the overflow in the last page, I will create a new
					System.out.println("I enter here");								// page without going through any loops
					table = Deserialize.Table(tableName);
					p = new Page(table.setNameForpage());
					p.getPageProp().put(p.getName(), new Pair(baseType(T)));

					p = p.updateMin(maxKeyInPreviousPage);
					T.addTuple(p);
					addInBtree(T, p, indexes, false);
					p = p.updateMax();
					table.getPages().add(p);
					table.getProps().put(p.getName(), p.getPageProp().get(p.getName()));
					Serialize.Table(table, tableName);
					pageName = p.getName();
					Serialize.Page(p, pageName);
				}

				else { // handle the overflow of pages by setting flag = true
						/// if flag is still false this means that the last page has an overflow
					while (index != table.getPages().size() - 1) {
						table = Deserialize.Table(tableName);
						pageName = table.getPages().elementAt(index + 1).getName();
						// System.out.println(pageName);
						p = Deserialize.Page(pageName);
						T.addTuple(p);
						if (!p.overFlowed()) {
							addInBtree(T, p, indexes, false);

							p = p.updateMin(maxKeyInPreviousPage);
							p = p.updateMax();
							table.getProps().put(p.getName(), p.getPageProp().get(p.getName()));
							Serialize.Table(table, tableName);
							pageName = p.getName();
							Serialize.Page(p, pageName);

							flag = true;
							break;
						} else {
							addInBtree(T, p, indexes, false);
							T = p.getTuplesInPage().remove(p.getMaxCount());
							deleteFromIndex(T, p, indexes, true);
							p = p.updateMax();
							p = p.updateMin(maxKeyInPreviousPage);
							table.getProps().put(p.getName(), p.getPageProp().get(p.getName()));
							Serialize.Table(table, tableName);
							maxKeyInPreviousPage = p.getTuplesInPage().lastElement().getPK();
							pageName = p.getName();
							Serialize.Page(p, pageName);

						}
						index++;
					}
					if (!flag) { // this if handles the last overflow can happen
						table = Deserialize.Table(tableName);
						p = new Page(table.setNameForpage());
						p.getPageProp().put(p.getName(), new Pair(baseType(T)));
						p = p.updateMin(maxKeyInPreviousPage);
						T.addTuple(p);
						addInBtree(T, p, indexes, false);
						p = p.updateMax();
						table.getPages().add(p);
						table.getProps().put(p.getName(), p.getPageProp().get(p.getName()));
						Serialize.Table(table, tableName);

						pageName = p.getName();
						Serialize.Page(p, pageName);
					}
				}

			}
		}
		return table;

	}

	public boolean hasPK(Hashtable<String, Object> h) {
		if (h.containsKey(this.getStrClusteringKeyColumn()))
			return true;
		return false;
	}

	public static boolean hasIndex(Hashtable<String, String> indexes, String strFromHshTblCol) {

		if (indexes.containsKey(strFromHshTblCol))
			return true;
		return false;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Page searchPK(Object key, boolean hasIndex, String indexName) throws DBAppException {
		Page p = null;
		if (hasIndex) {
			bplustree bTree = Deserialize.Index(indexName);
			ArrayList<String> page = bTree.search((Comparable) key);
			bTree.delete((Comparable) key, page);
			p = Deserialize.Page(page.get(0));
		} else {

			int index = 0;
			try {
				// index = t.binarySearch(key);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// p = Deserialize.Page(t.getPages().get(index).getName());
		}
		return p;
	}

	public void deleteTupleUsingKey(Page p, Tuple t) throws Exception {
		String pageName = null;
		Page prevPage = null;
		Page nextPage = null;
		int pageIndex = this.binarySearch(t.getPK(), false);
		String tableName = this.getStrTableName();
		// int index = Collections.binarySearch(p.getTuplesInPage(),key);
		// if(index<0)
		Object key = t.getPK();
		// throw new DBAppException("Key not Found");
		p.getTuplesInPage().remove(t);
		System.err.println("Primary Key : " + key + " deleted form " + p.getName());
		if (p.getTuplesInPage().size() > 0) {
			p.updateMax();
			this.getProps().put(p.getName(), p.getPageProp().get(p.getName()));
			Serialize.Table(this, tableName);
			Serialize.Page(p, p.getName());
			if (!(pageIndex == this.getPages().size() - 1)) {
				nextPage = this.getPages().get(pageIndex + 1);
				pageName = nextPage.getName();
				nextPage = Deserialize.Page(pageName);
				nextPage.updateMin(p.getPageProp().get(p.getName()).getMax());
				this.getProps().put(nextPage.getName(), nextPage.getPageProp().get(nextPage.getName()));
				Serialize.Table(this, tableName);
				Serialize.Page(nextPage, pageName);
			}

		} else {
			if (pageIndex == 0 & !(pageIndex == this.getPages().size() - 1)) {
				nextPage = this.getPages().get(pageIndex + 1);
				pageName = nextPage.getName();
				nextPage = Deserialize.Page(pageName);
				nextPage.updateMin(baseType(t.getPK()));
				this.getProps().put(pageName, nextPage.getPageProp().get(pageName));
				Serialize.Table(this, tableName);
				Serialize.Page(nextPage, pageName);
			} else {
				if (!(pageIndex == this.getPages().size() - 1)) {

					prevPage = this.getPages().get(pageIndex - 1);
					pageName = prevPage.getName();
					prevPage = Deserialize.Page(pageName);

					nextPage = this.getPages().get(pageIndex + 1);
					pageName = nextPage.getName();
					nextPage = Deserialize.Page(pageName);

					nextPage.updateMin(prevPage.getPageProp().get(prevPage.getName()).getMax());
					this.getProps().put(nextPage.getName(), nextPage.getPageProp().get(nextPage.getName()));
					Serialize.Table(this, tableName);
					Serialize.Page(nextPage, pageName);

				}

			}
			this.updatepages(p, pageIndex);

		}
		System.out.println(key + "  deleted");

	}

	public void updatepages(Page p, int pageIndex) {

		String pageName = p.getName();
		this.getPages().remove(pageIndex);
		this.getProps().remove(pageName);

		this.deleteFile(pageName);
		String tableName = this.getStrTableName();
		Serialize.Table(this, tableName);
		// return this;
	}

	public void deleteFile(String pageName) {
		String path = "Pages/" + pageName + ".ser";
		File file = new File(path);
		if (file.delete())
			System.err.println(pageName + " deleted");
		else
			System.err.println("file cannot be deleted");

	}

	public static boolean tupleMatched(Tuple t, Hashtable<String, Object> htblColNameValue) {
		Enumeration<String> attributes = htblColNameValue.keys();
		while (attributes.hasMoreElements()) {

			if (!(t.getAttributesInTuple().contains(htblColNameValue.get(attributes.nextElement()))))
				return false;

		}
		return true;
	}

	public void iterateOverChosenPages(Set<String> pageNames, Hashtable<String, Object> htblColNameValue,
			Hashtable<String, String> indexes) throws DBAppException {
		Page p = null;// Tuple t = null;//Vector<Tuple> v = new Vector<Tuple>();

		for (String name : pageNames) {
			p = Deserialize.Page(name);
			List<Tuple> tuples = new ArrayList<Tuple>(p.getTuplesInPage());
			// Iterator<Tuple> iter = v.iterator();
			for (Tuple t : tuples) {
				// t = iter.next();
				if (tupleMatched(t, htblColNameValue)) {
					try {
						this.deleteTupleUsingKey(p, t);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (indexes.size() != 0)
						deleteFromIndex(t, p, indexes, false);
				}
			}

		}

	}

	public static boolean hasAnyIndex(Hashtable<String, Object> htblColNameValue, Hashtable<String, String> indexes) {

		for (String key : htblColNameValue.keySet()) {
			if (indexes.containsKey(key))
				return true;
		}
		return false;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void deleteFromTable(Hashtable<String, Object> htblColNameValue, Hashtable<String, String> indexes)
			throws Exception {
		Page p = null;
		String pageName = null;
		int index;
		Tuple t = null;
		boolean flag = false;
		bplustree btree = null;
		String indexName = null;
		ArrayList<String> tuples = new ArrayList<String>();
		Object key = null;
		String indx = null;
		ArrayList<String> currIndex = null;
		Set<String> finalTuples = null;
		// First Case
		if (this.hasPK(htblColNameValue)) {
			key = htblColNameValue.get(this.getStrClusteringKeyColumn());
			if (!hasIndex(indexes, this.getStrClusteringKeyColumn())) {
				index = this.binarySearch(htblColNameValue.get(this.getStrClusteringKeyColumn()), false);
				pageName = this.getPages().elementAt(index).getName();
			} else {
				flag = true;
				indexName = indexes.get(this.getStrClusteringKeyColumn());
				btree = Deserialize.Index(indexName);
				pageName = (String) btree.search((Comparable) key).get(0);
			}
			p = Deserialize.Page(pageName);
			index = Collections.binarySearch(p.getTuplesInPage(), key);
			if (index < 0)
				throw new DBAppException("key not found");
			t = p.getTuplesInPage().elementAt(index);

			if (tupleMatched(t, htblColNameValue)) {
				deleteFromIndex(t, p, indexes, false);
				this.deleteTupleUsingKey(p, t);

			} else
				throw new DBAppException("Record not found");
		}
		// second case
		else if (indexes.size() != 0 & hasAnyIndex(htblColNameValue, indexes)) {
			Enumeration<String> keys = htblColNameValue.keys();
			while (keys.hasMoreElements()) {
				indx = keys.nextElement();
				indexName = indexes.get(indx);
				if (indexName != null) {
					btree = Deserialize.Index(indexName);
					key = htblColNameValue.get(indx);
					if (tuples.isEmpty())
						tuples = btree.search((Comparable) key);
					else {
						currIndex = btree.search((Comparable) key);
						tuples.retainAll(currIndex);
					}
				}
			}

			if (tuples.size() == 0)
				throw new DBAppException("Invalid Enteries");
			finalTuples = new HashSet<String>(tuples);
			this.iterateOverChosenPages(finalTuples, htblColNameValue, indexes);
		}
		// Third case
		else {
			finalTuples = new HashSet<String>(this.getProps().keySet());
			this.iterateOverChosenPages(finalTuples, htblColNameValue, indexes);
		}
	}

	public static Object getOriginalKey(String strClusteringKeyValue, String type) {
		Object key = null;

		if (type.equals("java.lang.Integer"))
			key = Integer.parseInt(strClusteringKeyValue);

		else if (type.equals("java.lang.String"))
			key = Double.parseDouble(strClusteringKeyValue);

		else
			key = strClusteringKeyValue;
		return key;
	}

	@SuppressWarnings("rawtypes")
	public void updateTable(Object key, Hashtable<String, String> indexes, Hashtable<String, Object> htblColNameValue)
			throws Exception {
		Page p = null;
		String pageName = null;
		String tableName = this.getStrTableName();
		int index = this.binarySearch(key, false);
		ArrayList<String> updateTree = new ArrayList<String>();
		pageName = this.getPages().elementAt(index).getName();
		String indexName = null;
		bplustree btree = null;
		p = Deserialize.Page(pageName);
		if (!p.tupleFounded(key))
			throw new DBAppException("Record not found");

		int tupleIndex;
		List<Tuple> tuples = new ArrayList<Tuple>(p.getTuplesInPage());
		// Iterator<Tuple> iter = v.iterator();
		for (Tuple t : tuples) {
			// t = iter.next();
			if (t.getPK().equals(key)) {
				try {
					tupleIndex = t.getActualIndex(p.getTuplesInPage());
					p.getTuplesInPage().set(tupleIndex, t.updateTuple(p.getName(), indexes, htblColNameValue));
					System.out.println("KEY : " + key + " Updated Correctly");
					Serialize.Page(p, pageName);
					Serialize.Table(this, tableName);

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}

	}

	// this method return the index of the right page to insert in
	public int binarySearch(Object key, Boolean insert) throws Exception {
		// Object key = T.getPK();
		int mid = 0;
		Vector<Page> v = this.getPages();
		int l = 0;
		int r = v.size() - 1;
		if (r == 0)
			return 0;

		while (l <= r) {
			mid = (l + r) / 2;

			Pair p = this.getProps().get(this.getPages().elementAt(mid).getName());
			// Page P = Deserialize.Page(v.elementAt(mid).getName());
			// Page P = this.getPages().elementAt(mid);
			// P.display();
			if (key instanceof Integer) {
				// int min = this.getPages().get(mid).getPageProp().get()
				int min = (int) p.getMin();
				int max = (int) p.getMax();
				if (min == (int) key || max == (int) key) {
					if (insert)
						throw new DBAppException("Duplicate Tuple");
					else {
						if (max == (int) key || mid == 0)
							return mid;
						else
							return mid - 1;

					}
				}

				if ((int) key > min & (int) key < max)
					return mid;
				else if (max > (int) key)
					r = mid - 1;
				else
					l = mid + 1;

			} else if (key instanceof Double) {
				Double min = (Double) p.getMin();
				Double max = (Double) p.getMax();
				if (min == (Double) key || max == (Double) key)
					throw new Exception("Duplicate Key");

				if ((Double) key > min & (Double) key < max)
					return mid;
				else if (max > (Double) key)
					r = mid - 1;
				else
					l = mid + 1;

			}

			else {
				String min = (String) p.getMin();
				String max = (String) p.getMax();
				if (min.equals(key) || max.equals(key))
					throw new Exception("Duplicate Key");

				if (((String) key).compareTo(min) > 0 & ((String) key).compareTo(max) < 0)
					return mid;
				else if (((String) key).compareTo(max) < 0)
					r = mid - 1;
				else
					l = mid + 1;

			}
		}

		return mid;
	}

	public ArrayList<Object> selectFromTableNoIndex(SQLTerm[] arrSQLTerms, String[] strarrOperators) {
		ArrayList<Object> res = new ArrayList<Object>();

		/*
		 * First case: Only one SQL term and no strarrOperators
		 * Implementing on:
		 * arrSQLTerms[0]._strTableName = "Student";
		 * arrSQLTerms[0]._strColumnName = "name";
		 * arrSQLTerms[0]._strOperator = "=";
		 * arrSQLTerms[0]._objValue = "John Noor";
		 */
		try {
			if (strarrOperators.length == 0 && arrSQLTerms.length == 1) {
				/*
				 * Check if the strColumn is a pk(clustering key) if yes --> Binarysearch
				 * (useful for =, >, >=)
				 * I think that for the operation !=, no binary search is needed, go through all
				 * records and include it except that one
				 */

				// make list iterator, give index, iterate forward and backwards
				// iterate through pages (for range queries) and within those iterations iterate
				// within the pages (through the tuples)

				if (arrSQLTerms[0]._strColumnName.equals(this.getStrClusteringKeyColumn())) {
					int pageIndex = this.binarySearch(arrSQLTerms[0]._objValue, false);
					// ListIterator<Page> listIterator = this.getPages().listIterator(pageIndex);

					switch (arrSQLTerms[0]._strOperator) {
						// O(log(n)*log(n))
						case "=":
							String pageName = this.getPages().elementAt(pageIndex).getName();
							Page p = Deserialize.Page(pageName);
							res.addAll(p.selectDistinctPK(arrSQLTerms, strarrOperators));
							Serialize.Page(p, p.getName());
							break;

						// O(log(n)*log(n) + N/3)
						case ">=":
						case ">": {
							int firstLoopMarker = 0;
							for (int i = pageIndex; i < this.getPages().size(); i++) {
								// I want to start printing from the tuple index (or not, if >) to the end
								Page p1 = Deserialize.Page(this.getPages().elementAt(i).getName());
								res.addAll(p1.selectRangePK(arrSQLTerms, strarrOperators, firstLoopMarker));
								Serialize.Page(p1, p1.getName());
								firstLoopMarker++;
							}
							break;
						}
						// O(N/3)
						case "<=":
						case "<":
						case "!=": {
							int firstLoopMarker = 0;
							for (Page page : this.getPages()) {
								Page p2 = Deserialize.Page(page.getName());
								res.addAll(p2.selectRangePK(arrSQLTerms, strarrOperators, firstLoopMarker));
								Serialize.Page(p2, p2.getName());
								firstLoopMarker++;
							}
							break;
						}

					}

				} else {
					// unfortunately have to iterate through all records since we are searching on a
					// (non-sorted column)
					for (Page p : this.getPages()) {
						Page p1 = Deserialize.Page(p.getName());
						res.addAll(p1.selectNoPK(arrSQLTerms, strarrOperators));
						Serialize.Page(p1, p1.getName());
					}

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return res;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ArrayList<Object> selectFromTableWithIndex(SQLTerm[] arrSQLTerms, String[] strarrOperators,
			Hashtable<String, String> indicies) {
		ArrayList<Object> res = new ArrayList<Object>();
		// indexes contains {gpa:gpaIndex} which is inside tableName inside SQLTerm (it
		// is always going to be the same table);
		try {
			// instead of this if should be a for loop iterating through the sql terms
			if (strarrOperators.length == 0 && arrSQLTerms.length == 1) {
				if (indicies.containsKey(arrSQLTerms[0]._strColumnName)) {
					String indexName = indicies.get(arrSQLTerms[0]._strColumnName);
					bplustree btree = Deserialize.Index(indexName);
					// value or key
					Object value = arrSQLTerms[0]._objValue;
					ArrayList<String> arr = new ArrayList<String>();

					switch (arrSQLTerms[0]._strOperator) {

						case "=":
							//must make sure of all pages output in arrayList
							arr = btree.search((Comparable) value);
							

							for (String pageName : arr) {
								Page p1 = Deserialize.Page(pageName);
								res.addAll(p1.selectNoPK(arrSQLTerms, strarrOperators));
								Serialize.Page(p1, p1.getName());
							}
							break;

						case ">=":
						case ">":
						case "<=":
						case "<":{
							arr = btree.rangeSearch((Comparable) value,arrSQLTerms[0]._strOperator);
							for (String pageName : arr) {
								Page p1 = Deserialize.Page(pageName);
								res.addAll(p1.selectNoPK(arrSQLTerms, strarrOperators));
								Serialize.Page(p1, p1.getName());
							}

							break;
						}
						//!= Does not benefift from B+tree so will iterate through everything
						case "!=": {
							for (Page p : this.getPages()) {
								Page p1 = Deserialize.Page(p.getName());
								res.addAll(p1.selectNoPK(arrSQLTerms, strarrOperators));
								Serialize.Page(p1, p1.getName());
							}
							break;
						}

					}

					Serialize.Index(btree, indexName);
				}
				else{
					selectFromTableNoIndex(arrSQLTerms, strarrOperators);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;

	}

	public static void main(String[] args) throws Exception {

	}

}
