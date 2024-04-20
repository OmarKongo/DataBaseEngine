package DataBaseEngine.DB;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Vector;

public class Tuple extends Page implements Comparable<Object>, Serializable {
	private Hashtable<String, Object> attributesInTuple = new Hashtable<String, Object>();
	private String primaryKey;

	public String getStrPrimaryKey() {
		return primaryKey;
	}

	public void setStrPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public Tuple(String primaryKey, Enumeration<String> keys, Enumeration<Object> values) {
		this.primaryKey = primaryKey;
		while (keys.hasMoreElements())
			this.attributesInTuple.put(keys.nextElement(), values.nextElement());

	}

	public Hashtable<String, Object> getAttributesInTuple() {
		return attributesInTuple;
	}

	public String toString() {
		String res = "";
		Enumeration<Object> en = getAttributesInTuple().elements();

		while (en.hasMoreElements()) {
			Object val = en.nextElement();
			res = val + res;
			if (en.hasMoreElements()) {
				res = "," + res;
			}

		}
		return res;
	}

	@Override
	public int compareTo(Object o) {
		// System.out.println("hena");
		//Tuple T = (Tuple) o;
		Object x = this.getAttributesInTuple().get(this.getStrPrimaryKey());
	 	//Object y = (T.getAttributesInTuple().get(T.getStrPrimaryKey()));
		return DBApp.compareValue(x , o);
	}

	public Object getPK() {
		Object pk = this.getAttributesInTuple().get(this.getStrPrimaryKey());
		return pk;
	}

	public int getIndex(Vector<Tuple> v) {
		int index = Collections.binarySearch(v, this.getPK());
		index = -1 * (index + 1);
		// System.out.println("index : "+index +" key : "+this.getPK());
		return index;

	}

	public Page addTuple(Page page) throws IOException {

		int index = this.getIndex(page.getTuplesInPage());
		page.getTuplesInPage().add(index, this);
		return page;
	}

	public int getActualIndex(Vector<Tuple> v) {
		int index = Collections.binarySearch(v, this.getPK());
		return index;

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Tuple updateTuple(String pageName, Hashtable<String, String> indexes,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		String indexName = null;
		Object newKey = null;
		bplustree btree = null;
		String indx = null;
		Enumeration<String> keys = null;
		Object oldKey = null;
		ArrayList<String> pageNames = null;
		ArrayList<String> currPage = new ArrayList<String>();
		currPage.add(pageName);
		keys = htblColNameValue.keys();
		while (keys.hasMoreElements()) {
			indx = keys.nextElement();
			indexName = indexes.get(indx);
			oldKey = this.getAttributesInTuple().get(indx);
			newKey = htblColNameValue.get(indx);
			if (indexName != null) {
				btree = Deserialize.Index(indexName);

				btree.update((Comparable) oldKey, currPage, (Comparable) newKey);
				System.out.println(oldKey + " Updated to " + newKey + " in " + indx + " index");
				Serialize.Index(btree, indexName);

			}
			this.getAttributesInTuple().put(indx, newKey);

		}

		return this;
	}

}
