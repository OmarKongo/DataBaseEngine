package DataBaseEngine.DB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable, Comparable<Object> {

	private String name;
	private Hashtable<String, Pair> pageProp;
	private Vector<Tuple> tuplesInPage = new Vector<Tuple>();

	public Page() {

	}

	public Page(String pageName) {
		this.pageProp = new Hashtable<String, Pair>();

		this.name = pageName;
	}

	public Hashtable<String, Pair> getPageProp() {
		return pageProp;
	}

	public void display() {
		System.out.print("Page [pageProp=" + pageProp + "]");
	}

	public String toString() {
		String res = "";
		for (int i = 0; i < tuplesInPage.size(); i++) {
			res = res + tuplesInPage.elementAt(i).toString();
			if (i != tuplesInPage.size() - 1) {
				res = res + ",";
			}
		}
		return res;
		// return "Page [pageProp=" + pageProp + "]";
	}

	public int getMaxCount() throws IOException {
		String filename = "DBApp.config";
		Properties prop = null;
		try (FileInputStream fis = new FileInputStream(filename)) {
			prop = new Properties();
			prop.load(fis);

		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		}
		String num = prop.getProperty("MaximumRowsCountinPage");
		return Integer.parseInt(num);

	}

	@Override
	public int compareTo(Object o) {

		Page P = (Page) o;
		Object fMin = this.getPageProp().get(this.getName()).getMin();
		Object sMin = P.getPageProp().get(P.getName()).getMin();

		return DBApp.compareValue(fMin, sMin);
	}

	public boolean tupleFounded(Tuple T) {
		int index = Collections.binarySearch(this.getTuplesInPage(), T);
		if (index < 0)
			return false;

		return true;

	}

	public void addNewPageToTheTable(Vector<Page> v) throws Exception {
		int index = Collections.binarySearch(v, this);

		index = -1 * (index + 1);
		v.add(index, this);

	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Vector<Tuple> getTuplesInPage() {
		return tuplesInPage;
	}

	public Page updateMax() {
		this.getPageProp().get(this.getName())
				.setMax(this.getTuplesInPage().lastElement().getPK());
		return this;
	}

	public Page updateMin(Object min) {
		this.getPageProp().get(this.getName()).setMin(min);
		return this;
	}

	public boolean overFlowed() throws IOException {
		return this.getTuplesInPage().size() > this.getMaxCount();

	}

	public ArrayList<Object> selectDistinctNoIndex(SQLTerm[] arrSQLTerms, String[] strarrOperators) {
		ArrayList<Object> res = new ArrayList<Object>();

		int tupleIndex = this.getTupleIndexUsingBS(arrSQLTerms);
		if (tupleIndex != -1)
			res.add(this.getTuplesInPage().elementAt(tupleIndex));

		return res;
	}

	public int getTupleIndexUsingBS(SQLTerm[] arrSQLTerms) {
		Hashtable<String, Object> attributesInTuple = new Hashtable<String, Object>();
		attributesInTuple.put(arrSQLTerms[0]._strColumnName, arrSQLTerms[0]._objValue);
		int tupleIndex = Collections.binarySearch(this.getTuplesInPage(),
				new Tuple(arrSQLTerms[0]._strColumnName, attributesInTuple.keys(),
						attributesInTuple.elements()));
		return tupleIndex;
	}

	public ArrayList<Object> selectNoIndexNoPK(SQLTerm[] arrSQLTerms, String[] strarrOperators) {
		ArrayList<Object> res = new ArrayList<Object>();
		String searchedColumn = arrSQLTerms[0]._strColumnName;
		Object searchedValue = arrSQLTerms[0]._objValue;
		for (Tuple t : this.getTuplesInPage()) {
			Object tupleObjectValue = t.getAttributesInTuple().get(searchedColumn);
			int comparison = DBApp.compareValue(tupleObjectValue, searchedValue);
			switch (arrSQLTerms[0]._strOperator) {
				case ">":
					if (comparison > 0) {
						res.add(t);
					}
					break;
				case ">=":
					if (comparison >= 0) {
						res.add(t);
					}
					break;
				case "<":
					if (comparison < 0) {
						res.add(t);
					}
					break;
				case "<=":
					if (comparison <= 0) {
						res.add(t);
					}
					break;
				case "!=":
					if (comparison != 0) {
						res.add(t);
					}
					break;
				case "=":
					if (comparison == 0) {
						res.add(t);
					}
					break;

			}
		}
		return res;
	}

	public ArrayList<Object> selectRangeNoIndexPK(SQLTerm[] arrSQLTerms, String[] strarrOperators,
			int firstLoopMarker) {
		ArrayList<Object> res = new ArrayList<Object>();
		String searchedColumn = arrSQLTerms[0]._strColumnName;
		Object searchedValue = arrSQLTerms[0]._objValue;
		int testCount = 0;
		switch (arrSQLTerms[0]._strOperator) {
			case ">":
			case ">=": {
				int j = 0;
				if (firstLoopMarker == 0) {
					j = this.getTupleIndexUsingBS(arrSQLTerms);
				}
				for (int i = j; i < this.getTuplesInPage().size(); i++) {
					testCount++;
					Tuple t = this.getTuplesInPage().elementAt(i);
					Object tupleObjectValue = t.getAttributesInTuple().get(searchedColumn);
					int comparison = DBApp.compareValue(tupleObjectValue, searchedValue);

					if (comparison != 0) {
						res.add(t);
					} else {
						if (arrSQLTerms[0]._strOperator.equals(">="))
							res.add(t);
					}
				}
			}
			case "<":
			case "<=":
			case "!=": {

				for (Tuple t : this.getTuplesInPage()) {
					testCount++;
					Object tupleObjectValue = t.getAttributesInTuple().get(searchedColumn);
					int comparison = DBApp.compareValue(tupleObjectValue, searchedValue);
					if (comparison != 0) {
						res.add(t);
					} else {
						if (arrSQLTerms[0]._strOperator.equals("<=")) {
							res.add(t);
							break;
						} else {
							if (arrSQLTerms[0]._strOperator.equals("<")) {
								break;
							}
						}

					}
				}

			}

		}

		System.out.println("test count is: " + testCount + " tuples in page is: " + this.getTuplesInPage().size());
		return res;
	}

	public static void main(String[] args) {
		Page p = new Page("s");
		int r = (int) (Math.random() * 5021100);

		System.out.print(r);

	}

}

class Pair implements Serializable {

	Object min, max;

	public Pair(Object start) {

		// this.max = max;
		this.min = start;
	}

	public Object getMin() {
		return min;
	}

	public void setMin(Object min) {
		this.min = min;
	}

	@Override
	public String toString() {
		return "Pair [min=" + min + ", max=" + max + "]";
	}

	public Object getMax() {
		return max;
	}

	public void setMax(Object max) {
		this.max = max;
	}

}