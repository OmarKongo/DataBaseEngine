package DataBaseEngine.DB;

import java.io.*;

import java.util.*;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

public class Table implements Serializable {
	// Is it correct that only the pageFileName are persistent?

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
		this.strTableName = strTableName;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.htblColNameType = htblColNameType;

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

	public void addTable(String tableName, String pK, String filePath) {
		try {
			checkDataType(this.htblColNameType, filePath, tableName);
			CSVWriter writer = new CSVWriter(new FileWriter(filePath, true));
			Enumeration<String> types = this.htblColNameType.elements();
			Enumeration<String> keys = this.htblColNameType.keys();
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

	public static void checkData(String tableName, Hashtable<String, Object> data, String filePath) throws Exception {
		CSVReader csvReader = new CSVReaderBuilder(new FileReader(filePath))
				.withSkipLines(1)
				.build();
		String[] nextRecord;
		boolean flag = false;
		Enumeration<Object> values = data.elements();
		Enumeration<String> keys = data.keys();
		Hashtable<String, String> collector = new Hashtable<String, String>();
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
			nextRecord = csvReader.readNext();

		}
		// this loop checks the entered data and throws exception if anything mismatches
		// the original attributes
		while (keys.hasMoreElements()) {
			String key = keys.nextElement();
			if (collector.containsKey(key)) {
				if (!(values.nextElement().getClass().equals(Class.forName(collector.get(key)))))
					throw new Exception("Mismatch type");

			} else
				throw new Exception("Mismatch key '" + key + "'");

		}
	}

	public static boolean checkIndex(String tableName, String filePath) throws Exception {

		CSVReader csvReader = new CSVReaderBuilder(new FileReader(filePath))
				.withSkipLines(1)
				.build();
		String[] nextRecord;
		boolean flag = false;
		boolean res = false;
		while ((nextRecord = csvReader.readNext()) != null) {

			if (nextRecord[0].equals(tableName)) {
				flag = true;
				if(!(nextRecord[4].equals(""))){
					res = true;
				}
				break;
			} else
				continue;
		}
		if (!flag)
			throw new Exception("Invalid Table");

		return res;
	}

	public String setNameForpage() {
		int counter = this.getPagesCounter();
		String pageName = this.getStrTableName() + counter;
		this.setPagesCounter(++counter);
		return pageName;
	}

	public static Object baseType(Tuple T) {
		Object key = T.getPK();
		if ((key instanceof String)) {
			return "";
		} else if (key instanceof Double)
			return 0.0;
		else
			return 0;

	}

	public Table insertIntoTable(Tuple T) throws Exception {
		Page p = null;
		String pageName;
		boolean flag = false;
		Object maxKeyInPreviousPage;// this attribute connect the pages with each other maxLastpage = minNextPage
		// Base Case

		if (this.getPages().size() == 0) {
			p = new Page(this.setNameForpage());
			p.getPageProp().put(p.getName(), new Pair(baseType(T)));
			this.getPages().add(p);
			T.addTuple(p);
			p = p.updateMax();

			Serialize.Page(p);

		}

		else { // Search for the right page to insert in

			int index = this.binarySearch(T);
			pageName = this.getPages().get(index).getName();
			p = Deserialize.Page(pageName);

			if (p.tupleFounded(T))
				throw new DBAppException("Duplicate Tuple");
			p = T.addTuple(p);
			if (!p.overFlowed()) {
				p = p.updateMax();
				Serialize.Page(p);
				// ADDED SUCCESSFULLY
			} else {
				// OVERFLOW ON PAGE BUT STILL ADDED
				T = p.getTuplesInPage().remove(p.getMaxCount());
				p = p.updateMax();
				maxKeyInPreviousPage = p.getTuplesInPage().lastElement().getPK();
				Serialize.Page(p);
				// OVERFLOW ON THE LAST PAGE
				if (index == this.getPages().size() - 1) { // if the overflow in the last page, I will create a new page
															// without going through any loops
					p = new Page(this.setNameForpage());
					p.getPageProp().put(p.getName(), new Pair(baseType(T)));
					this.getPages().add(p);
					p = p.updateMin(maxKeyInPreviousPage);
					T.addTuple(p);
					p = p.updateMax();
					Serialize.Page(p);
				}

				else { // handle the overflow of pages by setting flag = true
						/// if flag is still false this means that the last page has an overflow
					while (index != this.getPages().size() - 1) {
						pageName = this.getPages().get(index + 1).getName();
						// System.out.println(pageName);
						p = Deserialize.Page(pageName);
						T.addTuple(p);
						if (!p.overFlowed()) {
							p = p.updateMin(maxKeyInPreviousPage);
							p = p.updateMax();
							Serialize.Page(p);
							flag = true;
							break;
						} else {
							T = p.getTuplesInPage().remove(p.getMaxCount());
							p = p.updateMax();
							p = p.updateMin(maxKeyInPreviousPage);
							maxKeyInPreviousPage = p.getTuplesInPage().lastElement().getPK();
							Serialize.Page(p);
						}
						index++;
					}
					if (!flag) { // this if handles the last overflow can happen
						p = new Page(this.setNameForpage());
						p.getPageProp().put(p.getName(), new Pair(baseType(T)));
						this.getPages().add(p);
						p = p.updateMin(maxKeyInPreviousPage);
						T.addTuple(p);
						p = p.updateMax();
						Serialize.Page(p);
					}
				}

			}
		}
		return this;

	}

	public void updateRow() {

	}

	public void deleteRow() {

	}

	public void selectRow() {

	}

	// this method return the index of the right page to insert in
	public int binarySearch(Tuple T) throws Exception {
		Object key = T.getPK();
		int mid = 0;
		Vector<Page> v = this.getPages();
		int l = 0;
		int r = v.size() - 1;
		if (r == 0)
			return 0;

		while (l <= r) {
			mid = (l + r) / 2;

			// Page P = Deserialize.Page(v.get(mid).getName());
			Page P = this.getPages().get(mid);
			if (key instanceof Integer) {
				// int min = this.getPages().get(mid).getPageProp().get()
				int min = (int) P.getPageProp().get(P.getName()).getMin();
				int max = (int) P.getPageProp().get(P.getName()).getMax();
				if (min == (int) key || max == (int) key)
					throw new Exception("Duplicate Key");

				if ((int) key > min & (int) key < max)
					return mid;
				else if (max > (int) key)
					r = mid - 1;
				else
					l = mid + 1;

			} else if (key instanceof Double) {
				Double min = (Double) P.getPageProp().get(P.getName()).getMin();
				Double max = (Double) P.getPageProp().get(P.getName()).getMax();
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
				String min = (String) P.getPageProp().get(P.getName()).getMin();
				String max = (String) P.getPageProp().get(P.getName()).getMax();
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

	public static void main(String[] args) throws Exception {

	}

	public Iterator<Object> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'selectFromTable'");
	}

}
