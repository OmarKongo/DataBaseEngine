package DataBaseEngine.DB;

/** * @author Wael Abouelsaadat */

import java.util.Iterator;
import java.util.Properties;
import java.util.Stack;

import javax.print.DocFlavor.STRING;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Hashtable;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

public class DBApp {
	final static String csvPath = "metadata.csv";

	public DBApp() {
		// this.init();
	}

	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup

	public void init() {
		CSVWriter writer = null;
		try {

			writer = new CSVWriter(new FileWriter(csvPath, true));
			String[] header = { "Table Name", "Column Name", "Column Type", "Clustering Key", "IndexName",
					"IndexType" };

			writer.writeNext(header);
			writer.flush();
			writer.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static int compareValue(Object o1, Object o2) {
		// System.out.println(o1.getClass()+""+o2.getClass());
		if (o1 instanceof Integer && o2 instanceof Integer) {
			int first = (int) o1;
			int second = (int) o2;
			// the first is greater than return >0 if equal 0 if less <0
			return first - second;

		} else {
			if (o1 instanceof Double && o2 instanceof Double) {
				Double first = (Double) o1;
				Double second = (Double) o2;
				// the first is greater than return 1
				Double res = first - second;
				if (res > 0)
					return (int) Math.ceil(res);
				else
					return (int) Math.floor(res);
			} else {
				String first = (String) o1;
				String second = (String) o2;
				/*
				 * An int value: 0 if the string is equal to the other string, ignoring case
				 * differences.
				 * < 0 if the string is lexicographically less than the other string
				 * > 0 if the string is lexicographically greater than the other string (more
				 * characters)
				 */
				return first.compareToIgnoreCase(second);
			}

		}
	}

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value

	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {

		Table.addTable(strTableName, strClusteringKeyColumn, htblColNameType, csvPath);
		Table T = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		Serialize.Table(T, strTableName);

	}

	// following method creates a B+tree index
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void createIndex(String strTableName,
			String strColName,
			String strIndexName) throws DBAppException, IOException {
		Table t = Deserialize.Table(strTableName);
		String type = t.addIndex(strTableName, strColName, strIndexName, csvPath);
		bplustree btree = Table.btreeType(type);
		ArrayList<String> pageName = new ArrayList<String>();
		Serialize.Index(btree, strIndexName);
		for (int i = 0; i < t.getPages().size(); i++) {

			Page p = Deserialize.Page(t.getPages().elementAt(i).getName());
			pageName.add(p.getName());
			for (int j = 0; j < p.getTuplesInPage().size(); j++) {
				btree = Deserialize.Index(strIndexName);
				Tuple T = p.getTuplesInPage().get(j);
				btree.insert((Comparable) T.getAttributesInTuple().get(strColName), pageName);
				Serialize.Index(btree, strIndexName);
			}

			pageName.clear();
		}

	}

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		Hashtable<String, String> indexes = null;
		try {
			indexes = Table.checkData(strTableName, htblColNameValue, csvPath);
		} catch (Exception e) {

			e.printStackTrace();
		}
		Table T = Deserialize.Table(strTableName);
		Tuple record = new Tuple(T.getStrClusteringKeyColumn(), htblColNameValue.keys(), htblColNameValue.elements());
		try {
			T = T.insertIntoTable(record, indexes);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Serialize.Table(T, strTableName);

	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName,
			String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Hashtable<String, String> indexes = null;

		try {
			indexes = Table.checkData(strTableName, htblColNameValue, csvPath);
			Table T = Deserialize.Table(strTableName);
			if (T.getPages().size() == 0)
				throw new DBAppException("Empty Table");
			String pkType = T.getPkType(csvPath);
			Object key = Table.getOriginalKey(strClusteringKeyValue, pkType);
			T.updateTable(key, indexes, htblColNameValue);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		Hashtable<String, String> indexes = null;
		try {
			indexes = Table.checkData(strTableName, htblColNameValue, csvPath);
		} catch (Exception e) {

			e.printStackTrace();
		}
		Table T = Deserialize.Table(strTableName);
		try {
			T.deleteFromTable(htblColNameValue, indexes);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public Iterator<Object> selectFromTable(SQLTerm[] arrSQLTerms,
			String[] strarrOperators) throws DBAppException {

		try {
			// if no exception is thrown, continue
			checkValidSQLTerm(arrSQLTerms, strarrOperators);
		} catch (Exception e) {
			e.printStackTrace();
		}
		String tableName = arrSQLTerms[0]._strTableName;
		Table t = Deserialize.Table(tableName);

		ArrayList<ArrayList<Object>> resultSetList = new ArrayList<>();

		for (SQLTerm sqlTerm : arrSQLTerms) {
			resultSetList.add(selectSQLTerm(sqlTerm,t));
		}

		System.out.println((resultSetList.size())+ "size");

		ArrayList<Object> infix = convertToInfix(resultSetList, strarrOperators);
		System.out.println(infix+" infix ");
		ArrayList<Object> postfix = infixToPostfix(infix);

		System.out.println(postfix+" postfix ");
		Stack<Object> stck = new Stack<>();

		for (int i = 0; i < postfix.size(); i++) {
			if (!(postfix.get(i).equals("OR") || postfix.get(i).equals("AND") || postfix.get(i).equals("XOR"))) {
				stck.push(postfix.get(i));
			} else {
				ArrayList<Object> sqlTerm1 = (ArrayList<Object>) stck.pop();
				ArrayList<Object> sqlTerm2 = (ArrayList<Object>) stck.pop();
				ArrayList<Object> intermediary = new ArrayList<>();
				switch ((String) postfix.get(i)) {
					case "AND":
						System.out.println(	"in and case");
						intermediary = and(sqlTerm2, sqlTerm1);
						System.out.println(intermediary+" intermediary");
						stck.push(intermediary);
						break;
					case "OR":
						intermediary = or(sqlTerm2, sqlTerm1);
						stck.push(intermediary);
						break;
					default:
						intermediary = xor(sqlTerm2, sqlTerm1);
						stck.push(intermediary);
						break;
				}
			}
		}
		ArrayList<Object> finalResult = (ArrayList<Object>) stck.pop();

		Iterator<Object> result = finalResult.iterator();
		Serialize.Table(t, t.getStrTableName());

		return result;
	}

	public static ArrayList<Object> selectSQLTerm(SQLTerm sqlTerm, Table t) {


		ArrayList<Object> resList = new ArrayList<>();
		try {
			Hashtable<String, String> indicies = Table.outputIndicies(t.getStrTableName(), csvPath).get(0);
			if (indicies.isEmpty()) {
				resList.addAll(t.selectFromTableNoIndex(sqlTerm));
			} else {
				resList.addAll(t.selectFromTableWithIndex(sqlTerm, indicies));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return resList;
	}

	public void checkValidSQLTerm(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		// need to check if SQLTerm tables are in MetaData File with correct data types
		// Do I make this Hashtable global so as to decrease spatial complexity by not
		// creating a hashtable everytime we select?

		try {
			/*
			 * Checking if valid table and valid columns
			 * Making sure that no joins are allowed
			 * Making sure the operators inside the SQLTerm are supported
			 * By this order
			 */
			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
			ArrayList<String> checkNoJoin = new ArrayList<>();
			ArrayList<String> checkForStrOperators = new ArrayList<>(Arrays.asList(">", ">=", "<", "<=", "!=", "="));
			for (SQLTerm sqlTerm : arrSQLTerms) {
				// should I say that if table names differ throw exception "joins not
				// supported?"
				htblColNameValue.put(sqlTerm._strColumnName, sqlTerm._objValue);
				Table.checkData(sqlTerm._strTableName, htblColNameValue, csvPath);
				htblColNameValue.clear();

				if (arrSQLTerms[0].equals(sqlTerm)) {
					checkNoJoin.add(sqlTerm._strTableName);
				} else {
					if (!(sqlTerm._strTableName.equals(checkNoJoin.get(0)))) {
						throw new DBAppException("Multiple table queries are not supported on this engine.");
					}
				}
				if (!(checkForStrOperators.contains(sqlTerm._strOperator))) {
					throw new DBAppException(
							"Please enter a valid string operator.\nSupported operators are >, >=, <, <=, != or =");
				}
			}
			/*
			 * Checking if valid operators
			 */
			for (String operator : strarrOperators) {
				if (operator != "AND" && operator != "OR" && operator != "XOR") {
					throw new DBAppException("Please enter a valid operator.\nOperators supported are AND, OR or XOR");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static ArrayList<Object> and(ArrayList<Object> a, ArrayList<Object> b) {
		ArrayList<Object> res = new ArrayList<>();
		res.addAll(a);
		System.out.println(res +"res 1");
		res.retainAll(b);
		System.out.println(b+" this is b");
		System.out.println(res +"res 2");
		return res;
	}

	// No dups
	public static ArrayList<Object> or(ArrayList<Object> a, ArrayList<Object> b) {
		ArrayList<Object> res = new ArrayList<>();
		res.addAll(a);
		if (!(res.containsAll(b))) {
			res.removeAll(b);
			res.addAll(b);
		}
		return res;
	}

	public static ArrayList<Object> xor(ArrayList<Object> a, ArrayList<Object> b) {
		// XORING b and d

		ArrayList<Object> temp1 = a;
		ArrayList<Object> temp2 = b;
		ArrayList<Object> res = or(a, b);
		res.removeAll(and(temp1, temp2));

		return res;

	}

	public static ArrayList<Object> convertToInfix(ArrayList<ArrayList<Object>> arrSQLTerms, String[] strarrOperators) {
		ArrayList<Object> res = new ArrayList<>();

		Hashtable<String, ArrayList<Integer>> indiciesOfOperators = new Hashtable<>();
		ArrayList<Integer> and = new ArrayList<>();
		ArrayList<Integer> or = new ArrayList<>();
		ArrayList<Integer> xor = new ArrayList<>();
		ArrayList<Integer> all = new ArrayList<>();
		for (int i = 0; i < arrSQLTerms.size(); i++) {
			res.add(arrSQLTerms.get(i));
			if (i != strarrOperators.length) {
				res.add(strarrOperators[i]);
				switch (strarrOperators[i]) {
					case "AND":
						and.add((2 * i) + 1);
						all.add((2 * i) + 1);
						indiciesOfOperators.put(strarrOperators[i], and);
						break;
					case "OR":
						or.add((2 * i) + 1);
						all.add((2 * i) + 1);
						indiciesOfOperators.put(strarrOperators[i], or);
						break;
					default:
						xor.add((2 * i) + 1);
						all.add((2 * i) + 1);
						indiciesOfOperators.put(strarrOperators[i], xor);
						break;
				}
			}
		}

		addBracketsNoPriority(res, all);

		return res;
	}

	public static void addBracketsNoPriority(ArrayList<Object> infixNoBrackets,
			ArrayList<Integer> indiciesOfOperators) {
		// this for loop is to add brackets around AND
		int index;

		for (int i = 0; i < indiciesOfOperators.size(); i++) {
			if (i != 0) {
				indiciesOfOperators.set(i, indiciesOfOperators.get(i) + 2 * i);
			}

			index = indiciesOfOperators.get(i);

			// adding left bracket
			if (infixNoBrackets.get(index - 1) != ")") {
				infixNoBrackets.add(index - 1, "(");

				infixNoBrackets.add(index + 3, ")");

			}

			else {
				infixNoBrackets.add(index - 1 - (3 * i), "(");

				infixNoBrackets.add(index + 3, ")");

			}

		}

	}

	public static ArrayList<Object> infixToPostfix(ArrayList<Object> infix) {
		Stack<Object> stck = new Stack<>();
		ArrayList<Object> postfix = new ArrayList<>();
		for (int i = 0; i < infix.size(); i++) {
			if (infix.get(i) != "AND" && infix.get(i) != "OR" && infix.get(i) != "XOR" && infix.get(i) != "("
					&& infix.get(i) != ")") {
				postfix.add(infix.get(i));
			} else {
				if (infix.get(i).equals(")")) {
					while (stck.peek() != "(")
						postfix.add(stck.pop());
					stck.pop();
				} else {
					stck.push(infix.get(i));
				}

			}

		}
		return postfix;

	}

	public static int priority(String operator) {
		switch (operator) {
			case "AND":
				return 2;
			case "OR":
				return 1;
			default:
				return 0;
		}
	}


	@SuppressWarnings({ "removal" })
	public static void main(String[] args) {

		try {
			String strTableName = "Student";
			DBApp dbApp = new DBApp();

			Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.Double");
			dbApp.createTable(strTableName, "id", htblColNameType);
			dbApp.createIndex(strTableName, "gpa", "gpaIndex");

			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
			htblColNameValue.put("id", new Integer(2343432));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(453455));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(5674567));
			htblColNameValue.put("name", new String("Dalia Noor"));
			htblColNameValue.put("gpa", new Double(1.25));
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(23498));
			htblColNameValue.put("name", new String("John Noor"));
			htblColNameValue.put("gpa", new Double(1.5));
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(78452));
			htblColNameValue.put("name", new String("Zaky Noor"));
			htblColNameValue.put("gpa", new Double(0.88));
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			SQLTerm[] arrSQLTerms;
			arrSQLTerms = new SQLTerm[2];
			arrSQLTerms[0]._strTableName = "Student";
			arrSQLTerms[0]._strColumnName = "name";
			arrSQLTerms[0]._strOperator = "=";
			arrSQLTerms[0]._objValue = "John Noor";

			arrSQLTerms[1]._strTableName = "Student";
			arrSQLTerms[1]._strColumnName = "gpa";
			arrSQLTerms[1]._strOperator = "=";
			arrSQLTerms[1]._objValue = new Double(1.5);

			String[] strarrOperators = new String[1];
			strarrOperators[0] = "OR";
			// select * from Student where name = "John Noor" or gpa = 1.5;
			Iterator<Object> resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
		} catch (Exception exp) {
			exp.printStackTrace();
		}
	}
}
