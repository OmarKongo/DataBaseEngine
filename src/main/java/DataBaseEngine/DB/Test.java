package DataBaseEngine.DB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Stack;
import java.util.Vector;

public class Test {
	// private
	// Properties prop;
	public Test() throws Exception {

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

	public static void test() throws DBAppException, IOException {
		String strTableName = "AYB";

		DBApp dbApp = new DBApp();

		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		dbApp.createTable(strTableName, "id", htblColNameType);
		dbApp.createIndex(strTableName, "gpa", "gpaIndexAYB");
		//dbApp.createIndex(strTableName, "name", "nameIndex");
		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		Double[] d = { 2.0, 1.3, 1.2, 4.0, 3.1 };

		for (int i = 0; i < 25; i++) {
			int key = (int) (Math.random() * 5021100);
			int dus = (int) (Math.random() * 5);
			Double x = Math.random() * 4.0;
			DecimalFormat df = new DecimalFormat("#.#");
			df.format(x);
			// DecimalFormat df = new DecimalFormat("#.#");
			// String key = getAlphaNumericString(5);
			// Double key = Double.parseDouble(df.format(x));
			/// int key = arr[i];
			// System.out.println("id : "+key);
			htblColNameValue.put("id", new Integer(key));
			htblColNameValue.put("name", new String("Omar" + (i % 5)));
			htblColNameValue.put("gpa", new Double(d[dus]));
			dbApp.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();

		}
	}

	public static void printAllPages() throws DBAppException {
		/// Table t = Deserialize.Table("Student");
		// System.out.println(t.getPages());
		Page p = null;
		Table t = Deserialize.Table("AYB");
		for (int i = 0; i < t.getPages().size(); i++) {
			String pn = t.getPages().elementAt(i).getName();
			System.out.println();
			System.out.println("                           " + pn);
			System.out.println();
			p = Deserialize.Page(pn);
			System.out.println("             " + p.toString());
			p.display();

			System.out.println();

		}

	}

	public static void delete() throws DBAppException {
		DBApp D = new DBApp();
		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		 //htblColNameValue.put("name", new String( "" ) );
		// htblColNameValue.put("gpa", new Double( 1.5 ) );
		htblColNameValue.put("id", new Integer(78452));
		// D.updateTable("Student", "350321", htblColNameValue);
		D.deleteFromTable("Student", htblColNameValue);
	}

	public static void update() throws DBAppException {
		DBApp D = new DBApp();
		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		 htblColNameValue.put("name", new String( "brolosy" ) );
		 htblColNameValue.put("gpa", new Double(0.7));

		D.updateTable("Student", "78452", htblColNameValue);
		// D.deleteFromTable( "Student", htblColNameValue );
	}

	@SuppressWarnings("removal")
	public static void insert(DBApp d, String strTableName) throws DBAppException {
		Hashtable<String, Object> attributesInTuple = new Hashtable<String, Object>();
		// INSERTIONSSSSS
		for (int i = 0; i < 5; i++) {
			attributesInTuple.clear();
			attributesInTuple.put("id", new Integer(1 + i * 4));
			attributesInTuple.put("name", new String("John Noor"));
			attributesInTuple.put("gpa", new Double(1.2));
			// Tuple t10 = new Tuple("id",attributesInTuple.keys(),(Enumeration<Object>)
			// attributesInTuple.values());
			d.insertIntoTable(strTableName, attributesInTuple);
			attributesInTuple.clear();
			attributesInTuple.put("id", new Integer(2 + i * 4));
			attributesInTuple.put("name", new String("Brolos"));
			attributesInTuple.put("gpa", new Double(1.5));
			// Tuple t10 = new Tuple("id",attributesInTuple.keys(),(Enumeration<Object>)
			// attributesInTuple.values());
			d.insertIntoTable(strTableName, attributesInTuple);

			attributesInTuple.clear();
			attributesInTuple.put("id", new Integer(3 + i * 4));
			attributesInTuple.put("name", new String("Shaarawy"));
			attributesInTuple.put("gpa", new Double(1.7));
			// Tuple t10 = new Tuple("id",attributesInTuple.keys(),(Enumeration<Object>)
			// attributesInTuple.values());
			d.insertIntoTable(strTableName, attributesInTuple);

			attributesInTuple.clear();
			attributesInTuple.put("id", new Integer(4 + i * 4));
			attributesInTuple.put("name", new String("Andalusy"));
			attributesInTuple.put("gpa", new Double(1.9));
			d.insertIntoTable(strTableName, attributesInTuple);
		}
		// D.deleteFromTable( "Student", htblColNameValue );
	}



	@SuppressWarnings("removal")
	public static void insert2(DBApp d, String strTableName) throws DBAppException {
		Hashtable<String,Object> htblColNameValue = new Hashtable<String,Object>( );
		Double[]d1 = {2.0,1.3,1.2,4.0,3.1};
			
			for(int i = 0;i<25;i++) {
				int key = (int)(Math.random()*5021100);
				int dus  = (int)(Math.random()*5);
				
			
				htblColNameValue.put("id", new Integer( key));
				htblColNameValue.put("name", new String("Omar"+(i%5)) );
				htblColNameValue.put("gpa", new Double( d1[dus] ) );
				d.insertIntoTable( strTableName , htblColNameValue );
				htblColNameValue.clear();
			
			}
	}

	public static void createTable(DBApp d, String tableName) throws Exception {
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		d.createTable(tableName, "id", htblColNameType);

	}

	public static void createIndex(DBApp d, String tableName) throws Exception {

		d.createIndex(tableName, "gpa", "gpaIndex");
		// d.createIndex(tableName, "name", "nameIndex");
	}

	@SuppressWarnings("removal")
	public static void select(DBApp d, String strTableName) throws DBAppException, IOException {

		// Tuple t10 = new Tuple("id",attributesInTuple.keys(),(Enumeration<Object>)
		// attributesInTuple.values());

		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[1];
		arrSQLTerms[0] = new SQLTerm();
		arrSQLTerms[0]._strTableName = strTableName;
		arrSQLTerms[0]._strColumnName = "name";
		arrSQLTerms[0]._strOperator = ">";
		arrSQLTerms[0]._objValue = new String("Omar0");
		
		




		String[] strarrOperators = new String[0];
		

		Iterator<Object> resultSet = d.selectFromTable(arrSQLTerms, strarrOperators);

		System.out.println("Result Set: ");

		while (resultSet.hasNext()) {
			System.out.println(resultSet.next().toString());
		}

		// 3AAAAAAAAAAAA
		// 3AAAAAAAAAAAA
		// Andalusyhiiiiii
		// 2 length of arrSQLTerms
		// [John Noor,1.5,23498, Zaky Noor,0.88,78452, Dalia
		// Noor,1.25,5674567]Hereeeeeeeeeeee
		// in switchcase
		// test count is: 5 tuples in page is: 5
		// [John Noor,1.5,23498, Zaky Noor,0.88,78452, Ahmed Noor,0.95,453455, Ahmed
		// Noor,0.95,2343432, Dalia Noor,1.25,5674567]Hereeeeeeeeeeee
		// 2size
		// [(, [John Noor,1.5,23498, Zaky Noor,0.88,78452, Dalia Noor,1.25,5674567],
		// AND, [John Noor,1.5,23498, Zaky Noor,0.88,78452, Ahmed Noor,0.95,453455,
		// Ahmed Noor,0.95,2343432, Dalia Noor,1.25,5674567], )] infix
		// [[John Noor,1.5,23498, Zaky Noor,0.88,78452, Dalia Noor,1.25,5674567], [John
		// Noor,1.5,23498, Zaky Noor,0.88,78452, Ahmed Noor,0.95,453455, Ahmed
		// Noor,0.95,2343432, Dalia Noor,1.25,5674567], AND] postfix
		// Result Set:

	}

	@SuppressWarnings("removal")
	public static void select2(DBApp d, String strTableName) throws DBAppException, IOException {

		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[4];
		arrSQLTerms[0] = new SQLTerm();
		arrSQLTerms[0]._strTableName = strTableName;
		arrSQLTerms[0]._strColumnName = "name";
		arrSQLTerms[0]._strOperator = "=";
		arrSQLTerms[0]._objValue = new String("Omar0");
		
		arrSQLTerms[1] = new SQLTerm();
		arrSQLTerms[1]._strTableName = strTableName;
		arrSQLTerms[1]._strColumnName = "name";
		arrSQLTerms[1]._strOperator = "=";
		arrSQLTerms[1]._objValue = new String("Omar0");
		
		arrSQLTerms[2] = new SQLTerm();
		arrSQLTerms[2]._strTableName = strTableName;
		arrSQLTerms[2]._strColumnName = "name";
		arrSQLTerms[2]._strOperator = "=";
		arrSQLTerms[2]._objValue = new String("Omar4");
		
		arrSQLTerms[3] = new SQLTerm();
		arrSQLTerms[3]._strTableName = strTableName;
		arrSQLTerms[3]._strColumnName = "gpa";
		arrSQLTerms[3]._strOperator = "=";
		arrSQLTerms[3]._objValue = new Double(1.2);
		

		String[] strarrOperators = new String[3];
		strarrOperators[0] = "AND";strarrOperators[1] = "OR";strarrOperators[2] = "XOR";

		Iterator<Object> resultSet = d.selectFromTable(arrSQLTerms, strarrOperators);
		int i = 0;
		System.out.println("Result Set: ");

		while (resultSet.hasNext()) {
			i++;
			System.out.println(resultSet.next().toString());
		}
		System.out.println(i);
	}

	@SuppressWarnings("removal")
	public static void arrayListOperations() throws DBAppException, IOException {

		ArrayList<Object> a = new ArrayList<>();
		ArrayList<Object> b = new ArrayList<>();
		ArrayList<Object> c = new ArrayList<>();
		ArrayList<Object> d = new ArrayList<>();
		a.add("Yousef");
		a.add("Ahmed");
		a.add("Elbrolosy");
		b.add("Omar");
		b.add("Magdy");
		b.add("Kongo");
		c.add("Julian");
		c.add("Casablancas");
		d.add("Fabrizio");
		d.add("Elbrolosy");
		d.add("Magdy");

		// a --> {Yousef, Ahmed, Elbrolosy}
		// b --> {Omar, Magdy, Kongo}
		// c --> {Julian, Casablancas}
		// d --> {Fabrizio, Elbrolosy, Magdy}
		// ArrayList<String> addAll = new ArrayList<>();
		// ArrayList<String> retainAll = new ArrayList<>();
		// ArrayList<String> xorAll = new ArrayList<>();
		// addAll.addAll(b);
		// retainAll.addAll(b);
		// xorAll.addAll(b);
		// // ORING b and d
		// if (!(addAll.containsAll(d))) {
		// addAll.removeAll(d);
		// addAll.addAll(d);
		// }
		// //ANDING b and d
		// retainAll.retainAll(d);

		// //XORING b and d
		// if (!(xorAll.containsAll(d))) {
		// xorAll.removeAll(d);
		// System.out.println("1: "+ xorAll);
		// xorAll.addAll(d);
		// System.out.println("2: "+ xorAll);
		// }
		// xorAll.removeAll(retainAll);

		// (B and NOT D) OR (NOT B and D)

		// System.out.println(addAll);
		// System.out.println(retainAll);
		// System.out.println("3: "+xorAll);

		// System.out.println("hi: "+DBApp.or(b,d));
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[2];
		arrSQLTerms[0] = new SQLTerm();
		arrSQLTerms[0]._strTableName = "Student";
		arrSQLTerms[0]._strColumnName = "name";
		arrSQLTerms[0]._strOperator = ">";
		arrSQLTerms[0]._objValue = new String("Andalusy");
		arrSQLTerms[1] = new SQLTerm();
		arrSQLTerms[1]._strTableName = "Student";
		arrSQLTerms[1]._strColumnName = "id";
		arrSQLTerms[1]._strOperator = "<";
		arrSQLTerms[1]._objValue = new Integer(6);

		String[] strarrOperators = new String[1];
		strarrOperators[0] = "AND";

		// System.out.println(DBApp.infixToPostfix(DBApp.convertToInfix(arrSQLTerms,
		// strarrOperators)));

	}

	public static void arrayListTest() throws DBAppException, IOException {
		ArrayList<Object> test = new ArrayList<>();
		test.add("A");
		test.add(" XOR ");
		test.add("B");
		test.add(" AND ");
		test.add(" C ");
		test.add(" OR ");
		test.add(" D ");
		System.out.println(test);
		test.add(2, "(");
		System.out.println(test);
	}

	@SuppressWarnings({})
	public static void main(String[] args) throws Exception {
		/*
		 * Test T = new Test();
		 * int max = T.getMaxCount();
		 * System.out.println(max); // 200
		 */

		String strTableName = "AYB";

		DBApp d = new DBApp();
		// createTable(d, strTableName);
		// createIndex(d, strTableName);
		// insert(d,strTableName);
		select(d, strTableName);
		 //test();
		//delete();
		 //update();
		// insert();
		 printAllPages();

		// arrayListOperations();
		// arrayListTest();

	}
}
