package DataBaseEngine.DB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.text.DecimalFormat;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
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
		String strTableName = "Student";

		DBApp dbApp = new DBApp();

		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		dbApp.createTable(strTableName, "id", htblColNameType);
		dbApp.createIndex(strTableName, "gpa", "gpaIndex");
		dbApp.createIndex(strTableName, "name", "nameIndex");
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

	public static void printAllPages() {
		/// Table t = Deserialize.Table("Student");
		// System.out.println(t.getPages());
		Page p = null;
		Table t = Deserialize.Table("Student");
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
		// htblColNameValue.put("name", new String( "Omar4" ) );
		// htblColNameValue.put("gpa", new Double( 1.2 ) );
		htblColNameValue.put("id", new Integer(652529));
		// D.updateTable("Student", "350321", htblColNameValue);
		D.deleteFromTable("Student", htblColNameValue);
	}

	public static void update() throws DBAppException {
		DBApp D = new DBApp();
		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		// htblColNameValue.put("name", new String( "Hassan" ) );
		htblColNameValue.put("gpa", new Double(2.44));

		D.updateTable("Student", "3030158", htblColNameValue);
		// D.deleteFromTable( "Student", htblColNameValue );
	}

	public static void insert() throws DBAppException {
		DBApp D = new DBApp();
		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		htblColNameValue.put("name", new String("Brolosy"));
		htblColNameValue.put("gpa", new Double(1.0));
		htblColNameValue.put("id", new Integer(15));
		D.insertIntoTable("Student", htblColNameValue);
		// D.deleteFromTable( "Student", htblColNameValue );
	}

	@SuppressWarnings("removal")
	public static void select(DBApp d) throws DBAppException,IOException {

		Hashtable<String, Object> attributesInTuple = new Hashtable<String, Object>();
		/*
		String strTableName = "TestingTable";
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		d.createTable(strTableName, "id", htblColNameType);
		 */


		// INSERTIONSSSSS
		/* 
		attributesInTuple.put("id", new Integer(3));
		attributesInTuple.put("name", new String("John Noor"));
		attributesInTuple.put("gpa", new Double(1.5));
		// Tuple t10 = new Tuple("id",attributesInTuple.keys(),(Enumeration<Object>)
		// attributesInTuple.values());
		d.insertIntoTable("TestingTable", attributesInTuple);
		attributesInTuple.clear();
		attributesInTuple.put("id", new Integer(1));
		attributesInTuple.put("name", new String("Brolos"));
		attributesInTuple.put("gpa", new Double(1.2));
		// Tuple t10 = new Tuple("id",attributesInTuple.keys(),(Enumeration<Object>)
		// attributesInTuple.values());
		d.insertIntoTable("TestingTable", attributesInTuple);

		attributesInTuple.clear();
		attributesInTuple.put("id", new Integer(2));
		attributesInTuple.put("name", new String("Brolos"));
		attributesInTuple.put("gpa", new Double(1.7));
		// Tuple t10 = new Tuple("id",attributesInTuple.keys(),(Enumeration<Object>)
		// attributesInTuple.values());
		d.insertIntoTable("TestingTable", attributesInTuple);

		attributesInTuple.clear();
		attributesInTuple.put("id", new Integer(4));
		attributesInTuple.put("name", new String("Brolos"));
		attributesInTuple.put("gpa", new Double(1.9));
		// Tuple t10 = new Tuple("id",attributesInTuple.keys(),(Enumeration<Object>)
		// attributesInTuple.values());
		d.insertIntoTable("TestingTable", attributesInTuple);
		*/
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[1];
		arrSQLTerms[0] = new SQLTerm();
		arrSQLTerms[0]._strTableName = "TestingTable";
		arrSQLTerms[0]._strColumnName = "gpa";
		arrSQLTerms[0]._strOperator = ">=";
		arrSQLTerms[0]._objValue = new Double(1.5);

		String[] strarrOperators = new String[0];

		
		// d.createIndex("TestingTable", "gpa", "gpaIndex");
		// d.createIndex("TestingTable", "name", "nameIndex");
		
		// select * from Student where Student.name = "John Noor" or Student.gpa = 1.5;
		// will they all be from the same table? [ie all sql terms will have the same
		// table]
		// I think yes because no joins

		

		Iterator<Object> resultSet = d.selectFromTable(arrSQLTerms, strarrOperators);

		System.out.println("Result Set: ");

		while (resultSet.hasNext()) {
			System.out.println(resultSet.next().toString());
		}

	}

	@SuppressWarnings({})
	public static void main(String[] args) throws Exception {
		/*
		Test T = new Test();
		int max = T.getMaxCount();
		System.out.println(max); // 200
		*/
		DBApp d = new DBApp();
		select(d);
		// test();
		// delete();
		// update();
		// insert();
		// printAllPages();

	}
}
