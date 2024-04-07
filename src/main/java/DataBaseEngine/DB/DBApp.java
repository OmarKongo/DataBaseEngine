package DataBaseEngine.DB;

/** * @author Wael Abouelsaadat */

import java.util.Iterator;
import java.util.Properties;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

public class DBApp {
	final static String csvPath = "metadata.csv";

	public DBApp() {
		this.init();
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

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value

	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException, IOException {

		Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		t.addTable(strTableName, strClusteringKeyColumn, csvPath);
		Serialize.Table(t);

	}

	// following method creates a B+tree index
	public void createIndex(String strTableName,
			String strColName,
			String strIndexName) throws DBAppException {

		throw new DBAppException("not implemented yet");
	}

	public static void checkDataType(Hashtable<String, String> htblColNameType) throws DBAppException {
		for (String s : htblColNameType.values()) {
			if (!(s.equals("java.lang.Integer") || s.equals("java.lang.String") || s.equals("java.lang.Double"))) {
				throw new DBAppException("Column DataType invalid");
			}
		}

	}

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		;

		try {
			Table.checkData(strTableName, htblColNameValue, csvPath);
		} catch (Exception e) {

			e.printStackTrace();
		}
		Table t = Deserialize.Table(strTableName);
		Tuple record = new Tuple(t.getStrClusteringKeyColumn(), htblColNameValue.keys(), htblColNameValue.elements());
		try {
			t = t.insertIntoTable(record);
		} catch (Exception e) {
			e.printStackTrace();
		}
		Serialize.Table(t);

	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName,
			String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		throw new DBAppException("not implemented yet");
	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		throw new DBAppException("not implemented yet");
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
		ArrayList<Object> resList = new ArrayList<>();

		try {
			boolean withIndex = Table.checkIndex(tableName, csvPath);
			if (!(withIndex)) {
				resList = t.selectFromTableNoIndex(arrSQLTerms, strarrOperators);
			} else {
				resList = t.selectFromTableWithIndex(arrSQLTerms, strarrOperators);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		Iterator<Object> result = resList.iterator();
		Serialize.Table(t);
		return result;
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

	@SuppressWarnings({ "removal" })
	public static void main(String[] args) throws Exception {

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
			arrSQLTerms[0] = new SQLTerm();
			arrSQLTerms[0]._strTableName = "Student";
			arrSQLTerms[0]._strColumnName = "id";
			arrSQLTerms[0]._strOperator = "=";
			arrSQLTerms[0]._objValue = new Integer(1);


			String[] strarrOperators = new String[1];

			// select * from Student where Student.name = "John Noor" or Student.gpa = 1.5;
			// will they all be from the same table? [ie all sql terms will have the same
			// table]
			// I think yes because no joins
			Iterator<Object> resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
			System.out.println(resultSet.next());
		} catch (Exception exp) {
			exp.printStackTrace();
		}

	}
}
