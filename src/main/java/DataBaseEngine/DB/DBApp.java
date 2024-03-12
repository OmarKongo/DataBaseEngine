package DataBaseEngine.DB;

/** * @author Wael Abouelsaadat */

import java.util.Iterator;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import com.opencsv.*;

public class DBApp {

	public DBApp() {

	}

	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void init() {

	}

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		throw new DBAppException("not implemented yet");
	}

	// following method creates a B+tree index
	public void createIndex(String strTableName,
			String strColName,
			String strIndexName) throws DBAppException {

		throw new DBAppException("not implemented yet");
	}

	// mehtod to validate attributes of hashtable are in csv file
	public static boolean validateAttributesInCSV(Hashtable<String, Object> htblColNameValue) {
		try {
			FileReader filereader = new FileReader("src/meta.csv");
			CSVReader csvReader = new CSVReader(filereader);
			String[] nextRecord;
			ArrayList<ArrayList<String>> twoDCells = new ArrayList<ArrayList<String>>();
			while ((nextRecord = csvReader.readNext()) != null) {
				for (String cell : nextRecord) {
					String[] cellPieces = cell.split("\t");
					ArrayList<String> cellArrayList = new ArrayList<String>(Arrays.asList(cellPieces));
					twoDCells.add(cellArrayList);
				}
			}
			ArrayList<String> csvAttributes = new ArrayList<String>();
			for (String key : htblColNameValue.keySet()) {
				csvAttributes.add(key);
			}
			for (String key : htblColNameValue.keySet()) {
				for (int i = 0; i < twoDCells.size(); i++) {
					if (twoDCells.get(i).get(1).equals(key)) {
						csvAttributes.remove(key);
					}
				}
			}
			csvReader.close();
			if (csvAttributes.isEmpty()) {
				// System.out.println("All attributes are valid");
				return true;
			} else {
				// System.out.println("Not all attributes are valid");
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	// method to validate the data types from a hashtable to corresponding
	// attributes in csv file
	public static boolean validateDataTypesInCSV(Hashtable<String, Object> htblColNameValue) {
		try {
			FileReader filereader = new FileReader("src/meta.csv");
			CSVReader csvReader = new CSVReader(filereader);
			String[] nextRecord;
			ArrayList<ArrayList<String>> twoDCells = new ArrayList<ArrayList<String>>();
			while ((nextRecord = csvReader.readNext()) != null) {
				for (String cell : nextRecord) {
					String[] cellPieces = cell.split("\t");
					ArrayList<String> cellArrayList = new ArrayList<String>(Arrays.asList(cellPieces));
					twoDCells.add(cellArrayList);
				}
			}
			ArrayList<String> csvTypes = new ArrayList<String>();
			for (String key : htblColNameValue.keySet()) {
				csvTypes.add(key);
			}

			for (String key : htblColNameValue.keySet()) {
				for (int i = 0; i < twoDCells.size(); i++) {
					if (twoDCells.get(i).get(1).equals(key)) {
						if (twoDCells.get(i).get(2).equals(htblColNameValue.get(key).getClass().getName())) {
							// System.out.println("Type: " + key + " is " + twoDCells.get(i).get(2));
							csvTypes.remove(key);
						}
					}
				}
			}
			csvReader.close();
			if (csvTypes.isEmpty()) {
				// System.out.println("All data types are valid");
				return true;
			} else {
				// System.out.println("Not all data types are valid");
				return false;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		if (validateAttributesInCSV(htblColNameValue)) {
			if (validateDataTypesInCSV(htblColNameValue)) {

				// here the serialization will be done

			} else {
				System.out.println("Not all data types are valid");
			}
		} else {
			System.out.println("Not all attributes are valid");
		}
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

		return null;
	}

	@SuppressWarnings({ "removal" })
	public static void main(String[] args) {

		try {
			String strTableName = "Student";
			DBApp dbApp = new DBApp();

			Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.double");
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