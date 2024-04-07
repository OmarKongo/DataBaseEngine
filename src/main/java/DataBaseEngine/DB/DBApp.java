package DataBaseEngine.DB;
/** * @author Wael Abouelsaadat */ 


import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

import DataBaseEngine.DB.bplustree.Node;

public class DBApp {
	final static String csvPath = "metadata.csv";
	final static int NodeSize = 5;

	public DBApp( ){
		this.init();
	}

	// this does whatever initialization you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 

	
	public void init( ){
		CSVWriter writer =null;
		try {
			
			writer = new CSVWriter(new FileWriter(csvPath,true));
			String [] header = {"Table Name","Column Name","Column Type"
						,"Clustering Key","IndexName","IndexType"};
			
			writer.writeNext(header);
			 writer.flush();
	            writer.close();
		}
		catch(Exception ex) {
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
							Hashtable<String,String> htblColNameType) throws DBAppException, IOException{

								
                 
								Table T = new Table(strTableName,strClusteringKeyColumn,htblColNameType);
								T.addTable(strTableName,strClusteringKeyColumn,csvPath);
								Serialize.Table(T);			
							
	}

	// following method creates a B+tree index 
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException, ClassNotFoundException{
		
		//writes to index IF conditions are valid -- conditions state inside function				
		if(!CSV.writeIndex(strTableName, strColName, strIndexName))
			throw new DBAppException("Index Couldn't be created");
		
		//Insert all tuples previosly created inside of table
		String type = CSV.getcell(strTableName, strIndexName, CSV.COLUMN_TYPE_INDEX);
		bplustree tree = new bplustree(Class.forName(type), NodeSize);
		ArrayList<String> pageNames = new ArrayList<>();
		Table table = Deserialize.Table(strTableName);
		Page[] pages = (Page[])table.getPages().toArray();
		for(int i = 0; i < pages.length; i++) {
			Tuple[] tuples = (Tuple[]) pages[i].getTuplesInPage().toArray();
			for(int j = 0; j < tuples.length; i++) {
				Hashtable<String, Object> tupleAtt = tuples[j].getAttributesInTuple();
				Object value = tupleAtt.get(strColName);
				pageNames.add(pages[i].getName());
				tree.insert((Comparable) value, pageNames);
				pageNames.clear();
			}
		}

		Serialize.bplustree(tree, strIndexName);

	}


	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, 
								Hashtable<String,Object>  htblColNameValue) throws DBAppException{
;
		
		try {
			Table.checkData(strTableName, htblColNameValue, csvPath);
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		Table T = Deserialize.Table(strTableName);
		Tuple record = new Tuple(T.getStrClusteringKeyColumn(),htblColNameValue.keys(),htblColNameValue.elements());	
		try {
			T = T.insertIntoTable(record);
		} catch (Exception e) {
			e.printStackTrace();
		}
		Serialize.Table(T);
	
	}


	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException{
	
		throw new DBAppException("not implemented yet");
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, 
								Hashtable<String,Object> htblColNameValue) throws DBAppException{
	
		throw new DBAppException("not implemented yet");
	}


	public Iterator<Object> selectFromTable(SQLTerm[] arrSQLTerms, 
									String[]  strarrOperators) throws DBAppException{
										
		return null;
	}


	@SuppressWarnings({ "removal" })
	public static void main( String[] args ){
	
		try{
			String strTableName = "Student";
			DBApp	dbApp = new DBApp( );
			
			Hashtable<String,String> htblColNameType = new Hashtable<String,String>( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.double");
			dbApp.createTable( strTableName, "id", htblColNameType );
			dbApp.createIndex( strTableName, "gpa", "gpaIndex" );

			Hashtable<String,Object> htblColNameValue = new Hashtable<String,Object>( );
			htblColNameValue.put("id", new Integer( 2343432 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 453455 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.95 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 5674567 ));
			htblColNameValue.put("name", new String("Dalia Noor" ) );
			htblColNameValue.put("gpa", new Double( 1.25 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 23498 ));
			htblColNameValue.put("name", new String("John Noor" ) );
			htblColNameValue.put("gpa", new Double( 1.5 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", new Integer( 78452 ));
			htblColNameValue.put("name", new String("Zaky Noor" ) );
			htblColNameValue.put("gpa", new Double( 0.88 ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );


			SQLTerm[] arrSQLTerms;
			arrSQLTerms = new SQLTerm[2];
			arrSQLTerms[0]._strTableName =  "Student";
			arrSQLTerms[0]._strColumnName=  "name";
			arrSQLTerms[0]._strOperator  =  "=";
			arrSQLTerms[0]._objValue     =  "John Noor";

			arrSQLTerms[1]._strTableName =  "Student";
			arrSQLTerms[1]._strColumnName=  "gpa";
			arrSQLTerms[1]._strOperator  =  "=";
			arrSQLTerms[1]._objValue     =  new Double( 1.5 );

			String[]strarrOperators = new String[1];
			strarrOperators[0] = "OR";
			// select * from Student where name = "John Noor" or gpa = 1.5;
			Iterator<Object> resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
		}
		catch(Exception exp){
			exp.printStackTrace( );
		}}
	}
		
	