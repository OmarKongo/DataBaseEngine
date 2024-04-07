package DataBaseEngine.DB;
/** * @author Wael Abouelsaadat */ 


import java.util.Iterator;
import java.util.Properties;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

public class DBApp {
	final static String csvPath = "metadata.csv";

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
				Serialize.Table(T,strTableName);			
			
}

// following method creates a B+tree index 
@SuppressWarnings({ "rawtypes", "unchecked" })
public void createIndex(String   strTableName,
			String   strColName,
			String   strIndexName) throws DBAppException, IOException{

Table t = Deserialize.Table(strTableName);
String type =  t.addIndex(strTableName, strColName, strIndexName, csvPath);
bplustree btree = Table.btreeType(type);
ArrayList<String> pageName = new ArrayList<String>();
Serialize.Index(btree, strIndexName);
for(int i = 0;i<t.getPages().size();i++) {

Page p = Deserialize.Page(t.getPages().get(i).getName());
pageName.add(p.getName());
for(int j = 0 ;j<p.getTuplesInPage().size();j++) {
btree = Deserialize.Index(strIndexName);
Tuple T = p.getTuplesInPage().get(j);
btree.insert((Comparable) T.getAttributesInTuple().get(strColName) ,pageName);
Serialize.Index(btree, strIndexName);
}

pageName.clear();
}


}


// following method inserts one row only. 
// htblColNameValue must include a value for the primary key
public void insertIntoTable(String strTableName, 
				Hashtable<String,Object>  htblColNameValue) throws DBAppException{

Hashtable<String,String> indexes = null;
try {
indexes = Table.checkData(strTableName, htblColNameValue, csvPath);
} catch (Exception e) {

e.printStackTrace();
}
Table T = Deserialize.Table(strTableName);
Tuple record = new Tuple(T.getStrClusteringKeyColumn(),htblColNameValue.keys(),htblColNameValue.elements());	
try {
T = Table.insertIntoTable(record,indexes,strTableName);
} catch (Exception e) {
// TODO Auto-generated catch block
e.printStackTrace();
}
Serialize.Table(T,strTableName);

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
				
Hashtable<String,String> indexes = null;
try {
indexes = Table.checkData(strTableName, htblColNameValue, csvPath);
} catch (Exception e) {

e.printStackTrace();
}
Table T = Deserialize.Table(strTableName);Page p = null;
if (htblColNameValue.size()==1 & isPK(T,htblColNameValue)) {
//Tuple.deleteTuple(indexes);
String indexName = indexes.get(T.getStrClusteringKeyColumn());
Object key = htblColNameValue.get(T.getStrClusteringKeyColumn());
p = searchPK(T,key,hasIndex(indexes,T.getStrClusteringKeyColumn()),indexName);
p = T.deleteTupleUsingPk(p, key);
if(p.getTuplesInPage().size()==0)
 T = T.updatepages(p);

}



}

public static boolean isPK(Table t, Hashtable<String,Object> h ) {
if(h.containsKey(t.getStrClusteringKeyColumn()))
return true;
return false;
}

public static boolean hasIndex(Hashtable<String,String> indexes,String strFromHshTblCol ) {
if (indexes.containsKey(strFromHshTblCol))
return true;
return false ;
}
@SuppressWarnings({ "unchecked", "rawtypes" })
public static Page searchPK(Table t,Object key, boolean hasIndex,String indexName) throws DBAppException {
Page p = null;
if (hasIndex) {
bplustree bTree= Deserialize.Index(indexName);
ArrayList<String> page = bTree.search((Comparable)key);
bTree.delete((Comparable)key, page);
p = Deserialize.Page(page.get(0));
}
else {

int index = 0;
try {
//index = t.binarySearch(key);
} catch (Exception e) {
// TODO Auto-generated catch block
e.printStackTrace();
}
// p = Deserialize.Page(t.getPages().get(index).getName());
}
return p ;
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
		
	