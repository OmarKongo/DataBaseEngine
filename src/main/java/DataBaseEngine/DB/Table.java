package DataBaseEngine.DB;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.TreeMap;
import java.util.Vector;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;



public class Table implements Serializable{
    //Is it correct that only the pageFileName are persistent?

    private Hashtable<String,Pair> props;
	private Vector<Page> pages;
    private String strTableName;
    private String strClusteringKeyColumn;
    //tranisent since already in metadata?
    private transient Hashtable<String,String> htblColNameType;
    private int pagesCounter;











    public Table() {
    	
    }
    public Table(String strTableName, String strClusteringKeyColumn,
            Hashtable<String, String> htblColNameType) {
        pagesCounter = 1;
        pages = new Vector<Page>();
        props = new Hashtable<String,Pair>();
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.htblColNameType = htblColNameType;

    }
    public Hashtable<String, Pair> getProps() {
		return props;
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

   
    public String addIndex(String tableName,String column, String indexName,String filePath) {
        String type= "";
try {
	
    
	CSVReader reader2 = new CSVReaderBuilder(new FileReader(filePath))
			.withSkipLines(0) 
            .build();
    List<String[]> allElements = reader2.readAll();
    CSVWriter writer = new CSVWriter(new FileWriter(filePath));
    
    
    CSVReader csvReader = new CSVReaderBuilder(new FileReader(filePath)) 
            .withSkipLines(1) 
            .build();
	//String[] nextRecord;
    boolean flag = false;
	for(String[] nextRecord : allElements) {
		if(nextRecord[0].equals(tableName) & nextRecord[1].equals(column) ) {
        	nextRecord[4] = indexName;nextRecord[5] = "B+ tree"; type = nextRecord[2];
        	writer.writeAll(allElements);
    	    writer.flush();
             flag = true;
        	break;
        }
        
	}
    
	if(!flag)
		throw new DBAppException("Invalid Table or Column");
   
}
catch(Exception ex) {
    ex.printStackTrace();
}
return type;

}	
    
    
    @SuppressWarnings("rawtypes")
	public static bplustree btreeType(String type){
   	 bplustree btree = null;
		
        if(type.equals("java.lang.Integer")) 
     	  btree = new bplustree<Integer>(Integer.class,3);    
        
        else
     	   if(type.equals("java.lang.String")) 
     	   btree = new bplustree<String>(String.class,3);
     	   
     	   else 
     	      btree = new bplustree<Double>(Double.class,3);
        return btree;
    }

    
	public static void main(String[]args) throws Exception {
		
		
		
	}

}
