package DataBaseEngine.DB;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import com.opencsv.CSVWriter;

public class Table implements Serializable{
    //Is it correct that only the pageFileName are persistent?

    private ArrayList<String> pageFileNames;
    private String strTableName;
    private String strClusteringKeyColumn;
    //tranisent since already in metadata?
    private transient Hashtable<String,String> htblColNameType;
    private int pagesCounter;


    public Table(String strTableName, String strClusteringKeyColumn,
            Hashtable<String, String> htblColNameType) {
        pagesCounter = 1;
        this.pageFileNames = new ArrayList<>();
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.htblColNameType = htblColNameType;

    }

    public int getPagesCounter() {
        return pagesCounter;
    }

    public void setPagesCounter(int pagesCounter) {
        this.pagesCounter = pagesCounter;
    }

    public ArrayList<String> getPageFileNames() {
        return pageFileNames;
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

    public void setHtblColNameType(Hashtable<String, String> htblColNameType) {
        this.htblColNameType = htblColNameType;
    }







    public void addTable(String tableName,String pK,String filePath) {
        try {
            CSVWriter writer = new CSVWriter(new FileWriter(filePath,true));
            Enumeration<String> types = this.htblColNameType.elements();
            Enumeration<String> keys = this.htblColNameType.keys();
            String []line =  new String[6];
            
            while(types.hasMoreElements()) {
                line = insertLine(line,tableName,keys.nextElement(),types.nextElement(),pK);
                writer.writeNext(line);
            }
            writer.flush();
            writer.close();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    
    }

    public static String[] insertLine(String []line,String tableName,String name, String type,String primaryKey) {
		
		line[0] = tableName;line[1] = name; line[2] = type;
		if(primaryKey.equals(name))
			line[3] = "True";
		else
			line[3] = "False";
		line[4] = null;line[5] = null;
	
        return line;
    }

    public Page createPage(){
            
        int counter = this.getPagesCounter();
		String pageName = this.getStrTableName()+counter;
		Page p = new Page(pageName);
        this.setPagesCounter(++counter);
        return p;

    }


    public void appendPage(String pageFileName){
        pageFileNames.add(pageFileName);
    }
    
    public void insertRow(){
        //logic to choose page
        //deserialise correct page Page.p
        //pinsertIntoPage()

        //if specific chosen page is full,
        //create new page
        //insert in it
        //serialise
        //append page file name 
    }

    public void updateRow(){
        
    }

    public void deleteRow(){

    }

    public void selectRow(){

    }

    
    
}
