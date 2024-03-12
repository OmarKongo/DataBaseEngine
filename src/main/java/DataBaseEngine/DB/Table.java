package DataBaseEngine.DB;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;

public class Table implements Serializable{
    //Is it correct that only the pageFileName are persistent?

    ArrayList<String> pageFileNames;
    transient String strTableName;
    transient String strClusteringKeyColumn;
    transient Hashtable<String,String> htblColNameType;



    public Table(String strTableName, String strClusteringKeyColumn,
            Hashtable<String, String> htblColNameType) {
        this.pageFileNames = new ArrayList<>();
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.htblColNameType = htblColNameType;
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
