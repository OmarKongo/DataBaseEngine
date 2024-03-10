package DataBaseEngine.DB;

import java.util.ArrayList;
import java.util.Hashtable;

public class Table {
    ArrayList<String> pageFileNames;
    String strTableName;
    String strClusteringKeyColumn;
    Hashtable<String,String> htblColNameType;



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

    public void setPageFileNames(ArrayList<String> pageFileNames) {
        this.pageFileNames = pageFileNames;
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

    
}
