package DataBaseEngine.DB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable{
    private static Map<String,Integer> tableToNumOfPages= new HashMap<>();
    public static int maxNumberOfRows;
    public String name;
    Vector<Tuple> tuplesInPage = new Vector<Tuple>();
    //public static int pageNumber = 1;

    

    public Page(String tableName) {
        //defining naming scheme of pages
        //Does this make any sacrifices in terms of time and space complexity?

        //put() --> If it is used with a key that already exists, 
        //then the put method will update the associated value. Otherwise, it will add a new (key, value) pair.
        tableToNumOfPages.putIfAbsent(tableName, 0);
        int count = tableToNumOfPages.get(tableName);
        tableToNumOfPages.put(tableName, count + 1);
        this.name=tableName+""+(++count);
        //Page.pageNumber+=1;

        try{
            Properties properties = new Properties();
            properties.load(new FileInputStream("DBApp.config"));
            Page.maxNumberOfRows = Integer.parseInt(properties.getProperty("MaximumRowsCountinPage"));
        }
        catch(FileNotFoundException e){
            System.out.print(e.getStackTrace());
        } 
        catch(IOException e){
            System.out.print(e.getStackTrace());
        } 
       
       
    }

    public String toString(){
        String res = "";
        for(int i = 0; i<tuplesInPage.size(); i++){
            res = res + tuplesInPage.elementAt(i).toString();
            if(i!=tuplesInPage.size()-1){
                res = res + ",";
            }
        }
        return res;
    }

    public void insertIntoPage(){

    }
    public void updateInPage(){
        
    }
    public void deleteFromPage(){
        
    }
    public void selectFromPage(){
        
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    /* 
    public static int getPageNumber() {
        return pageNumber;
    }

    public static void setPageNumber(int pageNumber) {
        Page.pageNumber = pageNumber;
    }
    */
    public Vector<Tuple> getTuplesInPage() {
        return tuplesInPage;
    }

    public void setTuplesInPage(Vector<Tuple> tuplesInPage) {
        this.tuplesInPage = tuplesInPage;
    }

    public static Map<String, Integer> getTableToNumOfPages() {
        return tableToNumOfPages;
    }

    public static int getMaxNumberOfRows() {
        return maxNumberOfRows;
    }
    
    
}