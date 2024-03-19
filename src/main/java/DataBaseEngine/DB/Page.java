package DataBaseEngine.DB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable{

    private static int maxNumberOfRows;
    private String name;
    Vector<Tuple> tuplesInPage = new Vector<Tuple>();
    

    public Page(String pageName) {
        this.name = pageName;
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

    public Vector<Tuple> getTuplesInPage() {
        return tuplesInPage;
    }

    public void setTuplesInPage(Vector<Tuple> tuplesInPage) {
        this.tuplesInPage = tuplesInPage;
    }

    public static int getMaxNumberOfRows() {
        return maxNumberOfRows;
    }
}
