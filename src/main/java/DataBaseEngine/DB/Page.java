package DataBaseEngine.DB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable{
    // public because it should be accessed by Table (i think) and static because
    // the variable belongs to the class itself rather than to any specific instance of the class
    public static int N;
    Vector<Tuple> tuplesInPage = new Vector<Tuple>();
    //Vector<Tuple> tuplesInPage = new Vector<Tuple>();
    

    public Page() {

        try{
            Properties properties = new Properties();
            properties.load(new FileInputStream("DBApp.config"));
            Page.N = Integer.parseInt(properties.getProperty("MaximumRowsCountinPage"));
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
    
    
}