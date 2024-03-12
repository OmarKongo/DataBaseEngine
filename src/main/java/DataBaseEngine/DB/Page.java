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
    

    public Page(Vector<Tuple> tuplesInPage) {

        this.tuplesInPage = tuplesInPage;
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



    public static int initDBAppConfig() {
        Properties properties = new Properties();
        int n = 0;
        try{
            
            properties.load(new FileInputStream("DBApp.config"));
            n = Integer.parseInt(properties.getProperty("MaximumRowsCountinPage"));
        }
        catch(FileNotFoundException e){
            System.out.print(e.getStackTrace());
        } 
        catch(IOException e){
            System.out.print(e.getStackTrace());
        } 
        return n;
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