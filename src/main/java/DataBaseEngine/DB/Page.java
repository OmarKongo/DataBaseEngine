package DataBaseEngine.DB;

import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable{
    //Vector<Tuple> tuplesInPage = new Vector<Tuple>();
    Vector<Object> tuplesInPage = new Vector<Object>();

    public Page(Vector<Object> tuplesInPage) {
        this.tuplesInPage = tuplesInPage;
    }


    public String toString(){
        String logic = "do logic here";

        return logic;
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