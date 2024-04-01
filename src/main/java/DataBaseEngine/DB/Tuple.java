package DataBaseEngine.DB;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Vector;


public class Tuple implements Comparable<Tuple>,Serializable{
    private Hashtable<String, Object> attributesInTuple = new Hashtable<String,Object>();
    private String primaryKey;
 
    public String getStrPrimaryKey() {
		return primaryKey;
	}

	public void setStrPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public Tuple(String primaryKey,Enumeration<String> keys,Enumeration<Object> values) {
        this.primaryKey = primaryKey;
        while(keys.hasMoreElements())
        	this.attributesInTuple.put(keys.nextElement(), values.nextElement());
		
    }

    public Hashtable<String, Object> getAttributesInTuple() {
        return attributesInTuple;
    }

    public String toString(){
        String res = "";
        Enumeration<Object> en = getAttributesInTuple().elements();
 
        while (en.hasMoreElements()) {
            Object val = en.nextElement();
            res = val + res;
            if(en.hasMoreElements()){
                res = "," + res;
            }
        
        }
        return res;
    }
    /**
	 * Compares between two tuples based on their primary key.
	 * Note: Primary Key can be of different types, all of which should implement Comparable
	 * @param t: tuple to compare with
	 * @return positive int, zero, negative int if tuple specified is less than, equal to, 
	 * 			or greater than respectively 
	 */
	@Override
	public int compareTo(Tuple t) {

		int x = (int)this.getAttributesInTuple().get(this.getStrPrimaryKey());
		int y = (int) (t.getAttributesInTuple().get(t.getStrPrimaryKey()));
		return  x - y;
	}
	
	public int getPK() {
		int pk = (int) this.getAttributesInTuple().get(this.getStrPrimaryKey());
		return pk;
	}
	
	public int getIndex(Vector<Tuple> v) {
		int index = Collections.binarySearch(v,this);
		index = -1 * (index+1);
		System.out.println("index : "+index +" key : "+this.getPK());
		return index;
		
	}
	public Page addTuple(Page page) throws IOException {
		
	    int index = this.getIndex(page.getTuplesInPage());
	    page.getTuplesInPage().add(index,this);
	    return page;
	}

	
	
    
}
