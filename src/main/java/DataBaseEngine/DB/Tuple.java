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


public class Tuple extends Page implements Comparable<Object>,Serializable{
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
    
	@Override
	public int compareTo(Object o) {
		Tuple T = (Tuple) o;
		Object x = this.getAttributesInTuple().get(this.getStrPrimaryKey());
		Object y =  (T.getAttributesInTuple().get(T.getStrPrimaryKey()));
		if(x instanceof Integer) {
		  int first = (int) x; int second = (int) y;
		return  first - second;
		}
		else {
			if(x instanceof Double) {
				Double first = (Double) x;Double second = (Double) y;
				return (int)Math.ceil(first - second);
			}
			else {
				String first = (String) x;String second = (String) y;
				return first.compareToIgnoreCase(second);
			}
			
		}
	}
	
	public Object getPK() {
		Object pk = this.getAttributesInTuple().get(this.getStrPrimaryKey());
		return pk;
	}
	
	public int getIndex(Vector<Tuple> v) {
		int index = Collections.binarySearch(v,this);
		index = -1 * (index+1);
		//System.out.println("index : "+index +" key : "+this.getPK());
		return index;
		
	}
	public Page addTuple(Page page) throws IOException {
		
	    int index = this.getIndex(page.getTuplesInPage());
	    page.getTuplesInPage().add(index,this);
	    return page;
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Tuple updateTuple(String pageName,Hashtable<String,String> indexes,Hashtable<String,Object> htblColNameValue) {
		   String indexName = null;Object newKey = null;bplustree btree = null;String indx =null;
		   Enumeration<String> keys = null;Object oldKey = null;   ArrayList<String> pageNames = null;
		   ArrayList<String> currPage = new ArrayList<String>();currPage.add(pageName);
			     keys = htblColNameValue.keys();
				while(keys.hasMoreElements()) {
					  indx = keys.nextElement();
					  indexName = indexes.get(indx);
					  oldKey = this.getAttributesInTuple().get(indx);
					  newKey = htblColNameValue.get(indx);
					  if(indexName!=null) {
					  btree = Deserialize.Index(indexName);
			          
			          btree.update((Comparable)oldKey,currPage,(Comparable)newKey);
			          Serialize.Index(btree, indexName);
			         
					  }
					  this.getAttributesInTuple().put(indx,newKey);
					  
				}	
		   
		   return this;
	   }
	public int getActualIndex(Vector<Tuple> v) {
		int index = Collections.binarySearch(v,this.getPK());
		return index;
		
	}
	
	
    
}
