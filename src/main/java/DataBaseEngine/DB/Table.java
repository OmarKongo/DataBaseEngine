package DataBaseEngine.DB;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.TreeMap;
import java.util.Vector;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;



public class Table implements Serializable{
    //Is it correct that only the pageFileName are persistent?

    private Hashtable<String,Pair> props;
	private Vector<Page> pages;
    private String strTableName;
    private String strClusteringKeyColumn;
    //tranisent since already in metadata?
    private transient Hashtable<String,String> htblColNameType;
    private int pagesCounter;











    public Table() {
    	
    }
    public Table(String strTableName, String strClusteringKeyColumn,
            Hashtable<String, String> htblColNameType) {
        pagesCounter = 1;
        pages = new Vector<Page>();
        props = new Hashtable<String,Pair>();
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.htblColNameType = htblColNameType;

    }
    public Hashtable<String, Pair> getProps() {
		return props;
	}
	public Vector<Page> getPages() {
		return pages;
	}

    public int getPagesCounter() {
        return pagesCounter;
    }

    public void setPagesCounter(int pagesCounter) {
        this.pagesCounter = pagesCounter;
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

    public  String getPkType(String filePath) throws Exception {
		String tableName = this.getStrTableName();
		String pk = this.getStrClusteringKeyColumn();
    	String type = null;
    	CSVReader csvReader = new CSVReaderBuilder(new FileReader(filePath)) 
                .withSkipLines(1) 
                .build();
		String[] nextRecord;boolean flag = false;
		
        
        // this loop iterate over the csv file untill the table is founded
        // if it's not founded then an exception will be thrown
		while ((nextRecord = csvReader.readNext()) != null) { 
            
            if(nextRecord[0].equals(tableName) & nextRecord[1].equals(pk)) {
            	pk = nextRecord[2];
            	break;
            }
            else
            	continue;
        }
		return pk;
		
	}
    public static Hashtable<String,String> checkData(String tableName,Hashtable<String,Object> data,String filePath) throws Exception {
		CSVReader csvReader = new CSVReaderBuilder(new FileReader(filePath)) 
                .withSkipLines(1) 
                .build();
		String[] nextRecord;boolean flag = false;
		Hashtable<String,String> indexes = new Hashtable<String,String>();
		Enumeration<Object> values = data.elements();
        Enumeration<String> keys = data.keys();
        Hashtable<String,String> collector = new Hashtable<String,String>();
        // this loop iterate over the csv file untill the table is founded
        // if it's not founded then an exception will be thrown
		while ((nextRecord = csvReader.readNext()) != null) { 
            
            if(nextRecord[0].equals(tableName)) {
            	flag = true;
            	break;
            }
            else
            	continue;
        }
		if(!flag)
			throw new DBAppException("Invalid Table");
		// this loop insert all the original attributes of the table inside collector hashtable
		while(nextRecord!=null && nextRecord[0].equals(tableName)) {
			collector.put(nextRecord[1],nextRecord[2]);
			
			if(nextRecord[4]!="")
				indexes.put(nextRecord[1],nextRecord[4]);
			nextRecord = csvReader.readNext();
			
		}
		// this loop checks the entered data and throws exception if anything mismatches the original attributes 
		while(keys.hasMoreElements()) {
			String key = keys.nextElement();
			if(collector.containsKey(key)){
				if(!(values.nextElement().getClass().equals(Class.forName(collector.get(key)))))
					throw new DBAppException("Mismatch type");
				
			}
			else
				throw new DBAppException("mismatch key  "+key);
			
    	}
		return indexes;
	}
   
    public String setNameForpage() {
    	int counter = this.getPagesCounter();
		String pageName = this.getStrTableName()+counter;
		this.setPagesCounter(++counter);
		return pageName;
    }
     public static Object baseType(Object key) {
    	 
    	 if((key instanceof String)) {
    		 return "";
    	 }
    	 else
    		 if(key instanceof Double)
    			 return 0.0;
    		 else
    			 return 0;
    	 
     }
     public static bplustree btreeType(String type){
    	 bplustree btree = null;
 		
         if(type.equals("java.lang.Integer")) 
      	  btree = new bplustree<Integer>(Integer.class,3);    
         
         else
      	   if(type.equals("java.lang.String")) 
      	   btree = new bplustree<String>(String.class,3);
      	   
      	   else 
      	      btree = new bplustree<Double>(Double.class,3);
         return btree;
     }
     @SuppressWarnings({ "rawtypes", "unchecked" })
	public static void addInBtree(Tuple T,Page p,Hashtable<String,String> indexes) {
    	 ArrayList<String> pages = new ArrayList<String>();                                        
    	 for(String key : indexes.keySet()) {
    		 bplustree btree = Deserialize.Index(indexes.get(key));
    		// System.out.println(indexes.get(key));
    		 pages.add(p.getName());
    		 btree.insert((Comparable) T.getAttributesInTuple().get(key),pages);
    		 Serialize.Index(btree,indexes.get(key));
    		 pages.clear();
    	 }
     }
     @SuppressWarnings({ "rawtypes", "unchecked" })
	public static void deleteFromIndex(Tuple T,Page p,Hashtable<String,String> indexes) {
    	 
    	 ArrayList<String> pages = new ArrayList<String>();                                        
    	 for(String key : indexes.keySet()) {
    		 bplustree btree = Deserialize.Index(indexes.get(key));
    		 pages.add(p.getName());
    		 btree.delete((Comparable)T.getAttributesInTuple().get(key), pages);
    		 Serialize.Index(btree,indexes.get(key));
    		 pages.clear();
    	 }
     }
     
   
     public static Object getOriginalKey(String strClusteringKeyValue,String type){
      	 Object key = null;
   		
           if(type.equals("java.lang.Integer")) 
        	  key = Integer.parseInt(strClusteringKeyValue);    
           
           else
        	   if(type.equals("java.lang.String")) 
        	   key = Double.parseDouble(strClusteringKeyValue);
        	   
        	   else 
        	      key = strClusteringKeyValue;
           return key;
       }
     
     public void updateTable(Object key,Hashtable<String,String> indexes,Hashtable<String,Object> htblColNameValue) throws Exception {
     	Page p = null;String pageName = null;String tableName = this.getStrTableName();
     	int index = this.binarySearch(key,false);
     	pageName = this.getPages().elementAt(index).getName();
     	
     	p = Deserialize.Page(pageName);
     	if(!p.tupleFounded(key))
     		throw new DBAppException("Record not found");
     	
     	 int tupleIndex;
     	 List<Tuple> tuples = new ArrayList<Tuple>(p.getTuplesInPage());
 		  //Iterator<Tuple> iter = v.iterator();
 		  for(Tuple t : tuples) {
 			  //t = iter.next();
 			  if(t.getPK().equals(key)) {
 				  try {
 					  tupleIndex = t.getActualIndex(p.getTuplesInPage());
 	        p.getTuplesInPage().set(tupleIndex,t.updateTuple(p.getName(),indexes, htblColNameValue));
 	        Serialize.Page(p, pageName);
 	        Serialize.Table(this, tableName);
 					  //if(indexes.size()!=0)
 					      //update indexes
 				} catch (Exception e) {
 					// TODO Auto-generated catch block
 					e.printStackTrace();
 				}
 				
 		  }}
     	
     }
   
    // this method return the index of the right page to insert in
     public int binarySearch(Object key,Boolean insert) throws Exception
     {
     	//Object key = T.getPK();
     	int mid = 0;
     	Vector<Page> v =this.getPages();
 	  int l = 0; int r = v.size()-1;
 	  if(r == 0)
 		return 0;
 	                                                                              
         while (l <= r) {
              mid = (l + r) / 2;
              
  
             Pair p = this.getProps().get(this.getPages().elementAt(mid).getName());
            // Page P = Deserialize.Page(v.elementAt(mid).getName());
            // Page P = this.getPages().elementAt(mid);
            // P.display();
              if(key instanceof Integer) {
             	//int min = this.getPages().get(mid).getPageProp().get()
             int min = (int) p.getMin();
             int max = (int) p.getMax();
             if (min == (int)key || max == (int)key) {
             	if(insert)
                    throw new Exception("Duplicate Key");
             	else {
             		if(max == (int)key || mid==0)
             		  return mid;
             		else 
             			 return mid-1;
             			
             	
             		}
             	}
  
           
             if((int)key>min & (int)key<max)
             	return mid;
             else
             	if(max>(int)key)
             		r = mid - 1;
             	else
             		l = mid + 1;
             	
             }
              else 
             	 if(key instanceof Double) {
             		 Double min = (Double) p.getMin();
                      Double max = (Double) p.getMax();
                      if (min == (Double)key || max == (Double)key) 
                          throw new Exception("Duplicate Key");
                       
           
                    
                      if((Double)key>min & (Double)key<max)
                      	return mid;
                      else
                      	if(max>(Double)key)
                      		r = mid - 1;
                      	else
                      		l = mid + 1;
             		 
             	 }
              
              else {
             	 String min = (String) p.getMin();
                  String max = (String) p.getMax();
                  if (min.equals(key) || max.equals(key)) 
                      throw new Exception("Duplicate Key");
                   
       
                
                  if(((String)key).compareTo(min)>0 & ((String)key).compareTo(max)<0)
                  	return mid;
                  else
                  	if(((String)key).compareTo(max)<0)
                  		r = mid - 1;
                  	else
                  		l = mid + 1;
             	 
             	 
              }
         }
  
      
         return mid;
     }
 	
	
	public static void main(String[]args) throws Exception {
		
		
		
	}

}
