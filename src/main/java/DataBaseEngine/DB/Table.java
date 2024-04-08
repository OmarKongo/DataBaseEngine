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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import javax.print.attribute.standard.PDLOverrideSupported;

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
			throw new Exception("Invalid Table");
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
					throw new Exception("Mismatch type");
				
			}
			else
				throw new Exception("mismatch key  "+key);
			
    	}
		return indexes;
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
     
    
    
     public  boolean hasPK( Hashtable<String,Object> h ) {
  		if(h.containsKey(this.getStrClusteringKeyColumn()))
  			return true;
  		return false;
  	}
  
  	public static boolean hasIndex(Hashtable<String,String> indexes,String strFromHshTblCol ) {
  		
  		if (indexes.containsKey(strFromHshTblCol))
  			return true;
  		return false ;
  	}
  	@SuppressWarnings({ "unchecked", "rawtypes" })
  	public Page searchPK(Object key, boolean hasIndex,String indexName) throws DBAppException {
  		Page p = null;
  		if (hasIndex) {
  			bplustree bTree= Deserialize.Index(indexName);
  			ArrayList<String> page = bTree.search((Comparable)key);
  			bTree.delete((Comparable)key, page);
  			p = Deserialize.Page(page.get(0));
  		}
  		else {
  			
  			int index = 0;
  			try {
  				//index = t.binarySearch(key);
  			} catch (Exception e) {
  				// TODO Auto-generated catch block
  				e.printStackTrace();
  			}
  			// p = Deserialize.Page(t.getPages().get(index).getName());
  		}
  		return p ;
  	}
  	
  	public  void deleteTupleUsingKey(Page p,Tuple t) throws Exception {
 	     String pageName = null;Page prevPage = null;Page nextPage = null;int pageIndex = this.binarySearch(t.getPK(),false);
 	  String tableName = this.getStrTableName();
 	   //  int index = Collections.binarySearch(p.getTuplesInPage(),key);
 	    //if(index<0)
 	  Object key = t.getPK();
 	    //	throw new DBAppException("Key not Found");
 	     p.getTuplesInPage().remove(t);
 	    if(p.getTuplesInPage().size()>0) {
 	    	p.updateMax();
 	    	this.getProps().put(p.getName(),p.getPageProp().get(p.getName()));
     		Serialize.Table(this, tableName);
     		Serialize.Page(p,p.getName());
 	    	if(!(pageIndex==this.getPages().size()-1)) {
 	    	    nextPage = this.getPages().get(pageIndex+1);
 	    	    pageName = nextPage.getName();
 	    	    nextPage = Deserialize.Page(pageName);
 	    	    nextPage.updateMin(p.getPageProp().get(p.getName()).getMax());
 	    	    this.getProps().put(nextPage.getName(),nextPage.getPageProp().get(nextPage.getName()));
 	     		Serialize.Table(this, tableName);
 	    	    Serialize.Page(nextPage,pageName);
 	    	}
 	    	
 	    }
 	    else {
 	    	if(pageIndex==0 & !(pageIndex==this.getPages().size()-1)) {
 	    		nextPage = this.getPages().get(pageIndex+1);
 	    		pageName = nextPage.getName();
 	    	    nextPage = Deserialize.Page(pageName);
 	    	    nextPage.updateMin(baseType(t.getPK()));
 	    	    this.getProps().put(pageName,nextPage.getPageProp().get(pageName));
 	     		Serialize.Table(this, tableName);
 	    	    Serialize.Page(nextPage,pageName);
 	    	}
 	    	else {
 	    		if(!(pageIndex==this.getPages().size()-1)) {
 	    			
 	    			
        prevPage = this.getPages().get(pageIndex-1);pageName = prevPage.getName();
                           prevPage = Deserialize.Page(pageName);
               
                           
        nextPage = this.getPages().get(pageIndex+1);pageName = nextPage.getName();
                           nextPage = Deserialize.Page(pageName);
                           
                           
                nextPage.updateMin(prevPage.getPageProp().get(prevPage.getName()).getMax());
                this.getProps().put(nextPage.getName(),nextPage.getPageProp().get(nextPage.getName()));
 	     		Serialize.Table(this, tableName);            
                Serialize.Page(nextPage,pageName);
 	    			
 	    		}
 	    		
 	    	}
 	    	this.updatepages(p,pageIndex);
 	    
 	    }
 	    System.out.println(key + "  deleted");	
 	
 }
  	 public void updatepages(Page p,int pageIndex) {
  
      	String pageName = p.getName();
  		this.getPages().remove(pageIndex);
  		this.getProps().remove(pageName);
  		
  		this.deleteFile(pageName);
  		String tableName = this.getStrTableName();
      	Serialize.Table(this,tableName);
      	//return this;
      }
  	public  void deleteFile(String pageName) {
 		String path = "Pages/"+pageName+".ser";
 		File file = new File(path);
 		if(file.delete())
 			System.out.println(pageName+" deleted");
 		else
 			System.out.println("file cannot be deleted");
 		
 		
 	}
  	public static boolean tupleMatched(Tuple t,Hashtable<String,Object> htblColNameValue) {
  		Enumeration<String> attributes = htblColNameValue.keys();
   	  while(attributes.hasMoreElements()) {
   		  
   		  if(!(t.getAttributesInTuple().contains(htblColNameValue.get(attributes.nextElement()))))
   		 	  return false;
   		 
   	  }
   	  return true;
  	}
  	public  void iterateOverChosenPages(Set<String> pageNames,Hashtable<String,Object> htblColNameValue,Hashtable<String,String> indexes) throws DBAppException {
  		  Page p = null;//Tuple t = null;//Vector<Tuple> v = new Vector<Tuple>();
  		  
  		  for(String name : pageNames) {
  			  p = Deserialize.Page(name);
  			  List<Tuple> tuples = new ArrayList<Tuple>(p.getTuplesInPage());
  			  //Iterator<Tuple> iter = v.iterator();
  			  for(Tuple t : tuples) {
  				  //t = iter.next();
  				  if(tupleMatched(t,htblColNameValue)) {
  					  try {
 						this.deleteTupleUsingKey(p, t);
 					} catch (Exception e) {
 						// TODO Auto-generated catch block
 						e.printStackTrace();
 					}
  					if(indexes.size()!=0)
  			      deleteFromIndex(t,p, indexes);
  			  }}
  			  
  		  }
  		
  	}
  	public static boolean hasAnyIndex(Hashtable <String,Object> htblColNameValue,Hashtable<String,String> indexes ) {
  		
  		for(String key : htblColNameValue.keySet()) {
  			if(indexes.containsKey(key))
  				return true;
  		}
  		return false;
  	}
  	
     @SuppressWarnings({  "rawtypes", "unchecked" })
 	public void deleteFromTable(Hashtable<String,Object> htblColNameValue,Hashtable<String,String>indexes) throws Exception {
     	Page p =null;String pageName = null;int index;Tuple t = null;boolean flag =false;
     	bplustree btree = null;String indexName = null;
     	 ArrayList<String> tuples = new ArrayList<String>();Object key = null;String indx = null;
     	 ArrayList<String> currIndex = null;Set<String> finalTuples = null;
     	//First Case
     	if(this.hasPK(htblColNameValue)) {
     		 key = htblColNameValue.get(this.getStrClusteringKeyColumn());
     	    if(!hasIndex(indexes,this.getStrClusteringKeyColumn())) {
     		index = this.binarySearch(htblColNameValue.get(this.getStrClusteringKeyColumn()),false);
     	  pageName = this.getPages().elementAt(index).getName();
     	    }
     	 else {
     		 flag = true;
     		 indexName = indexes.get(this.getStrClusteringKeyColumn());
     		 btree = Deserialize.Index(indexName);
     		 pageName = (String) btree.search((Comparable)key).get(0); 
     	 }
     	  p = Deserialize.Page(pageName);
     	  index = Collections.binarySearch(p.getTuplesInPage(),key);
     	  if(index<0)
     		  throw new DBAppException("key not found");
     	  t = p.getTuplesInPage().elementAt(index);
     	   
     	  
     	  if(tupleMatched(t,htblColNameValue)) {
     		  deleteFromIndex(t,p,indexes);
     		  this.deleteTupleUsingKey(p, t);  
     	  
     	  }
     	  else
     		  throw new DBAppException("Record not found");
     	}
     	//second case
     	else
     		if(indexes.size()!=0 & hasAnyIndex(htblColNameValue,indexes)) {
     			Enumeration<String> keys = htblColNameValue.keys();
     			while(keys.hasMoreElements()) {
     				  indx = keys.nextElement();
     				  indexName = indexes.get(indx);
     				  if(indexName!=null) {
     				  btree = Deserialize.Index(indexName);
     		          key = htblColNameValue.get(indx);
     		          if(tuples.isEmpty())
     		        	  tuples = btree.search((Comparable)key);
     		          else {
     		        	  currIndex = btree.search((Comparable)key);
     		        	  tuples.retainAll(currIndex);
     		          }
     				  }
     			}
     			
     			if(tuples.size()==0)
     				throw new DBAppException("Invalid Enteries");
     			 finalTuples = new HashSet<String>(tuples);
     			 this.iterateOverChosenPages(finalTuples, htblColNameValue,indexes);
     		}
     	 // Third case
     		else {
     			finalTuples = new HashSet<String>(this.getProps().keySet());
     			this.iterateOverChosenPages(finalTuples, htblColNameValue,indexes);
     		}
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
