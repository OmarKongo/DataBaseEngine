package DataBaseEngine.DB;

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

    
	private Vector<Page> pages ;
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
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.htblColNameType = htblColNameType;

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

    public void addTable(String tableName,String pK,String filePath) {
        try {
            checkDataType(this.htblColNameType,filePath,tableName);
            CSVWriter writer = new CSVWriter(new FileWriter(filePath,true));
            Enumeration<String> types = this.htblColNameType.elements();
            Enumeration<String> keys = this.htblColNameType.keys();
            String []line =  new String[6];
            
            while(types.hasMoreElements()) {
                line = insertLine(line,tableName,keys.nextElement(),types.nextElement(),pK);
                writer.writeNext(line);
            }
            writer.flush();
            writer.close();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    
    }
    public String addIndex(String tableName,String column, String indexName,String filePath) {
                String type= "";
    	try {
    		
            
    		CSVReader reader2 = new CSVReaderBuilder(new FileReader(filePath))
    				.withSkipLines(0) 
                    .build();
    	    List<String[]> allElements = reader2.readAll();
    	    CSVWriter writer = new CSVWriter(new FileWriter(filePath));
    	    
            
            CSVReader csvReader = new CSVReaderBuilder(new FileReader(filePath)) 
                    .withSkipLines(1) 
                    .build();
    		//String[] nextRecord;
            boolean flag = false;
    		for(String[] nextRecord : allElements) {
    			if(nextRecord[0].equals(tableName) & nextRecord[1].equals(column) ) {
                	nextRecord[4] = indexName;nextRecord[5] = "B+ tree"; type = nextRecord[2];
                	writer.writeAll(allElements);
            	    writer.flush();
                     flag = true;
                	break;
                }
                
    		}
            
    		if(!flag)
    			throw new DBAppException("Invalid Table or Column");
           
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    	return type;
    
    }	
    	
    

    public static void checkDataType(Hashtable<String,String> htblColNameType,String filePath,String tableName) throws DBAppException, IOException{
		for(String s : htblColNameType.values()){
			if(!(s.equals("java.lang.Integer") || s.equals("java.lang.String") || s.equals("java.lang.Double"))){
				throw new DBAppException("Column DataType invalid");
			}
		}
		CSVReader csvReader = null;
		try {
			csvReader = new CSVReaderBuilder(new FileReader(filePath)) 
			        .withSkipLines(1) 
			        .build();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] nextRecord;boolean flag = false;
		
        // this loop iterate over the csv file untill the table is founded
        // if it's  founded then an exception will be thrown
		while ((nextRecord = csvReader.readNext()) != null) { 
            
            if(nextRecord[0].equals(tableName))
            	throw new DBAppException("Duplicate Table");
            	
            else
            	continue;
        

	}
    }
    public static String[] insertLine(String []line,String tableName,String name, String type,String primaryKey) {
		
		line[0] = tableName;line[1] = name; line[2] = type;
		if(primaryKey.equals(name))
			line[3] = "True";
		else
			line[3] = "False";
		line[4] = null;line[5] = null;
	
        return line;
    }
    public static boolean tableFounded(String tableName,String filePath) throws DBAppException, IOException {
    	CSVReader csvReader = null;
		try {
			csvReader = new CSVReaderBuilder(new FileReader(filePath)) 
			        .withSkipLines(1) 
			        .build();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] nextRecord;boolean flag = false;
		
        // this loop iterate over the csv file untill the table is founded
        // if it's  founded then an exception will be thrown
		while ((nextRecord = csvReader.readNext()) != null) { 
            
            if(nextRecord[0].equals(tableName))
            	return true;
            	
            else
            	continue;
        

	}
		return false;
    	
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
   
    public String setNameForpage() {
    	int counter = this.getPagesCounter();
		String pageName = this.getStrTableName()+counter;
		this.setPagesCounter(++counter);
		return pageName;
    }
     public static Object baseType(Tuple T) {
    	 Object key = T.getPK();
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
     
    public  Table insertIntoTable(Tuple T,Hashtable<String,String> indexes) throws Exception {
    	Page p = null;String pageName;boolean flag = false;
           Object maxKeyInPreviousPage;// this attribute connect the pages with each other maxLastpage = minNextPage
    	//Base Case
    
    	if(this.getPages().size()==0) {
    		 p = new Page(this.setNameForpage());
    		 p.getPageProp().put(p.getName(),new Pair(baseType(T)));
    		 this.getPages().add(p);
    		 T.addTuple(p);
    		 p = p.updateMax();
    		 addInBtree(T,p,indexes);
    		 Serialize.Page(p);
    		 
    	}
    	
    	else {  // Search for the right page to insert in
    		
    		int index = this.binarySearch(T.getPK());
    		 pageName = this.getPages().get(index).getName();
    		 p = Deserialize.Page(pageName);
    	
    		if(p.tupleFounded(T))
    	        	throw new DBAppException("Duplicate Tuple");
    		p = T.addTuple(p);
    		if(!p.overFlowed()) {
    			p = p.updateMax();
    			addInBtree(T,p,indexes);
    			Serialize.Page(p);
    	    	//ADDED SUCCESSFULLY
    	    }
    	    else {
    	    	//OVERFLOW ON PAGE BUT STILL ADDED
    	    	addInBtree(T,p,indexes);
    	    	T = p.getTuplesInPage().remove(p.getMaxCount());
    	    	deleteFromIndex(T,p,indexes);
    	    	p = p.updateMax();
          maxKeyInPreviousPage = p.getTuplesInPage().lastElement().getPK();
                   Serialize.Page(p);
                 //OVERFLOW ON THE LAST PAGE
                if(index == this.getPages().size()-1) { // if the overflow in the last page, I will create a new page without going through any loops
                	p = new Page(this.setNameForpage());p.getPageProp().put(p.getName(),new Pair(baseType(T))); 
                	this.getPages().add(p);p = p.updateMin(maxKeyInPreviousPage);	
                	T.addTuple(p);addInBtree(T,p,indexes);p = p.updateMax();Serialize.Page(p);
                }
                
                else { // handle the overflow of pages by setting flag = true 
                	 /// if flag is still false this means that the last page has an overflow 
                	while(index!=this.getPages().size()-1) {
                	pageName = this.getPages().get(index+1).getName();
                	//System.out.println(pageName);
                	p = Deserialize.Page(pageName);
                	T.addTuple(p);
                	if(!p.overFlowed()) {
                		addInBtree(T,p,indexes);
                		p = p.updateMin(maxKeyInPreviousPage);p = p.updateMax();
                		Serialize.Page(p);
                		flag = true;
                		break;
                	}
                	else {
                		addInBtree(T,p,indexes);
                		T = p.getTuplesInPage().remove(p.getMaxCount());
                		deleteFromIndex(T,p,indexes);
                		p = p.updateMax();p = p.updateMin(maxKeyInPreviousPage);
                        maxKeyInPreviousPage = p.getTuplesInPage().lastElement().getPK();
                        Serialize.Page(p);
                	}
                	index++;
                }
                	if(!flag) { // this if handles the last overflow can happen
                		p = new Page(this.setNameForpage());p.getPageProp().put(p.getName(),new Pair(baseType(T)));
                		this.getPages().add(p);p = p.updateMin(maxKeyInPreviousPage);	
                    	T.addTuple(p); addInBtree(T,p,indexes);p = p.updateMax();Serialize.Page(p);
                	}
                }
    	    	
    	    }
    	}
    	return this;
    	
    }
    
	public void updateRow(){
        
    }

    public void deleteRow(){

    }

    public void selectRow(){

    }
    // this method return the index of the right page to insert in
    public int binarySearch(Object key) throws Exception
    {
    	//Object key = T.getPK();
    	int mid = 0;
    	Vector<Page> v =this.getPages();
	  int l = 0; int r = v.size()-1;
	  if(r == 0)
		return 0;
	                                                                              
        while (l <= r) {
             mid = (l + r) / 2;
             
 
            
           // Page P = Deserialize.Page(v.get(mid).getName());
            Page P = this.getPages().get(mid);
             if(key instanceof Integer) {
            	//int min = this.getPages().get(mid).getPageProp().get()
            int min = (int) P.getPageProp().get(P.getName()).getMin();
            int max = (int) P.getPageProp().get(P.getName()).getMax();
            if (min == (int)key || max == (int)key) 
                throw new Exception("Duplicate Key");
             
 
          
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
            		 Double min = (Double) P.getPageProp().get(P.getName()).getMin();
                     Double max = (Double) P.getPageProp().get(P.getName()).getMax();
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
            	 String min = (String) P.getPageProp().get(P.getName()).getMin();
                 String max = (String) P.getPageProp().get(P.getName()).getMax();
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
