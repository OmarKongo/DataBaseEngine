package DataBaseEngine.DB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

public class Page  implements Serializable,Comparable<Object>{
  
	private String name;
	private Hashtable<String,Pair> pageProp;
    private Vector<Tuple> tuplesInPage = new Vector<Tuple>();
    
 public Page() {
    	
    }
    public Page(String pageName) {
        this.pageProp = new Hashtable<String,Pair>(); 
        
         this.name = pageName;
     }
    
	public Hashtable<String, Pair> getPageProp() {
		return pageProp;
	}

	
	public void display() {
	System.out.print("                      Page [pageProp=" + pageProp + "]");
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
		//return "Page [pageProp=" + pageProp + "]";
    }

   

    public int getMaxCount() throws IOException {
		String filename = "DBApp.config";
		Properties prop = null;
		try(FileInputStream fis = new FileInputStream(filename) ){
			prop = new Properties();
			prop.load(fis);
			
		}catch(FileNotFoundException ex) {
			ex.printStackTrace();
		}
	  String num = prop.getProperty("MaximumRowsCountinPage");
	  return Integer.parseInt(num);
		
	}
    @Override
	public int compareTo(Object o) {
		
	    Page P = (Page) o;
	    Object fMin = this.getPageProp().get(this.getName()).getMin();
	    Object sMin = P.getPageProp().get(P.getName()).getMin();
	    
	    if(fMin instanceof Integer) {
			  int first = (int) fMin; int second = (int) sMin;
			return  first - second;
			}
			else {
				if(fMin instanceof Double) {
					Double first = (Double) fMin;Double second = (Double) sMin;
					return (int)Math.ceil(first - second);
				}
				else {
					String first = (String) fMin;String second = (String) sMin;
					return first.compareToIgnoreCase(second);
				}
				
			}
	}
    public boolean tupleFounded(Tuple T) {
    	int index = Collections.binarySearch(this.getTuplesInPage(),T);
    	if(index <0)
    		return false;
    	
    	
    	return true;
    	
    }
    
   public void addNewPageToTheTable(Vector<Page> v) throws Exception {
	   int index = Collections.binarySearch(v,this);
		
		index = -1*(index+1);
		v.add(index,this);
	   
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

    public Page updateMax() {
    	this.getPageProp().get(this.getName())
		.setMax(this.getTuplesInPage().lastElement().getPK());
    	return this;
    }
    public Page updateMin(Object min) {
    	this.getPageProp().get(this.getName()).setMin(min);
    return this;
    }
    public boolean overFlowed() throws IOException {
    	return this.getTuplesInPage().size()>this.getMaxCount();
    	
    }
   
    public static void main(String[]args) {
    	Page p = new Page("s");
    	int r = (int)(Math.random()*5021100);
    	
			System.out.print(r);
		
    }
    
    
}

class Pair implements Serializable{

	   Object min,max;
	   public Pair(Object start) {
		   
		  // this.max = max;
		   this.min = start;
	   }
	public Object getMin() {
		return min;
	}
	public void setMin(Object min) {
		this.min = min;
	}
	@Override
	public String toString() {
		return "Pair [min=" + min + ", max=" + max + "]";
	}
	public Object getMax() {
		return max;
	}
	public void setMax(Object max) {
		this.max = max;
	}
		
	}