package DataBaseEngine.DB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Stack;
import java.util.Vector;

public class Test {
	//private 
	//Properties prop;
	public Test()  throws Exception{
		
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
	public static void test() throws DBAppException, IOException {
		String strTableName = "Student";
		
			DBApp	dbApp = new DBApp( );
			
			Hashtable<String,String> htblColNameType = new Hashtable<String,String>( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.Double");
			//dbApp.createTable( strTableName, "id", htblColNameType );
			//dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
			//dbApp.createIndex( strTableName, "name", "nameIndex" );
			Hashtable<String,Object> htblColNameValue = new Hashtable<String,Object>( );
			Double[]d = {2.0,1.3,1.2,4.0,3.1};
			
			for(int i = 0;i<25;i++) {
				int key = (int)(Math.random()*5021100);
				int dus  = (int)(Math.random()*5);
				Double x = Math.random()*4.0;
			    DecimalFormat df = new DecimalFormat("#.#");
			    df.format(x);
			 //   DecimalFormat df = new DecimalFormat("#.#");
			  // String key =  getAlphaNumericString(5);
			    // Double key = Double.parseDouble(df.format(x));
				///int key = arr[i];
				//System.out.println("id : "+key);
				htblColNameValue.put("id", new Integer( key));
				htblColNameValue.put("name", new String("Omar"+(i%5)) );
				htblColNameValue.put("gpa", new Double( d[dus] ) );
				dbApp.insertIntoTable( strTableName , htblColNameValue );
				htblColNameValue.clear();
			
			}
	}
	public static void printAllPages() {
		///Table t = Deserialize.Table("Student");
	//	System.out.println(t.getPages());
		   Page p = null;
		   Table  t = Deserialize.Table("Student");
			for(int i =0 ;i<t.getPages().size();i++) {
				String pn = t.getPages().elementAt(i).getName();
				System.out.println();
				System.out.println("                           "+pn);                                             
				System.out.println();
			 p = Deserialize.Page(pn);
	        System.out.println( "             "+p.toString());
	        p.display();
	        
	        System.out.println();
	       
			}
			
		  
		  
			
	}
	public static void delete() throws DBAppException {
		DBApp  D = new DBApp(); 
		Hashtable<String,Object> htblColNameValue = new Hashtable<String,Object>( );
		//htblColNameValue.put("name", new String( "Omar4" ) );
		//htblColNameValue.put("gpa", new Double( 1.2 ) );
		//htblColNameValue.put("id", new Integer( 652529) );
		//D.updateTable("Student", "350321", htblColNameValue);
	    D.deleteFromTable( "Student", htblColNameValue );
	}
	public static void update() throws DBAppException {
		DBApp  D = new DBApp(); 
		Hashtable<String,Object> htblColNameValue = new Hashtable<String,Object>( );
		//htblColNameValue.put("name", new String( "Hassan" ) );
		htblColNameValue.put("gpa", new Double( 2.44 ) );
	
		D.updateTable("Student", "2519133", htblColNameValue);
	    //D.deleteFromTable( "Student", htblColNameValue );
	}
	public static void insert() throws DBAppException {
		DBApp  D = new DBApp(); 
		Hashtable<String,Object> htblColNameValue = new Hashtable<String,Object>( );
		htblColNameValue.put("name", new String( "Brolosy" ) );
		htblColNameValue.put("gpa", new Double( 1.0 ) );
		htblColNameValue.put("id", new Integer( 15) );
		D.insertIntoTable("Student", htblColNameValue);
	    //D.deleteFromTable( "Student", htblColNameValue );
	}
	public static Object[] infix(SQLTerm[]arr1,String[]arr2) {
		Object [] arr3 = new Object[arr1.length+arr2.length];
		   
		   int i = 0, j = 0, k = 0;
		   
	       // Traverse both array
		   while (i < arr1.length || j < arr2.length) {
	           if(i < arr1.length)
	    	       arr3[k++] = arr1[i++];
	           if(j < arr2.length)
	           arr3[k++] = arr2[j++];
	       }
	       
	return arr3;
	}
	public static void select() throws DBAppException {
		DBApp  D = new DBApp(); 
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[4];
		arrSQLTerms[0]=new SQLTerm();  
		arrSQLTerms[1]=new SQLTerm();
		arrSQLTerms[2]=new SQLTerm();
		arrSQLTerms[3]=new SQLTerm();
		
		arrSQLTerms[0]._strTableName =  "Student";
		arrSQLTerms[0]._strColumnName=  "name";
		arrSQLTerms[0]._strOperator  =  "=";
		arrSQLTerms[0]._objValue     =  "Omar1";

		arrSQLTerms[1]._strTableName =  "Student";
		arrSQLTerms[1]._strColumnName=  "gpa";
		arrSQLTerms[1]._strOperator  =  "=";
		arrSQLTerms[1]._objValue     =  new Double(1.2);
		
		arrSQLTerms[2]._strTableName =  "Student";
		arrSQLTerms[2]._strColumnName=  "name";
		arrSQLTerms[2]._strOperator  =  "=";
		arrSQLTerms[2]._objValue     =  "Omar0";
		
		arrSQLTerms[3]._strTableName =  "Student";
		arrSQLTerms[3]._strColumnName=  "gpa";
		arrSQLTerms[3]._strOperator  =  "=";
		arrSQLTerms[3]._objValue     =  new Double(1.3);
		
		
		
		
		

		String[]strarrOperators = new String[3];
		strarrOperators[0] = "AND";strarrOperators[1] = "OR";strarrOperators[2] = "AND";;
		// select * from Student where name = "John Noor" or gpa = 1.5;
		//infixToPostfix(infix(arrSQLTerms,strarrOperators));
			Iterator<Object> resultSet = D.selectFromTable(arrSQLTerms , strarrOperators);
			int i =0;
			while(resultSet.hasNext()) {
	        i++;
				System.out.println(resultSet.next());
	        	System.out.println();
	}
			System.out.println(i);
			}
	
	 static boolean isOperator(Object c)
	    {
	        return c.equals("AND") || c.equals("OR") || c.equals("XOR");
 	    }
	 static boolean isSQLTerm(Object sql)
	    {
	        return sql instanceof SQLTerm;
	    }
	 
	    // Function to get priority of operators
	    static int getPriority(Object C)
	    {
	        if (C.equals("XOR"))
	            return 1;
	        else if (C.equals("OR"))
	            return 2;
	        else if (C.equals("AND"))
	            return 3;
	 
	        return 0;
	    }
	 // Reverse the letters of the word
	    static Object[] reverse(Object str[], int start, int end)
	    {
	        // Temporary variable to store character
	        Object temp;
	        while (start < end) {
	            // Swapping the first and last character
	            temp = str[start];
	            str[start] = str[end];
	            str[end] = temp;
	            start++;
	            end--;
	        }
	        return str;
	    }
	    
	    // Function to convert infix to postfix expression
	    static Object[] infixToPostfix(Object[] infix1)
	    {
	       // String infix = '(' + String.valueOf(infix1) + ')';
	      System.out.println();
	        int l = infix1.length;
	        Stack<Object> char_stack = new Stack<>();
	        Object [] output = new Object[l];
	           int i=0;int x =0;
	        for (i = 0; i < l; i++) {
	 
	            // If the scanned character is an
	            // operand, add it to output.
	            if (isSQLTerm(infix1[i]))
	                output[x++] = infix1[i];
	 
	 
	            // Operator found
	            else {
	            	if(char_stack.isEmpty())
	            		char_stack.add(infix1[i]);
	            	else {
	            		
	                
	                    while (
	                        (getPriority(infix1[i])
	                         < getPriority(char_stack.peek())))               
	                             {
	                        output[x++]= char_stack.peek();
	                        char_stack.pop();
	                        if(char_stack.isEmpty())
	                        	break;
	                    }
	 
	                    // Push current Operator on stack
	                    char_stack.add(infix1[i]);
	                }
	            }
	        }
	        while (!char_stack.empty()) {
	        //	System.out.println(char_stack.peek());
	            output[x++]= char_stack.pop();
	        }
	        for (int lo = 0; lo < output.length; lo++)
		           System.out.print(output[lo] + " ");
	        
	        return output;
	    }
	 
	
	@SuppressWarnings({"unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception {
		
		Test T = new Test();
		int max = T.getMaxCount();
	//	System.out.println(max); //200
		//test();
		//delete();
		update();
		//insert();
		//select();
		printAllPages();
		
	     //System.out.println(Math.);
		// select * from Student where name = "ahmed noor" or gpa > 2.0 and age != 20;
		// check sql terms 
		// get hashtable <name>,<ahmed noor> //      
		 //              <gpa>,<2.0>         //
		  //             <age>,<20>          //
		 // indexes ={}
		// oper = {=,>,!=}       operators = {or,and}
		 /// 2 to 1 
		// methods : equals(String attr ,Object value) check if it has index or not 
	       // notEqual : (String attr , Object value) check if it has index or not 
	      // more than or equal : (String attr, Object value,boolean OrEqual) 
	     //  less tha or equal : (String attr , Object value, boolean OrEqual)
	      /// how to satisfy the select 
	                       // if OR  resList.AddAll(tuples<>) A : resList B : tuples 
	                      // if AND  resList.retainAll(tuples<>) A : resList B : tuples
	                     // A  & A' & B & B'
	                     // if XOR   store it in <> first then resList.retainAll(not<>).
	                        
		
		
	}
	
     
   










}
