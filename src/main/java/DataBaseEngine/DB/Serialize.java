package DataBaseEngine.DB;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Serialize {
	static String pagesPath = "Pages";
	static String tablesPath = "Tables";
	static String indexPath = "Indices";

	public static void  Page(Page p) {
		//System.out.println(p.getName());
		String path = pagesPath+"/"+p.getName()+".ser";
		
		try
	        {   
	            //Saving of object in a file
			 
	            FileOutputStream file = new FileOutputStream(path);
	            ObjectOutputStream out = new ObjectOutputStream(file);
	             
	            // Method for serialization of object
	            out.writeObject(p);
	             
	            out.close();
	            file.close();
	             
	          //  System.out.println("Object has been serialized ff");
	 
	        }
	         
	        catch(IOException ex)
	        {
	            System.out.println("IOException is caught qq");
	        }
		//return path;
	}
	/**
     * @author Brolosy
     * @param p the table to be serialised
     * serialises table
     */
	public static void Table(Table t) {
		  String path = tablesPath+"/"+t.getStrTableName()+".ser"; 
		  //System.out.print(path);
		try
	        {   
	            //Saving of object in a file
			
	            FileOutputStream file = new FileOutputStream(path);
	            ObjectOutputStream out = new ObjectOutputStream(file);
	             
	            // Method for serialization of object
	            out.writeObject(t);
	             
	            out.close();
	            file.close();
	             
	           // System.out.println("Object has been serialized");
	 
	        }
	         
	        catch(IOException ex)
	        {
	            System.out.println("IOException is caught yy");
	        }
		 //return path;
	}

	public static <K extends Comparable<K> & Serializable> boolean bplustree(bplustree<K> tree, String indexName) {

		String path = indexPath + File.separator + indexName + ".ser";

		try(FileOutputStream outFile = new FileOutputStream(path);
		ObjectOutputStream outObject = new ObjectOutputStream(outFile)){

			outObject.writeObject(tree);
			return true;

		} catch(IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	
}
class Deserialize{
	static String pagesPath = "Pages";
	static String tablesPath = "Tables";
	static String indexPath = "Indices";
	
	/**
     * @author Brolosy
     * @param tableFileName the .ser filename of a page
     * @return the table after deserialisation
     * @throws Exception either FileNotFoundException or IOException (i think)
     * Deserialises the specified table.
     */
	public static Table Table(String tableName) {
		Table t = null;
		String path = tablesPath+"/"+tableName+".ser";
		try
        {   
            // Reading the object from a file
            FileInputStream file = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(file);
             
            // Method for deserialization of object
             t = (Table)in.readObject();
             
            in.close();
            file.close();
             
            //System.out.println("Object has been deserialized ");
       
        }
         
        catch(IOException ex)
        {
            System.out.println("IOException is caught zz");
        }
         
        catch(ClassNotFoundException ex)
        {
            System.out.println("ClassNotFoundException is caught");
        }
		return t;
	}
	
	public static Page Page(String pageName) {
		Page p = null;
		String path = pagesPath+"/"+pageName+".ser";
		try
        {   
            // Reading the object from a file
            FileInputStream file = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(file);
             
            // Method for deserialization of object
             p = (Page)in.readObject();
             
            in.close();
            file.close();
             
           // System.out.println("Object has been deserialized ");
       
        }
         
        catch(IOException ex)
        {
            System.out.println("IOException is caught xx");
        }
         
        catch(ClassNotFoundException ex)
        {
            System.out.println("ClassNotFoundException is caught");
        }
		return p;
	}
	
	public static bplustree<?> bplustree(String indexName) {
		
		String path = indexPath + File.separator + indexName + ".ser";

		try(FileInputStream inFile = new FileInputStream(path);
		ObjectInputStream inObject = new ObjectInputStream(inFile)){

			return (bplustree<?>) inObject.readObject();

		} catch(IOException e) {
			e.printStackTrace();
			return null;
		} catch (ClassNotFoundException e) { 
			e.printStackTrace();
			return null;
		}
		}
	
}
