package DataBaseEngine.DB;


import java.io.FileInputStream;

import java.io.FileOutputStream;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class Serialize {
	
	//final static String filePath = "Tables.ser";
	
    public static String serializeTable(Table t) {
    	String filePath = t.getStrTableName()+".ser";
    	try {
    		
    		 //Saving of object in a file
            FileOutputStream file = new FileOutputStream(filePath);
            ObjectOutputStream out = new ObjectOutputStream(file);
            // Method for serialization of object
            out.writeObject(t);
             
            out.close();
            file.close();
             
            System.out.println("Object has been serialized");
    		
    	}
    	catch(Exception ex) {
    		ex.printStackTrace();
    	}
    	return filePath;
	}
    public static String serializePage(Page P) {
    	String filePath = P.getName()+".ser";
    	try {
    		
   		 //Saving of object in a file
           FileOutputStream file = new FileOutputStream(filePath);
           ObjectOutputStream out = new ObjectOutputStream(file);
            
           // Method for serialization of object
           out.writeObject(P);
            
           out.close();
           file.close();
            
           System.out.println("Object has been serialized");
   		
   	}
   	catch(Exception ex) {
   		ex.printStackTrace();
   	}
   	return filePath;
    	/// serialize page
    	
    }

    /**
     * @author Brolosy
     * @param pageFileName the .ser filename of a page
     * @return the page after deserialisation
     * @throws Exception either FileNotFoundException or IOException (i think)
     * Deserialises the specified page.
     */
    public static Page deserializePage(String pageFileName) throws Exception{
		// Deserialize a string and date from a file.
        //must make try catch statement when calling this
		FileInputStream in = new FileInputStream(pageFileName);
		ObjectInputStream s = new ObjectInputStream(in);

		Page p = (Page)s.readObject();
		s.close();

        return p;
    }

    public static Table deserializeTable(String tableFileName) throws Exception{
		// Deserialize a string and date from a file.
        //must make try catch statement when calling this
		FileInputStream in = new FileInputStream(tableFileName);
		ObjectInputStream s = new ObjectInputStream(in);

		Table t = (Table)s.readObject();
		s.close();

        return t;
    }
	
}
