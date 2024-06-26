package DataBaseEngine.DB;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Serialize {
	static String pagesPath = "Pages";
	static String tablesPath = "Tables";
	static String indexesPath = "Indexes";

	public static void Index(bplustree btree, String indexName) {
		// System.out.println(p.getName());
		String path = indexesPath + "/" + indexName + ".ser";

		try {
			// Saving of object in a file

			FileOutputStream file = new FileOutputStream(path);
			ObjectOutputStream out = new ObjectOutputStream(file);

			// Method for serialization of object
			out.writeObject(btree);

			out.close();
			file.close();

			// System.out.println("Object has been serialized ff");

		}

		catch (IOException ex) {
			System.out.println("IOException is caught qq");
		}
		// return path;
	}

	public static void Page(Page p, String pageName) {
		// System.out.println(p.getName());
		String path = pagesPath + "/" + pageName + ".ser";

		try {
			// Saving of object in a file

			FileOutputStream file = new FileOutputStream(path);
			ObjectOutputStream out = new ObjectOutputStream(file);

			// Method for serialization of object
			out.writeObject(p);

			out.close();
			file.close();

			// System.out.println("Object has been serialized ff");

		}

		catch (IOException ex) {
			System.out.println("IOException is caught qq");
		}
		// return path;
	}

	/**
	 * @author Brolosy
	 * @param p the table to be serialised
	 *          serialises table
	 */
	public static void Table(Table t, String tableName) {
		String path = tablesPath + "/" + tableName + ".ser";
		// System.out.print(path);
		try {
			// Saving of object in a file

			FileOutputStream file = new FileOutputStream(path);
			ObjectOutputStream out = new ObjectOutputStream(file);

			// Method for serialization of object
			out.writeObject(t);

			out.close();
			file.close();

			// System.out.println("Object has been serialized");

		}

		catch (IOException ex) {
			System.out.println("IOException is caught yy");
		}
		// return path;
	}

}

class Deserialize {
	static String pagesPath = "Pages";
	static String tablesPath = "Tables";
	static String indexesPath = "Indexes";

	/**
	 * @author Brolosy
	 * @param tableFileName the .ser filename of a page
	 * @return the table after deserialisation
	 * @throws DBAppException 
	 * @throws Exception either FileNotFoundException or IOException (i think)
	 *                   Deserialises the specified table.
	 */

	@SuppressWarnings("resource")
	public static Table Table(String tableName) throws DBAppException {
		Table t = null;
		String path = tablesPath + "/" + tableName + ".ser";
		try {
			// Reading the object from a file
			FileInputStream file = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(file);

			// Method for deserialization of object
			t = (Table) in.readObject();
           if(t==null)
        	   throw new DBAppException("fileNotFound");
			in.close();
			file.close();

			// System.out.println("Object has been deserialized ");

		}

		catch (IOException ex) {
			System.out.println("IOException is caught zz");
		}

		catch (ClassNotFoundException ex) {
			System.out.println("ClassNotFoundException is caught");
		}
		return t;
	}

	@SuppressWarnings("resource")
	public static Page Page(String pageName) throws DBAppException {
		Page p = null;
		String path = pagesPath + "/" + pageName + ".ser";
		try {
			// Reading the object from a file
			FileInputStream file = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(file);

			// Method for deserialization of object
			p = (Page) in.readObject();
			if(p==null)
	        	   throw new DBAppException("fileNotFound");

			in.close();
			file.close();

			// System.out.println("Object has been deserialized ");

		}

		catch (IOException ex) {
			System.out.println("IOException is caught xx");
		}

		catch (ClassNotFoundException ex) {
			System.out.println("ClassNotFoundException is caught");
		}
		return p;
	}

	@SuppressWarnings({ "rawtypes", "resource" })
	public static bplustree Index(String indexName) throws DBAppException {
		bplustree btree = null;
		String path = indexesPath + "/" + indexName + ".ser";
		try {
			// Reading the object from a file
			FileInputStream file = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(file);

			// Method for deserialization of object
			btree = (bplustree) in.readObject();
			if(btree==null)
	        	   throw new DBAppException("fileNotFound");

			in.close();
			file.close();

			// System.out.println("Object has been deserialized ");

		}

		catch (IOException ex) {
			System.out.println("IOException is caught zz");
		}

		catch (ClassNotFoundException ex) {
			System.out.println("ClassNotFoundException is caught");
		}
		return btree;
	}

}
