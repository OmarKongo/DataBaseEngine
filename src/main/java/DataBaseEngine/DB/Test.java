package DataBaseEngine.DB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Test {
	private String filename = "DBApp.config";
	Properties prop;int x;
	public Test()  throws Exception{
		try(FileInputStream fis = new FileInputStream(filename) ){
			prop = new Properties();
			prop.load(fis);
			
		}catch(FileNotFoundException ex) {
			ex.printStackTrace();
		}
	}
	public int getMaxCount() {
	  String num = prop.getProperty("MaximumRowsCountinPage");
	  return Integer.parseInt(num);
		
	}

	@SuppressWarnings({ "removal", "deprecation" })
	public static void main(String[] args) throws Exception {
		
		Test T = new Test();
		int max = T.getMaxCount();
		System.out.println(max); //200
		/* 
		Page p = new Page("Student");
		Page p1 = new Page("Student");
		Page p2 = new Page("Book");
		System.out.println(p.getName());
		System.out.println(p1.getName());
		System.out.println(p2.getName());
		*/
		try{
			
			Page p = new Page("PC");
			// Serialize created table to a file and appending to array of tables.
			FileOutputStream f = new FileOutputStream("PC.ser");

			ObjectOutput s = new ObjectOutputStream(f);
			s.writeObject(p);
			s.close();
			}
			//or should be explicitly DBApp exception?
			catch(Exception e){
				System.out.println(e.getMessage());
			}

			// Deserialize a string and date from a file.
		FileInputStream in = new FileInputStream("PC.ser");
		ObjectInputStream s = new ObjectInputStream(in);

		Page p = (Page)s.readObject();
		s.close();
		System.out.println(p.getName());



	
	Hashtable<String,Object> attributesInTuple = new Hashtable<String,Object>();
	attributesInTuple.put("id", new Integer( 23498 ));
	attributesInTuple.put("name", new String("John Noor" ) );
	attributesInTuple.put("gpa", new Double( 1.5 ) );
	
	Tuple t1 = new Tuple(attributesInTuple);


	Hashtable<String,Object> attributesInTuple2 = new Hashtable<String,Object>();
	attributesInTuple2.put("id", new Integer( 2615 ));
	attributesInTuple2.put("name", new String("brolo" ) );
	attributesInTuple2.put("gpa", new Double( 1.2 ) );

	Tuple t2 = new Tuple(attributesInTuple2);

	String res = "";
	Enumeration<Object> en = attributesInTuple.elements();

	while (en.hasMoreElements()) {
		Object val = en.nextElement();

		res = val + res;
		if(en.hasMoreElements()){
			res = "," + res;
		}
	
	}
	System.out.println(res);
	//testing the page toString()
	Vector<Tuple> tuplesInPage = new Vector<Tuple>();
	tuplesInPage.add(t1);
	tuplesInPage.add(t2);
	String res2 = "";
	for(int i = 0; i<tuplesInPage.size(); i++){
		res2 = res2 + tuplesInPage.elementAt(i).toString();
		if(i!=tuplesInPage.size()-1){
			res2 = res2 + ",";
		}
	}

	//makes all separated by commas
	System.out.println(res2);


	}}
