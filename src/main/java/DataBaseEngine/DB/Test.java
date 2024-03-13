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

public class Test {
	private String filename = "DBApp.config";
	Properties prop;
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

	@SuppressWarnings("removal")
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

		try{
			
			Page pa = new Page("PC");
			// Serialize created table to a file and appending to array of tables.
			FileOutputStream f = new FileOutputStream("PC.ser");

			ObjectOutput s2 = new ObjectOutputStream(f);

			s2.writeObject(pa);
			s2.close();
			}
			//or should be explicitly DBApp exception?
			catch(Exception e){
				System.out.println(e.getMessage());
			}

			// Deserialize a string and date from a file.
		FileInputStream in1 = new FileInputStream("PC.ser");
		ObjectInputStream s1 = new ObjectInputStream(in1);

		Page pa = (Page)s1.readObject();
		s1.close();



		System.out.println(p.getName());
		System.out.println(pa.getName());


		try{
			
			Page pag = new Page("Book");
			// Serialize created table to a file and appending to array of tables.
			FileOutputStream f = new FileOutputStream("Book.ser");

			ObjectOutput s3 = new ObjectOutputStream(f);

			s3.writeObject(pag);
			s3.close();
			}
			//or should be explicitly DBApp exception?
			catch(Exception e){
				System.out.println(e.getMessage());
			}

			// Deserialize a string and date from a file.
		FileInputStream in2 = new FileInputStream("Book.ser");
		ObjectInputStream s3 = new ObjectInputStream(in2);

		Page pag = (Page)s3.readObject();
		s3.close();
		
		System.out.println(pag.getName());



		Page p4 = new Page("Book");
		System.out.println(p4.getName());

		Page p40 = new Page("CD");
		System.out.println(p40.getName());
		Page p41 = new Page("CD");
		System.out.println(p41.getName());


		Page p5 = new Page("PC");
		System.out.println(p5.getName());
		System.out.println(Page.getTableToNumOfPages());

	
	Hashtable<String,Object> attributesInTuple = new Hashtable<String,Object>();
	attributesInTuple.put("id", new Integer( 23498 ));
	attributesInTuple.put("name", new String("John Noor" ) );
	attributesInTuple.put("gpa", new Double( 1.5 ) );
	String res = "";
	Enumeration<Object> en = attributesInTuple.elements();

	while (en.hasMoreElements()) {
		Object val = en.nextElement();

		res = res + val;
		if(en.hasMoreElements()){
			res = res + ",";
		}
	
	}
	System.out.println(res);

	}}
