package DataBaseEngine.DB;
/*ayaaa */
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

	public static void main(String[] args) throws Exception {
		
		Test T = new Test();
		int max = T.getMaxCount();
		System.out.print(max); //200
		

	}

}
