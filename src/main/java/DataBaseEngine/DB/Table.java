package DataBaseEngine.DB;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import com.opencsv.CSVWriter;

public class Table implements Serializable{
    //Is it correct that only the pageFileName are persistent?

    private ArrayList<String> pageFileNames;
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
        this.pageFileNames = new ArrayList<>();
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.htblColNameType = htblColNameType;

    }

    public int getPagesCounter() {
        return pagesCounter;
    }

    public void setPagesCounter(int pagesCounter) {
        this.pagesCounter = pagesCounter;
    }

    public ArrayList<String> getPageFileNames() {
        return pageFileNames;
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

    public void setHtblColNameType(Hashtable<String, String> htblColNameType) {
        this.htblColNameType = htblColNameType;
    }







    public void addTable(String tableName,String pK,String filePath) {
        try {
            checkDataType(this.htblColNameType);
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

    public static void checkDataType(Hashtable<String,String> htblColNameType) throws DBAppException{
		for(String s : htblColNameType.values()){
			if(!(s.equals("java.lang.Integer") || s.equals("java.lang.String") || s.equals("java.lang.Double"))){
				throw new DBAppException("Column DataType invalid");
			}
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
/* 
    public Page createPage(String tableName) {
		int counter = this.getPagesCounter();
		String pageName = tableName+counter;
		Page p = new Page(pageName);
		this.setPagesCounter(++counter);
        
        String serializedName = Serialize.serializePage(p);
		this.getPageFileNames().add(serializedName);
		
		
		return p;
		
	}
*/
    public Page createPage(){
            
        int counter = this.getPagesCounter();
		String pageName = this.getStrTableName()+counter;
		Page p = new Page(pageName);
        this.setPagesCounter(++counter);
        return p;

    }


    public void appendPage(String pageFileName){
        pageFileNames.add(pageFileName);
    }
    
   // public void insertRow(){
        //logic to choose page
        //deserialise correct page Page.p
        //pinsertIntoPage()

        //if specific chosen page is full,
        //create new page
        //insert in it
        //serialise
        //append page file name 
    //}

    public void updateRow(){
        
    }

    public void deleteRow(){

    }

    public void selectRow(){

    }

    /**
     * @author Brolosy
     * @param p the table to be serialised
     * serialises table
     * @return returns filename of type .ser
     */

    public String serialiseTable(){
        String filename = this.getStrTableName()+".ser";
        try{
            //this.pageFileNames.add(filename);
            FileOutputStream f = new FileOutputStream(filename);
            
            ObjectOutput s = new ObjectOutputStream(f);
            s.writeObject(this);
            s.close();
            }
        //or should be explicitly DBApp exception?
        catch(Exception e){
            System.out.println(e.getMessage());
        }
        return filename;
    }

    /**
     * @author Brolosy
     * @param tableFileName the .ser filename of a page
     * @return the table after deserialisation
     * @throws Exception either FileNotFoundException or IOException (i think)
     * Deserialises the specified table.
     */
    public Table deserialiseTable(String tableFileName) throws Exception{
		// Deserialize a string and date from a file.
        //must make try catch statement when calling this
		FileInputStream in = new FileInputStream(tableFileName);
		ObjectInputStream s = new ObjectInputStream(in);

		Table t = (Table)s.readObject();
		s.close();

        return t;
    }


    
    
}
