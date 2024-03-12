package DataBaseEngine.DB;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;

public class Table implements Serializable{
    //Is it correct that only the pageFileName are persistent?

    ArrayList<String> pageFileNames;
    transient String strTableName;
    transient String strClusteringKeyColumn;
    transient Hashtable<String,String> htblColNameType;



    public Table(String strTableName, String strClusteringKeyColumn,
            Hashtable<String, String> htblColNameType) {
        this.pageFileNames = new ArrayList<>();
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.htblColNameType = htblColNameType;
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

    public void appendPage(String pageFileName){
        pageFileNames.add(pageFileName);
    }
    
    public void insertRow(){
        //logic to choose page
        //deserialise correct page Page.p
        //pinsertIntoPage()

        //if specific chosen page is full,
        //create new page
        //insert in it
        //serialise
        //append page file name 
    }

    public void updateRow(){
        
    }

    public void deleteRow(){

    }

    public void selectRow(){

    }


 
    /**
     * @author Brolosy
     * @return A new Page
     */
    public Page createPage(){
        
        Page p = new Page(this.getStrTableName());
        return p;

    }

    /**
     * @author Brolosy
     * @param p the page to be serialised
     * serialises page, then appends to the pageFileNames array
     */
    public void serialisePage(Page p){
        try{
            String filename = p.getName()+".ser";
            this.pageFileNames.add(filename);
            FileOutputStream f = new FileOutputStream(filename);
            
            ObjectOutput s = new ObjectOutputStream(f);
            s.writeObject(p);
            s.close();
            }
        //or should be explicitly DBApp exception?
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    /**
     * @author Brolosy
     * @param pageFileName the .ser filename of a page
     * @return the page after deserialisation
     * @throws Exception either FileNotFoundException or IOException (i think)
     * Deserialises the specified page.
     */
    public Page deserialisePage(String pageFileName) throws Exception{
		// Deserialize a string and date from a file.
        //must make try catch statement when calling this
		FileInputStream in = new FileInputStream(pageFileName);
		ObjectInputStream s = new ObjectInputStream(in);

		Page p = (Page)s.readObject();
		s.close();

        return p;
    }
    
    
}
