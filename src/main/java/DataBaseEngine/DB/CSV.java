package DataBaseEngine.DB;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;


public class CSV {

    private CSV() {
    }

    public static final String FILE_PATH = "metadata.csv";
    /*
     * column headers + indices of csv file to enhance readability and to introduce flexibility
     */
    public static final int NUMBER_OF_COLUMNS = 6;
    public static final int TABLE_NAME_INDEX = 0;
    public static final int COLUMN_NAME_INDEX = 1;
    public static final int COLUMN_TYPE_INDEX = 2;
    public static final int CLUSTERING_KEY_INDEX = 3;
    public static final int INDEX_NAME_INDEX = 4;
    public static final int INDEX_TYPE_INDEX = 5;
    /*
     * column types allowed in DB app
     */
    public static final String STRING_TYPE = "java.lang.String";
	public static final String DOUBLE_TYPE = "java.lang.Double";
	public static final String INTEGER_TYPE = "java.lang.Integer";


    

    public static CSVReader createReader() {

        CSVReader csvReader = null;
        try {
            csvReader = new CSVReaderBuilder(new FileReader(FILE_PATH)) 
				.withSkipLines(1) 
				.build();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
		
		return csvReader;
	} 

    public static ArrayList<String[]> getCSV() {
        ArrayList<String[]> csvRows = new ArrayList<>();
        
        try(CSVReader csvReader = createReader()) {
        Iterator<String[]> iter = csvReader.iterator();
        while(iter.hasNext()) {
            csvRows.add(iter.next());
        }
        } catch(IOException e) {
            e.printStackTrace();
        }
        return csvRows;
    }

    public static ArrayList<String[]> getTableRows(String tableName) {
        ArrayList<String[]> tableRows = new ArrayList<>();
        Iterator<String[]> csvIter = getCSV().iterator();
        String[] row = new String[NUMBER_OF_COLUMNS];
        while(csvIter.hasNext()) {
            row = csvIter.next();
            if(row[TABLE_NAME_INDEX].equals(tableName))
                tableRows.add(row);
        }
        return tableRows;
    }

    public static String[] getTableColumnRow(String tableName, String column) {
        ArrayList<String[]> csvRows = getTableRows(tableName);

        Iterator<String[]> arrayListIterator = csvRows.iterator();
        while(arrayListIterator.hasNext()) {
            String[] row = arrayListIterator.next();
            if(row[COLUMN_NAME_INDEX].equals(column)) 
                return row;
        }
        System.err.printf("Row not found based on given arguments\nTableName: %s\nColumn: %s\n",
            tableName, column);
        return null;
    }

    public static String getcell(String tableName, String column, int rowIndex){
        String[] csvRow = getTableColumnRow(tableName, column);
        return csvRow[rowIndex];
    }

    private static boolean checkIndexFileExists(String indexName) {
        File file = new File(Serialize.indexPath + File.separator + indexName + ".ser");
        return file.exists();
    }

    public static boolean writeIndex
        (String tableName, String columnName, String indexName) {

        if(checkIndexFileExists(indexName)) {
            System.err.println("An index file with this name already exists, please pick to a unique name.\n" +
                "A common convention is '<tableName><columnName>Index' in camelCase.");
            return false;
        }

        //Create a new csv file to move data to
        File tempCSV = new File("tempmetadata.csv");

        //used when checking for valid conditions
        boolean rowExists = false;
        boolean cellEmpty = false;

        try(            
            CSVWriter writer = new CSVWriter(new FileWriter(tempCSV));
            //created new csvReader that goes through all of rows as opposed to createReader() method
            CSVReader reader = new CSVReaderBuilder(new FileReader(FILE_PATH)).build();
        ) {

            tempCSV.createNewFile();

            Iterator<String[]> iter = reader.iterator();

            while(iter.hasNext()){
                String[] row = iter.next();

                if(row[TABLE_NAME_INDEX].equals(tableName) && 
                    row[COLUMN_NAME_INDEX].equals(columnName)){
                        rowExists = true;
                        if(!row[INDEX_NAME_INDEX].isBlank()) break;
                        cellEmpty = true;
                        row[INDEX_NAME_INDEX] = indexName;
                        row[INDEX_TYPE_INDEX] = "B+Tree";
                    }
                writer.writeNext(row);
            }
        } catch(IOException e) {
            e.printStackTrace();
        }

        if(rowExists && cellEmpty) {
            File oldCSV = new File(FILE_PATH);

            boolean deleteOP = oldCSV.delete();
            System.out.println("Deletion is: " + deleteOP);

            boolean renamingOP = tempCSV.renameTo(oldCSV);
            System.out.println("Renaming operation is: " + renamingOP);

            System.out.println("Added the value: " + cellEmpty + " to its location successfully");
            return true;
        }
        //failed to write to cell
        tempCSV.delete();

        if(!rowExists) {
            System.out.println("Row isn't found based on given arguments!");
        } else {
            System.out.println("Cell isn't empty and can't be overriden!");
        }
        return false;
    }
}
