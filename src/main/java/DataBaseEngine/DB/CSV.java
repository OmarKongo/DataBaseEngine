package DataBaseEngine.DB;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;


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
    public static final int INDEX_TYPE = 5;


    

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
        CSVReader csvReader = createReader();
        Iterator<String[]> iter = csvReader.iterator();
        while(iter.hasNext()) {
            csvRows.add(iter.next());
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
}
