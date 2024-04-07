package DataBaseEngine.DB;
import java.util.ArrayList;
import java.util.Arrays;

public class CSVTest {
    public static void main(String[] args) {
        
        CSV.getCSV().forEach(element -> {
			String representation = Arrays.toString(element) + " Size: " + element.length;
			System.out.println(representation);
		});

		System.out.println("New Test!!!");
		
		CSV.getTableRows("Doctor").forEach(element -> {
			String representation = Arrays.toString(element) + " Size: " + element.length;
			System.out.println(representation);
		});
		int[] testArray = {1, 2, 3, 4, 5};
		System.out.println(testArray[0]);

		System.out.println(
			Arrays.toString(CSV.getTableColumnRow("Doctor", "Faculty")));

		//Get a cell provided a tableName, columnName, and index are given
		//final static variables are defined in csv to indicate what each index maps to in csv
		System.out.println(
			CSV.getcell("TA", "id", CSV.TABLE_NAME_INDEX)
		);

		ArrayList<String[]> rows = CSV.getTableRows("Student");
        rows.forEach(element -> {
            for(int i = 0; i < element.length; i++) {
                System.out.println(element[i]);
            }
        });

        /*
        //tests array length and array.isEmpty vs array.isBlank
        boolean state = CSV.writeCell("Student", "gpa", CSV.INDEX_NAME_INDEX, "StudentIndex", false);
        System.out.println(state);

        String[] row = CSV.getTableColumnRow("Student", "name");
        System.out.println(row.length);
        System.out.println(row[CSV.INDEX_NAME_INDEX].isEmpty());
         */
        
        /*
        //tests File class features
        File file1 = new File("metadata.csv");
        String absolutePath = file1.getAbsolutePath();
        System.out.println(absolutePath);
        File file2 = new File("tempmetadata.csv");
        String path = file2.getAbsolutePath();
        System.out.println(path);

        // file1.createNewFile();
        // file1.delete();
        // File file2 = new File("bye.txt");
        // file2.createNewFile();

        // boolean result = file2.renameTo(file1);
        // System.out.println(result);
        */
        /*
         * tests CSV.writeCell method
         */

        // boolean result = CSV.writeIndex("Student", "id", "StudentIndex");
        // System.out.println(result);

        // boolean exists = Serialize.checkExisting(Main.INDEX_DIRECTORY_PATH, "StudentIndex");
        // System.out.println(exists);

        /*
         * testing separatorChar vs pathSeparator
         */
        System.out.println(Serialize.indexPath + "StudentIndex" + ".ser");
        // System.out.println(File.separatorChar);

        System.out.println(CSV.getcell("Student", "gpa", CSV.COLUMN_TYPE_INDEX));
    }
}