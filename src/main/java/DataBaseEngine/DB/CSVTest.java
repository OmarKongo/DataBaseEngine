import java.util.Arrays;

import DataBaseEngine.DB.CSV;

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
			CSV.getcell("Doctor", "id", CSV.TABLE_NAME_INDEX)
		);

    }
}
