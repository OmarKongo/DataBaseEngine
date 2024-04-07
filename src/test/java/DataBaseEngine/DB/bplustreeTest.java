package DataBaseEngine.DB;
import java.util.ArrayList;
import java.util.Collections;

public class bplustreeTest {
    public static void main(String[] args) throws ClassNotFoundException {

        //dummy data to add to tree
        bplustree<String> bpt = new bplustree<>(String.class, 10);

        for(int i = 0; i < 100; i++) {
            String key = "key" + (i % 10);
            String page = "page" + i;
            ArrayList<String> pageNames = new ArrayList<>();
            pageNames.add(page);

            bpt.insert(key, pageNames);
        }

        //testing tree methods with data inserted
        ArrayList<String> pages = bpt.search("key3");

        System.out.println(pages + "size: " + pages.size());

        ArrayList<String> deletedPages = new ArrayList<>();
        deletedPages.add("page23");
        deletedPages.add("Page93");

        System.out.println("Deleting key3 pages");

        bpt.delete("key3", deletedPages);
        System.out.println(bpt.search("key3"));

        /*
         * focuses on update method
         */
        ArrayList<String> movePages = new ArrayList<>();
        Collections.addAll(movePages, "page5", "page10");

        System.out.println(bpt.search("key3"));
        System.out.println(bpt.search("key9"));

        bpt.update("key5", movePages, "key9");

        System.out.println(bpt.search("key5"));
        System.out.println(bpt.search("key9"));

        ArrayList<String> stringSearch = bpt.search("key8", "key10");
        stringSearch.forEach(element -> System.out.println(element));

        /*
         * Testing serialization process!
         */

        //before serialization
        ArrayList<String> keyTenPagesBefore = bpt.search("key10");
        System.out.println(keyTenPagesBefore);

        keyTenPagesBefore.add("pageTen");
        bpt.insert("key10", keyTenPagesBefore);

        Serialize.bplustree(bpt, "StudentIndex");
        bplustree<String> bptNew = (bplustree<String>) Deserialize.bplustree("StudentIndex");
        
        //After serialization
        ArrayList<String> keyTenPagesAfter = bpt.search("key10");
        System.out.println(keyTenPagesAfter);

        //Using non-generic form of bplustree
        bplustree btree = new bplustree(String.class, 3);
        btree.insert("key10", pages);
        ArrayList<String> getPages = btree.search("key10");
        System.out.println(getPages);
    }
}
