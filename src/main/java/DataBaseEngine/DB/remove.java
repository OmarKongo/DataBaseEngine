package DataBaseEngine.DB;

import DataBaseEngine.DB.bplustree.DictionaryPair;

public class remove {
    public static void main(String[] args) {
        String s = "lol";
        Comparable<String> cs = s;
        Comparable<String> a = s;
        String b = "hohoho";

        int result = a.compareTo(b);
        System.out.println("The types of objects being compared are:\n" 
            + "variable <a> is of type " + a.getClass().toGenericString() + " with value: " + a +
            "\n"
            + "variable <b> is of type " + b.getClass().toGenericString() + " with value: " + b +
            "\n"
            + "The result of a.compareTo(b) is: " + result);

        "lol".compareTo(s);

        Key k1 = new Key(5);
        Key k2 = new Key("hello");

        // k1.compareTo(k2);

        Double d = 5.0;
        Integer i = 5;
        String str = "lol";

        System.out.printf(
                "className of:\n Doube: %s\n Integer: %s\n String: %s\n",
                 d.getClass().descriptorString(),
                  i.getClass().descriptorString(),
                   str.getClass().descriptorString());
    }
}
