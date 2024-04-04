package DataBaseEngine.DB;

import java.util.ArrayList;

public interface BtreeInterface {

    public void insert(bplustree.DictionaryPair dp);

    public ArrayList<String> search(Comparable key);

    public ArrayList<String> search(Comparable lowerbound,Comparable upperbound);

    public ArrayList<String> delete(bplustree.DictionaryPair dp);

    public void update(bplustree.DictionaryPair oldDP,Comparable newkey);
}