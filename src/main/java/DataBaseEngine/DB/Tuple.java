package DataBaseEngine.DB;

import java.util.Enumeration;
import java.util.Hashtable;


public class Tuple {
    private Hashtable<String, Object> attributesInTuple = new Hashtable<String,Object>();

    public Tuple(Hashtable<String, Object> attributesInTuple) {
        this.attributesInTuple = attributesInTuple;
    }

    public Hashtable<String, Object> getAttributesInTuple() {
        return attributesInTuple;
    }

    public void setAttributesInTuple(Hashtable<String, Object> attributesInTuple) {
        this.attributesInTuple = attributesInTuple;
    }



    public String toString(){
        String res = "";



        Enumeration<Object> en = getAttributesInTuple().elements();
    
        while (en.hasMoreElements()) {
            Object val = en.nextElement();

            res = val + res;
            if(en.hasMoreElements()){
                res = "," + res;
            }
        
        }
        return res;

    }
    
}
