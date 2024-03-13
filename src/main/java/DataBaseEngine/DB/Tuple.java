package DataBaseEngine.DB;

import java.util.Enumeration;
import java.util.Hashtable;


public class Tuple {
    Hashtable<String, Object> attributesInTuple = new Hashtable<String,Object>();

    public Tuple(Hashtable<String, Object> attributesInTuple) {
        this.attributesInTuple = attributesInTuple;
    }

    public Hashtable<String, Object> getAttributesInTuple() {
        return attributesInTuple;
    }

    public void setAttributesInTuple(Hashtable<String, Object> attributesInTuple) {
        this.attributesInTuple = attributesInTuple;
    }

    @SuppressWarnings("removal")
    public String toString(){
        String res = "";

        this.attributesInTuple.put("id", new Integer( 23498 ));
        this.attributesInTuple.put("name", new String("John Noor" ) );
        this.attributesInTuple.put("gpa", new Double( 1.5 ) );

        Enumeration<Object> en = attributesInTuple.elements();
    
        while (en.hasMoreElements()) {
            Object val = en.nextElement();
    
            res = res + val;
            if(en.hasMoreElements()){
                res = res + ",";
            }
        
        }
        return res;

    }
    
}
