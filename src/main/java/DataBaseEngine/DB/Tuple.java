package DataBaseEngine.DB;

import java.util.Vector;

public class Tuple {
    Vector<Object> attributesInTuple = new Vector<Object>();

    public Tuple(Vector<Object> attributesInTuple) {
        this.attributesInTuple = attributesInTuple;
    }

    public Vector<Object> getAttributesInTuple() {
        return attributesInTuple;
    }

    public void setAttributesInTuple(Vector<Object> attributesInTuple) {
        this.attributesInTuple = attributesInTuple;
    }

    public String toString(){
        String res = "";
        for(int i = 0; i<attributesInTuple.size(); i++){
            //what does the toString of Object class do?
            //not yet finished because I dont know how to convert from different column types stored in object toString()
            //default toString() of object is a bit not specific
            res = res + attributesInTuple.elementAt(i).toString();
            if(i!=attributesInTuple.size()-1){
                res = res + ",";
            }
        }
        return res;
    }
    
}
