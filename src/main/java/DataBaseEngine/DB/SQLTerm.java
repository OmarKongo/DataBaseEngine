package DataBaseEngine.DB;
/** * @author Wael Abouelsaadat */ 

public class SQLTerm {

	public String _strTableName,_strColumnName, _strOperator;
	public Object _objValue;
	public static int instanceCount = 0;
	public int x = 0;

	public SQLTerm(  ){
		++instanceCount;
		x = instanceCount;
	}

	@Override
	public String toString(){
		return "SQLTerm "+x ;
	}

}