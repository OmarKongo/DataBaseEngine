package DataBaseEngine.DB;
/** * @author Wael Abouelsaadat */ 

public class SQLTerm {

	public String _strTableName,_strColumnName, _strOperator;
	public Object _objValue;

	public SQLTerm(  ){
		
	}

	@Override
	public String toString() {
		return "SQLTerm [_strColumnName=" + _strColumnName + ", _strOperator=" + _strOperator + ", _objValue="
				+ _objValue + "]";
	}

}