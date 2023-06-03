public class SQLTerm {
    public String _strTableName;
    public String _strColumnName;
    public String _strOperator;
    public Object _objValue;

   
        public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue) throws DBAppException {
            this._strTableName =strTableName;
            this._strColumnName =strColumnName;
            if(strOperator.equals(">") || strOperator.equals(">=") ||
                    strOperator.equals("<") || strOperator.equals("<=")||
                    strOperator.equals("=") || strOperator.equals("!="))
                this._strOperator =strOperator;
            else
                throw new DBAppException("Invalid operator");
            this._objValue =objValue;

        }
    }