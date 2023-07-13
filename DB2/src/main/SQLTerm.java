package main;

public class SQLTerm {
	
	 String _strTableName;
	 String _strColumnName;
	 String _strOperator;
	 Object _objValue;
	 
	 public SQLTerm() {
		 
	 }
	 
	 
	 public void inverse() {
		 
		 
		 if(_strOperator.equals(">")) 
			 _strOperator = "<=";
		 
		 
		 else if(_strOperator.equals(">=")) 
			 _strOperator = "<";
		 
		 
		 else if(_strOperator.equals("<")) 
			 _strOperator = ">=";
		 
		 
		 else if(_strOperator.equals("<=")) 
			 _strOperator = ">";
		 
		 
		 else if(_strOperator.equals("=")) 
			 _strOperator = "!=";
		 
		 
		 else if(_strOperator.equals("!=")) 
			 _strOperator = "=";
	 }
}