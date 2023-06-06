package engine;
public class DBAppException extends Exception {
	String Exception;
	public DBAppException(String s){
		super(s);
	}
//	public String getdatatypemessage(){
//		return "Incorrect data type";
//	}
//	public String getpkmessage() {
//		return "Enter a value for primary key";
//	}
}
