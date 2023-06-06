package engine;
import java.io.IOException;
import java.io.InputStream;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
public class DBApp { 
	 //static variable: accessed from anywhere in the program by calling class name
	static int MaximumRowsCountinTablePage;
	public static void main(String[] args){
//		String strTableName = "Student";
		String strTableName2 = "Student";
		DBApp dbApp = new DBApp();
		dbApp.init();
		Hashtable<String,String>htblColNameType = new Hashtable<String,String>( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		Hashtable<String,String>htblColNameMin = new Hashtable<String,String>( );
		htblColNameMin.put("id", "1");
		htblColNameMin.put("name", "A");
		htblColNameMin.put("gpa", "0.0");
		Hashtable<String,String>htblColNameMax = new Hashtable<String,String>( );
		htblColNameMax.put("id", "10000000");
		htblColNameMax.put("name", "ZZZZZZZZZZZZZZZZ");
		htblColNameMax.put("gpa", "10.0");
		dbApp.createTable(strTableName2, "id", htblColNameType, htblColNameMin, htblColNameMax);
//		dbApp.createIndex(strTableName2, "id");
//		dbApp.createIndex(strTableName2, "age");
		//dbApp.createTable(strTableName3,"id",htblColNameType, htblColNameMin, htblColNameMax);
		Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>( );
		htblColNameValue.put("id", new Integer( 2343432 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		dbApp.insertIntoTable( strTableName2 , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 453455 ));
		htblColNameValue.put("name", new String("Ahmed Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.95 ) );
		dbApp.insertIntoTable( strTableName2 , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 5674567 ));
		htblColNameValue.put("name", new String("Dalia Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.25 ) );
		dbApp.insertIntoTable( strTableName2 , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 23498 ));
		htblColNameValue.put("name", new String("John Noor" ) );
		htblColNameValue.put("gpa", new Double( 1.5 ) );
		dbApp.insertIntoTable( strTableName2 , htblColNameValue );
		htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer( 78452 ));
		htblColNameValue.put("name", new String("Zaky Noor" ) );
		htblColNameValue.put("gpa", new Double( 0.88 ) );
		dbApp.insertIntoTable( strTableName2 , htblColNameValue );
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[2];
		arrSQLTerms[0] = new SQLTerm();
		arrSQLTerms[0]._strTableName = "Student";
	arrSQLTerms[0]._strColumnName= "name";
		arrSQLTerms[0]._strOperator = "=";
		arrSQLTerms[0]._objValue = "Dalia Noor";
		arrSQLTerms[1] = new SQLTerm();
		arrSQLTerms[1]._strTableName = "Student";
		arrSQLTerms[1]._strColumnName= "id";
		arrSQLTerms[1]._strOperator = "=";
		arrSQLTerms[1]._objValue = new Integer(23);
		String[]strarrOperators = new String[1];
		strarrOperators[0] = "OR";
		// select * from Student where name = “John Noor” or gpa = 1.5;
		Iterator<String[]> resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
		while (resultSet.hasNext()) {
            Object element = resultSet.next();
            System.out.println(element);
        }
	}
	public void init() {
		 Properties prop = new Properties();
	        String fileName = "src/resources/DBApp.config";
	        InputStream is = null;
	        try {
	            is = new FileInputStream(fileName);
	        } catch (FileNotFoundException ex) {
	            ex.printStackTrace();
	        }
	        try {
	            prop.load(is);
	        } catch (IOException ex) {
	            ex.printStackTrace();
	        }
	        MaximumRowsCountinTablePage = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));

	}
//	public void init()throws IOException{
//		FileWriter writer = new FileWriter("D:\\DB_project\\metadata.csv"); 
//	    writer.append("TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType,min,max");
//	    writer.append("\n");
//	    writer.close();
//		  System.out.println("starting write metadata.csv file: ");
//	}
//	public boolean table_existance_check(String table_name) throws IOException {
//		BufferedReader br = new BufferedReader(new FileReader("D:\\DB_project\\metadata.csv"));
//		String line = "";
//		line = br.readLine();
//		while(line !=null){
//			String[]data = line.split(",");
//			if(data[0]==table_name ){
//				br.close();
//				return true;
//			}else if(data[0]!=table_name){
//				continue;
//			}
//		}
//		br.close();
//		return false;
//	}
	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String,String> htblColNameType,
			Hashtable<String,String> htblColNameMin,
			Hashtable<String,String> htblColNameMax)
			{
		// writing in metadata file for each column information to state that table is created in hard_disk
		if(Table.tables_created.isEmpty()) { 
			new Table_information();
		}
//		if(table_existance_check(strTableName) == true){
//			throw new DBAppException("Table is already created");
//		}
		// table must not be created before checking if it is already exists in the metadat with same name
		// declaring and creating table object with input parameter of table name
			// initializing a new table object with reseting table attributes
		Table t = new Table(strTableName);
		try {
		for(Table table:Table.tables_created){
			if(table.name.equals(t.name)) 
			throw new DBAppException("Table aleady exists");
		}
		}catch(DBAppException db) {
			db.printStackTrace();
		}
			// adding newly create table object to arraylist of tables
		Table.tables_created.add(t);
		try {
		 t.create(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
		}catch(DBAppException db){
			db.printStackTrace();
		}
	}
	public void createIndex(String strTableName,String strColName){
		// Exceptions before creating index for the required column
		if(Table.total_no_pages==0 && Sparseindex.created_indices.size()==0) {
			Page.write_pages_file();
		}
		try {
			
		if(table_found(strTableName)==false){
			throw new DBAppException("table not exist");
	}
		for(Table table: Table.tables_created) {
			if(table.name.equals(strTableName)){
			if(!(table.available_attributes.contains(strColName))) {
					throw new DBAppException(table.name + " does not contain column "+strColName);
			}
			else if((table.available_attributes.contains(strColName))) {
				if(index_exist(strTableName,strColName)==false) {
				   Sparseindex index = new Sparseindex(table,strColName);
				   index.create_index();
				}else {
					throw new DBAppException(strColName + " of table "+ strTableName + " already has an index");
				}
		}
			}
		}
		}catch(DBAppException e) {
			e.printStackTrace();
		}
		
	}
	// checking if there is already an index on the column
	public boolean index_exist(String table,String column){
		for(Sparseindex index : Sparseindex.created_indices) {
			if(index.table.name.equals(table) && index.column_name.equals(column)) {
				return true;
			}
		}
		return false;
	}
//		FileWriter fileWriter = new FileWriter("D:\\DB_project\\metadata.csv",true);
//		ArrayList <String> keys = Collections.list(htblColNameType.keys());
//	//	ArrayList <String> keys1 = new ArrayList<String>(htblColNameType.keySet());
//		for(int i = keys.size()-1;i>=0;i--){
//			String s = keys.get(i);
//			fileWriter.append(strTableName);
//			fileWriter.append(",");
//				fileWriter.write(s);
//				fileWriter.append(",");
//				fileWriter.write(htblColNameType.get(s));
//				fileWriter.append(",");
//			if(s.equals(strClusteringKeyColumn)){
//				fileWriter.write("True");
//				fileWriter.append(",");
//			}else {
//				fileWriter.write("False");
//				fileWriter.append(",");
//			}
//			fileWriter.write("null");
//			fileWriter.append(",");
//			fileWriter.write("null");
//			fileWriter.append(",");
//			fileWriter.write(htblColNameMin.get(s));
//			fileWriter.append(",");
//            fileWriter.write(htblColNameMax.get(s));
//            fileWriter.append("\n");	
//		}
//		fileWriter.close();
	
//	public boolean IntegerCheck(String s){
//		int s1 = Integer.parseInt(s);
//		if(s1>=0 & s1<=9) {
//			return true;
//		}
//		return false;
//	}
	public void insertIntoTable(String strTableName,
		Hashtable<String,Object> htblColNameValue){
//		String pk_name = fetch_pk_name(strTableName);
//		if(htblColNameValue.get(pk_name) == null){
//			// primary key must contain a value 
//			throw new DBAppException("Enter a value for the primary key column");
//		}else {
//		Table t;
		if (Table.total_no_pages==0 && Sparseindex.created_indices.size()==0){ // first page for created for all tables
			Page.write_pages_file();
		}
		        
			try {
				if(table_found(strTableName)==true){
					Table t = finding_table(strTableName);
					 t.inserting_row(t.name, htblColNameValue);
				}
				else if(table_found(strTableName)==false){
				throw new DBAppException("Cannot insert into table that did not exist");
			}
			}catch(DBAppException e) {
				e.printStackTrace();
			}
	}
	public void updateTable(String strTableName,
			String strClusteringKeyValue,
			Hashtable<String,Object> htblColNameValue ){
		try {
			if(table_found(strTableName)==true){
				Table t = finding_table(strTableName);
				 t.updating_table(strTableName, strClusteringKeyValue, htblColNameValue);
			}
			else if(table_found(strTableName)==false){
			throw new DBAppException("Cannot update a table that did not exist");
		}
		}catch(DBAppException e) {
			e.printStackTrace();
		}
	}
	public void deleteFromTable(String strTableName,
			Hashtable<String,Object> htblColNameValue){
		try {
			if(table_found(strTableName)==true){
				Table t = finding_table(strTableName);
				 t.deleting_table(strTableName, htblColNameValue);
			}
			else if(table_found(strTableName)==false){
			throw new DBAppException("Cannot delete from a table that did not exist");
		}
		}catch(DBAppException e) {
			e.printStackTrace();
		}
	}
	public Iterator<String[]> selectFromTable(SQLTerm[]sqlterm,String[]starrOperators){
		ArrayList<String[]> a = new ArrayList<String[]>() ;
	//	Iterator<String[]> iterator = s.iterator();
		int operators_count = 0;
		Table t;
		try{
		for(int i = 0;i<sqlterm.length-1;i+=2){
			SQLTerm sqlterm1 = sqlterm[i];
			ArrayList<String[]> a1 = new ArrayList<String[]>();
			SQLTerm sqlterm2 = sqlterm[i+1];
			ArrayList<String[]> a2 = new ArrayList<String[]>();
					if (table_found(sqlterm1._strTableName)==false){
						throw new DBAppException("Cannot select from a table that did not exist");
					}else {
						t = finding_table(sqlterm1._strTableName);
					if(table_found(sqlterm1._strTableName)==true){
					    a1 = t.selecting_from_table(sqlterm1,a1);
					}
					if(table_found(sqlterm2._strTableName)==true){
						t = finding_table(sqlterm2._strTableName);
					    a2 = t.selecting_from_table(sqlterm2,a1);
					}
					if(starrOperators[operators_count].equals("AND")){
						a = this.get_matching_results(a1,a2,sqlterm1,sqlterm2,t);
					}else if(starrOperators[operators_count].equals("OR")) {
						a = this.add_results(a1,a2,sqlterm1,sqlterm2,t);
					}
					operators_count++;
		}
		}
		}catch(DBAppException e) {
			e.printStackTrace();
		}
			return a.iterator();
	}
	public ArrayList<String[]> get_matching_results(ArrayList<String[]> a1, ArrayList<String[]> a2, SQLTerm term1,SQLTerm term2,Table t) {
		ArrayList<String[]> a = new ArrayList<String[]>() ;
		int loc1 = 0 ;
		int loc2= 0;
		try {
		 loc1 = t.col_location(term1._strColumnName);
		 loc2 = t.col_location(term2._strColumnName);
	}catch(IOException e) {
		e.printStackTrace();
	}
		for(int i = 0;i<a1.size();i++){
			String[] s1 = a1.get(i);
			for(int j = 0;j<a2.size();j++){
				String[] s2 = a2.get(j);
				if(term1._objValue.equals(s2[loc1]) && term2._objValue.equals(s1[loc2])){
					a.add(s1);
					}
			}
		}
		return a;
	}
	public ArrayList<String[]> add_results(ArrayList<String[]> a1, ArrayList<String[]> a2,SQLTerm term1,SQLTerm term2,Table t){
		ArrayList<String[]> a = new ArrayList<String[]>() ;
		int loc1 = 0 ;
		int loc2= 0;
		try {
		 loc1 = t.col_location(term1._strColumnName);
		 loc2 = t.col_location(term2._strColumnName);
	}catch(IOException e) {
		e.printStackTrace();
	}
		for(int i = 0;i<a1.size();i++){
			String[] s1 = a1.get(i);
			for(int j = 0;j<a2.size();j++){
				String[] s2 = a2.get(j);
				if(term1._objValue.equals(s2[loc1]) && term2._objValue.equals(s1[loc2])){
					a.add(s1);
					}
				else {
					a.add(s1);
					a.add(s2);
				}
			}
		}
		return a;
	}
	public Table finding_table(String name){
		for(Table table:Table.tables_created){
			if(table.name.equals(name))
				return table;
		}
		return null;
	}
	public boolean table_found(String name){
		for(Table table:Table.tables_created){
			if(table.name.equals(name))
				return true;
		}
		return false;
}
}


