package engine;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.ListIterator;
import engine.Table_information;
public class Table{
	String name;
	ArrayList<Page> created_Pages;// for  between Pages 
	ArrayList<String> available_attributes; // storing names of the attributes
	String Clustering_key;
	int page_number;
	//static Page_information info;
	Page current_page;
	static ArrayList<Table> tables_created = new ArrayList<Table>();
	ArrayList<Sparseindex> tables_indices = new ArrayList<Sparseindex>();
	static int total_no_pages;
//	public Table() {
//		this.name = name;
//		this.page_number = page_number;
//	}
	public Table(String name){
		this.name = name;
		page_number = 0;
		this.created_Pages = new ArrayList<Page>();
		this.available_attributes= new ArrayList<String>();
	}
//	public Table(String name,String strClusteringKeyColumn,
//			Hashtable<String,String> htblColNameType,
//			Hashtable<String,String> htblColNameMin,
//			Hashtable<String,String> htblColNameMax)
//			throws DBAppException,IOException{
//		FileWriter fileWriter = new FileWriter("D:\\DB_project\\metadata.csv",true);
//		ArrayList <String> keys = Collections.list(htblColNameType.keys());
//	//	ArrayList <String> keys1 = new ArrayList<String>(htblColNameType.keySet());
//		for(int i = keys.size()-1;i>=0;i--){
//			String s = keys.get(i);
//			fileWriter.append(this.name);
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
	
	public void create(String name,String strClusteringKeyColumn,
			Hashtable<String,String> htblColNameType,
			Hashtable<String,String> htblColNameMin,
			Hashtable<String,String> htblColNameMax) throws DBAppException{
		try {
		FileWriter fileWriter = new FileWriter("src/resources/metadata.csv",true);
		ArrayList <String> keys = Collections.list(htblColNameType.keys());
	//	ArrayList <String> keys1 = new ArrayList<String>(htblColNameType.keySet());
		for(int i = keys.size()-1;i>=0;i--){
			String key = keys.get(i);
			this.available_attributes.add(key);
			fileWriter.append(this.name);
			fileWriter.append(",");
				fileWriter.write(key);
				fileWriter.append(",");
				if(htblColNameType.get(key).equals("java.lang.Integer") || htblColNameType.get(key).equals("java.lang.String")
						|| htblColNameType.get(key).equals("java.lang.Double") || htblColNameType.get(key).equals("java.util.Date")) {
				fileWriter.write(htblColNameType.get(key));
				}else{
					throw new DBAppException("Data type entered for "+key+" is unaccepted");
				}
				fileWriter.append(",");
			if(key.equals(strClusteringKeyColumn)){
				this.Clustering_key = strClusteringKeyColumn;
				fileWriter.write("True");
				fileWriter.append(",");
			}else {
				fileWriter.write("False");
				fileWriter.append(",");
			}
			fileWriter.write("null");
			fileWriter.append(",");
			fileWriter.write("null");
			fileWriter.append(",");
			if(data_checking_createtable(htblColNameType.get(key),htblColNameMin.get(key))==false){
				throw new DBAppException("Minimum value of "+key+" is unaccepted");
			}
			fileWriter.write(htblColNameMin.get(key));
			fileWriter.append(",");
			if(data_checking_createtable(htblColNameType.get(key),htblColNameMin.get(key))==false) {
				throw new DBAppException("Maximum value of "+key+" is unaccepted");
			}
			if(htblColNameType.get(key).equals("java.lang.Integer")){
				Integer Min_int = Integer.parseInt(htblColNameMin.get(key));
				Integer Max_int = Integer.parseInt(htblColNameMax.get(key));
				if(Min_int>Max_int) {
					throw new DBAppException("Minimum value of Attribute "+ key + " is bigger than maximum value");
				}
			}else if(htblColNameType.get(key).equals("java.lang.String")){
				if(htblColNameMin.get(key).compareTo(htblColNameMax.get(key))>0) {
					throw new DBAppException("Minimum value of Attribute "+ key + " is bigger than maximum value");
				}
			}else if(htblColNameType.get(key).equals("java.lang.Double")){
				Double Min_double = Double.parseDouble(htblColNameMin.get(key));
				Double Max_double = Double.parseDouble(htblColNameMax.get(key));
				if(Min_double.compareTo(Max_double)>0){
					throw new DBAppException("Minimum value of Attribute "+ key + " is bigger than maximum value");
				}
			}else if(htblColNameType.get(key).equals("java.util.Date")) {
				try {
				Date min_date = new SimpleDateFormat("YYYY-MM-DD").parse(htblColNameMin.get(key));
				 Date max_date =  new SimpleDateFormat("YYYY-MM-DD").parse(htblColNameMax.get(key));
				 if(min_date.compareTo(max_date)>0){
					 throw new DBAppException("Minimum value of Attribute "+ key + " is bigger than maximum value");
				 }
				 }catch(ParseException db) {
					 db.printStackTrace();
				 }
				}
            fileWriter.write(htblColNameMax.get(key));
            fileWriter.append("\n");	
		}
		fileWriter.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		}
	public boolean data_checking_createtable(String data_type,String value) {
		if(data_type.equals("java.lang.Integer")) {
			Integer value_int = Integer.parseInt(value);
			return value_int instanceof Integer;
		}else if(data_type.equals("java.lang.String")){
			return value instanceof String;
		}else if(data_type.equals("java.lang.Double")){
			Double value_double = Double.parseDouble(value);
			return value_double instanceof Double;
		}else if(data_type.equals("java.util.Date")){
			try {
			Date value_date = new SimpleDateFormat("YYYY-MM-DD").parse(value);
			return value_date instanceof Date;
		}catch(ParseException p) {
			p.printStackTrace();
		}
	}
		return false;
	}
	public void inserting_row(String strTableName,Hashtable<String,Object>htblColNameValue) throws DBAppException{
		ArrayList<Object> tuple = new ArrayList<Object>();
		ArrayList <String> keys = Collections.list(htblColNameValue.keys()); // ArrayList of keys
		// To get value stored in every col_key
		// this.name = strTableName;
		// check if no pages created before, create the first page object
		
			// pages arraylist is empty
		 // file.csv is created when the first page for the first table is created
		if(!(htblColNameValue.containsKey(this.Clustering_key))){
			throw new DBAppException("Primary key of table "+ this.name + " does not exist in the input");
		}
		if(this.key_duplicate_checking(this.Clustering_key,htblColNameValue.get(this.Clustering_key))==true) {
			throw new DBAppException("Primary_key value " + htblColNameValue.get(this.Clustering_key)+" is already inserted before");
		}
		for(int i = keys.size()-1;i>=0;i--){
			// iterate backwards 
			// iterate through attribute name arraylist to show user which col name will need to be changed
			String key = keys.get(i);
			if(!(this.available_attributes.contains(key))) {
				throw new DBAppException("Table does not contain a column called "+ key);
			}
				if(htblColNameValue.get(this.Clustering_key).equals(" ")) {
					throw new DBAppException("Key Column must have a value");
				}
			if(this.datatype_checking(key,htblColNameValue.get(key)) == false){
				throw new DBAppException("An Invalid datatype for "+ key);
			}
			if(this.value_within_range(key,htblColNameValue.get(key))== false){
				throw new DBAppException("out of range value for "+ key);
			}
			// only primary key does not contain duplicates
			tuple.add(htblColNameValue.get(key));
			}
			//tuple.add(htblColNameValue.get(key));
				// inserting a row must begin from the first from the first page of the table 
				// in order to sort key values correctly
		if(this.created_Pages.isEmpty()) { // first page in the table
			current_page = new Page(this);
		    current_page.create_new_page(tuple);
		}else {
		for(Sparseindex index: this.tables_indices){
			if(index.column_name.equals(this.Clustering_key)){
				if(this.created_Pages.size()==1 && current_page.rows_used==1)
					index.write_one_level();
			index.insert_one_level(tuple);
			return;
			}
		}
		current_page = this.created_Pages.get(0); 
	    current_page.sorted_insertion(tuple,0);
		}
	}
	//	current_page.addrow(tuple);
	
//	public void sorting_insertion(ArrayList<Object>tuple) {
//		for(Page p: this.created_Pages) {
//			if(p.sorted_insertion(tuple)) {
//				
//			}
//		}
//	}
	public void updating_table(String strTableName,String strClusteringKeyValue,Hashtable<String,Object> htblColNameValue) throws DBAppException {
		// htblColNameValue does not contain key_value of specific col
		// checking if evey input_value fits with col data_type
		//ArrayList<Object> tuple = new ArrayList<Object>();
		ArrayList <String> keys = Collections.list(htblColNameValue.keys()); // ArrayList of keys
		if(this.created_Pages.size()==0) {
			// no rows to be deleted from this table
			throw new DBAppException(this.name + " table contains no rows to be updated");
		}
		if(this.key_duplicate_checking(this.Clustering_key,strClusteringKeyValue)==false) {
			throw new DBAppException("Primary_key value not exist in the table to update row");
		}
		for(int i = keys.size()-1;i>=0;i--) {
			String key = keys.get(i);
			if(!(this.available_attributes.contains(key))) {
				throw new DBAppException("Table does not contain a column called "+key);
			}
			if(this.datatype_checking(key,htblColNameValue.get(key)) == false){
				throw new DBAppException("An Invalid datatype for "+key);
			}
			if(this.value_within_range(key,htblColNameValue.get(key))== false){
				throw new DBAppException("out of range value for "+key);
			}
		}
		for(Sparseindex index: this.tables_indices){
			if(index.column_name.equals(this.Clustering_key)) {
			index.update_one_level(strClusteringKeyValue,htblColNameValue);
			return;
			}
		}
		update_using_pages(strClusteringKeyValue,htblColNameValue);
	}
	public void update_using_pages(String key_value,Hashtable<String,Object> htblColNameValue) {
		for(Page p: this.created_Pages){
			// iterate in each page to found the key wanted to update its row
			if(p.update_occurred(key_value,htblColNameValue)==true){
//			     the_row = p.getting_key_row(this, key_value); 
//				update_row(p,htblColNameValue,the_row);
				break;
			}
		}
	}
//	public void update_row(Page p,Hashtable<String,Object> htblColNameValue,int update_row) {
//		try{BufferedReader br = new BufferedReader(new FileReader("D:\\DB_project\\"+p.page_name));
//		String line = "";
//		while((line = br.readLine())!=null){
//			String[]data = line.split(",");
//			if(rows_read==update_row){
//				
//			}
//			}
//			
//		}catch(IOException e) {
//			e.printStackTrace();
//		}
//	} 
	public int col_location(String col_name) throws IOException {
		int location=-1;
		for(int i = 0;i<this.available_attributes.size();i++) {
			String name = this.available_attributes.get(i);
			if(name.equals(col_name))
				location = i;
		}
		return location;
	}
	public void deleting_table(String strTableName,
			Hashtable<String,Object> htblColNameValue) throws DBAppException {
	//	ArrayList<Object> tuple = new ArrayList<Object>();
	 ArrayList<String> col_has_index = new ArrayList<String>();
		ArrayList <String> keys = Collections.list(htblColNameValue.keys()); // ArrayList of keys 
		if(this.created_Pages.size()==0) {
			// no rows to be deleted from this table
			throw new DBAppException(this.name + " table contains no rows to be deleted");
		}
		for(int i = keys.size()-1;i>=0;i--) {
			String key = keys.get(i);
			if(!(this.available_attributes.contains(key))) {
				throw new DBAppException(this.name + " table does not contain a column called "+key);
			}
//			if(this.key_duplicate_checking(key,htblColNameValue.get(key))==false) {
//				throw new DBAppException("value of "+key+ " does not exist");
//			}
			if(this.datatype_checking(key,htblColNameValue.get(key)) == false){
				throw new DBAppException("An Invalid datatype for "+key);
			}
			if(this.value_within_range(key,htblColNameValue.get(key))== false){
				throw new DBAppException("out of range value for "+key);	
	}		
		for(Sparseindex index: this.tables_indices){
			if(index.column_name.equals(key)) {
			index.deleting_rows(htblColNameValue.get(key));
			// removing already deleted col using index from hashtable
			htblColNameValue.remove(key);
			}
		}
		}
		this.pages_iteration_deletion(htblColNameValue);
	}
	public void pages_iteration_deletion(Hashtable<String,Object> htblColNameValue) throws DBAppException{
		ArrayList <String> keys = Collections.list(htblColNameValue.keys());
		for(int i = keys.size()-1;i>=0;i--){
			String key = keys.get(i);
			// loop  through all hash table values
			// Array List of primary keys for each key in the hash table
			// for each key, loop through pages to check if value exist in the page.
			// if value exist in the page, extract the primary key and add it to primary values arrayList
			//of the row in the iterated page 
			ArrayList<Object>primary_values = new ArrayList<Object>();
			for(Page p: this.created_Pages){
				// adding pk values of the rows that fits with key value of hash table
				primary_values = p.get_pk_values(htblColNameValue,key,primary_values);
			}
			// deleting rows using primary values array list
			if (primary_values.size()==0){
				continue;
			}else {
			for(int j = 0;j<this.created_Pages.size();j++){
				// filter array list if page contains values equal to primary values
				Page p = this.created_Pages.get(j);
				primary_values = p.deleting_rows(primary_values);
			//	System.out.println(primary_values.size());
				// if all array list has no more values
				if(primary_values.isEmpty()){
					// primary values containing the key value of hash table is empty
					// meaning hash table value does not exist in any other row
					break;
			}
		}
			// iterate in each page to found the key wanted to update its row
	}
	}
		
		this.deleting_empty_pages();
	}
	public void deleting_empty_pages() {
		for(int i = 0;i<this.created_Pages.size();i++) {
			Page p  = this.created_Pages.get(i);
			File file = new File("src/resources/"+p.page_name);
			// condition for empty pages to be deleted 
			if(p.rows_used==0) {
				this.created_Pages.remove(i);
				file.delete();
				i--;
			}
		}
	}
	public ArrayList<String[]> selecting_from_table(SQLTerm sqlterm, ArrayList<String[]> a) throws DBAppException{
		boolean index_found = false;
		if(!(this.available_attributes.contains(sqlterm._strColumnName))) {
			throw new DBAppException(this.name + " table does not contain a column called "+sqlterm._strColumnName);
		}
		if(this.created_Pages.size()==0) {
			// no rows to be deleted from this table
			throw new DBAppException(this.name + " table contains no rows to be deleted");
		}
		for(Sparseindex index: this.tables_indices){
			if(index.column_name.equals(sqlterm._strColumnName)){
			a  = index.selecting_from_index(sqlterm,a);
			index_found = true;
			}
	}
		if(index_found==true) {
			return a;
		}else {
		for(Page p: this.created_Pages){
	      a =  p.selecting_from_pages(sqlterm,a);
	      if(a.size()==1 && this.Clustering_key.equals(sqlterm._strColumnName)) {
	    	  return a;
	      }
	}
		}
		return a;
	}
	
	// Used to get the primary key name of the table using table name  
//		public String fetch_pk_name(String table_name) {
//			try {
//			BufferedReader br = new BufferedReader(new FileReader("D:\\DB_project\\metadata.csv"));
//			String line = "";
//			String name;
//			while(true) {
//				line = br.readLine();
//				if(line == null) break;
//				String[]data = line.split(",");
//				if(data[0].equals(table_name) && data[3].equals("True")) {
//						// data[1] = col_name
//						name = data[1];
//						br.close();
//						return name;
//					}
//				}
//			br.close();
//			}catch(IOException e) {
//				e.printStackTrace();
//			}
//			return null;
//		}
		public boolean datatype_checking(String col_name,Object input_value)  {
			try{BufferedReader br = new BufferedReader(new FileReader("src/resources/metadata.csv"));
			String line = "";
			String datatype;
			while((line = br.readLine())!=null){
				String[]data = line.split(",");
				if(data[0].equals(this.name) && data[1].equals(col_name)){
					datatype = data[2];
					switch(datatype){
					case"java.lang.Integer":
						br.close();
						return input_value instanceof Integer;					
					case"java.lang.String":
						br.close();
						return input_value instanceof String; 
					case "java.lang.Double":
						br.close();
						return input_value instanceof Double;
					case"java.util.Date":
						br.close();
						return input_value instanceof Date;
					}
				}
		}
			br.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
			return false;
		}
		public boolean value_within_range(String col_name,Object input_value) {
			try{BufferedReader br = new BufferedReader(new FileReader("src/resources/metadata.csv"));
			String line = "";
			String datatype;
			while((line = br.readLine())!=null){
				String[]data = line.split(",");
				// skipping the header row
				if(data[0].equals("Table Name")) {
					continue;
				}
				else if(data[0].equals(this.name) && data[1].equals(col_name)){
					datatype = data[2];
					if(datatype.equals("java.lang.Integer")){
						int min_int = Integer.parseInt(data[6]);
						int max_int = Integer.parseInt(data[7]);
						Integer value_int = (Integer)input_value;
						if(value_int >= min_int &&  value_int<=max_int){
							br.close();
							return true;
						}else {
							break;
						}
					}
					else if(datatype.equals("java.lang.String")){ 
						String min_str =  data[6];
					 	String max_str = data[7];
					 	String value_str = String.valueOf(input_value);
					 	if(value_str.compareToIgnoreCase(min_str)>=0  && value_str.compareToIgnoreCase(max_str)<=0){
					 		br.close();
					 		return true;
					 	}else {
					 		break ;
					 	}
					}
					else if(datatype.equals("java.lang.Double")) {
						Double min_double = Double.parseDouble(data[6]);
						Double max_double = Double.parseDouble(data[7]);
						Double value_double = Double.parseDouble(String.valueOf(input_value));
						if(value_double.compareTo(min_double)>=0 && value_double.compareTo(max_double) <= 0) {
							br.close();
							return true;
						}else {
							break;
						}
					}
					else if (datatype.equals("java.util.Date")) {
						
						Date min_date;
						Date max_date;
						//Date value_date;
						try {
							min_date = new SimpleDateFormat("YYYY-MM-DD").parse(data[6]);
							 max_date =  new SimpleDateFormat("YYYY-MM-DD").parse(data[7]);
						//	 value_date = new SimpleDateFormat("YYYY-MM-DD").parse(input_value.toString());
							 if(((Date)input_value).compareTo(min_date)>=0 && ((Date)input_value).compareTo(max_date)<=0) {
									br.close();
									return true;
							 }else {
								 break;
							 }
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
		}
		// every row entered must check if table page reached its capacity or not
		}br.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
			return false;
		}
		public String get_datatype(String col_name) throws IOException{
			String data_type = "";
		    BufferedReader br = new BufferedReader(new FileReader("src/resources/metadata.csv"));
			String line = "";
			while((line = br.readLine())!=null){
				String[]data = line.split(",");
				if(data[0].equals(this.name) && data[1].equals(col_name)) {
					// compare input table name with table names in the csv file
					// and show if the row contains the primary key name to
					// to get right col type
					data_type = data[2];// col_type name
				    break;
				}
			}    
				br.close();
            
			return data_type;
		}
		public boolean key_duplicate_checking(String col,Object o){
			try{
				for(Page p: this.created_Pages) {
					if(duplicates_exist(p,col, o)==true) 
						return true;
				}
			}catch(IOException e) {
				e.printStackTrace();
			}
			return false;
		}
		public boolean duplicates_exist(Page p,String col,Object o) throws IOException{
		int col_location = this.col_location(col);
		String pk_datatype = this.get_datatype(col);
		// getting the key_attribute location in the metadata
			 BufferedReader br = new BufferedReader(new FileReader("src/resources/"+p.page_name));
				String line = "";
				while((line = br.readLine())!=null){
					String[]data = line.split(",");
					// will not be reached until object is an integer
					if (pk_datatype.equals("java.lang.Integer")){
						Integer key_int = Integer.parseInt(data[col_location]);
						Integer value_int;
						if(o instanceof String) {
						      value_int = Integer.parseInt((String)o);
						}else {
						 value_int = (Integer)o;
						}
						if(value_int == key_int){
							br.close();
			         		return true;
						}
					}
					else if(pk_datatype.equals("java.lang.String")) {
						String key_str = data[col_location];
					 	String value_str = String.valueOf(o);
					 	if(key_str.compareTo(value_str)==0){
					 		br.close();
					 		return true;
					 	}
					}
					else if (pk_datatype.equals("java.lang.Double")) {
						Double key_double = Double.parseDouble(data[col_location]);
						Double value_double;
						if(o instanceof String) {
							value_double = Double.parseDouble((String)o);
						}else {
						value_double = (Double)o;
						}
						if(key_double.compareTo(value_double)==0) {
							br.close();
							return true;
						}
					}
		}
				br.close();
				return false;
		}
//		public int obj_to_int() {
//			
//		}
//		public String obj_to_str() {
//			
//		}
//		public Double obj_to_double() {
//			
//		}
}
