package engine;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.ListIterator;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.*;

import engine.Table_information;
public class Page{
	int rows_used;
	String page_name;
	boolean capacity_reached;// when page is created ,there is no row used
	Table table;
	
//	ArrayList<Object> max_row = new ArrayList<Object>();
//	ArrayList<Object> min_row = new ArrayList<Object>();
	// Page_information info = new Page_information();
//	Table t ;// Table the page is made for storing tuples
	public Page(Table t){
		// the new Page object created 
		this.rows_used = 0;
		this.capacity_reached = false;
		this.table = t;
}
	
	public void addrow(ArrayList<Object> tuple){
		// for adding new row in page's csv file 
		// checking if row is added, page capacity is reached or not 
		// if yes calling an method of creating a page
		// checks if last paged table 
		// if we insert a row in a table and then insert into another table and then came back to insert in the first table
		if (this.table.created_Pages.isEmpty()){
			// no pages created before for this table
			// if used rows of pages less than page capacity
			// No Page is formed for table OR current page capacity of the table is full
			//current_page = new Page();
			this.create_new_page(tuple);
		}
		// when rows reaches the capacity size of the page, create 
		else if(this.capacity_reached==true){
			Page next_page = new Page(table);
			next_page.create_new_page(tuple);
			// next page will be the current page in the method create_new_page
		}
			// if used rows of pages less than page capacity
			// No Page is formed for table OR current page capacity of the table is full
		else{
		this.rows_used++;
		if(this.rows_used == DBApp.MaximumRowsCountinTablePage) {
			this.capacity_reached = true;	
		}
//		if(rows_used>page_capacity) {
//		capacity_reached = true;
//		create_new_page(table_name,tuple);
//		}else {
		
		// thispage_name = t.name + t.page_number+".csv";
		try {
		FileWriter filewriter = new FileWriter("src/resources/"+ this.page_name,true);
		//BufferedWriter bw = new BufferedWriter(filewriter);
		for(int i = 0;i<tuple.size();i++){
			Object value = tuple.get(i);
			if(value instanceof Date) {
				  SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			        String formattedDate = dateFormat.format(value);
			        filewriter.write(formattedDate);
			}else {
			filewriter.write(value.toString());
			}
			if(i!=tuple.size()-1)
			filewriter.append(","); 
		}
		filewriter.append("\n");
		filewriter.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	}
	public void create_new_page(ArrayList<Object> tuple){
		// set row
					// checking if this is the first time the table is paged 
					// in order to set page number to 1 
					// what if it is the maximum row in the page and then 
				// when page is created, no rows is used yet
		// checking if this is the first time the table is paged 
				// in order to set page number to 1 
				// what if it is the maximum row in the page and then 
			// rest page row counter	
			// increment page name
		 Table.total_no_pages++;
		 table.page_number++;
		 this.page_name = table.name + table.page_number + ".csv";
	
			try { 
			FileWriter filewriter = new FileWriter("src/resources/"+this.page_name);
//				BufferedWriter bw = new BufferedWriter(filewriter);
			for(int i = 0;i<tuple.size();i++){
				Object value = tuple.get(i);
				if(value instanceof Date) {
					  SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				        String formattedDate = dateFormat.format(value);
				        filewriter.write(formattedDate);
				}else {
				filewriter.write(String.valueOf(value));
				}
				// condition for not writing comma after last value in a row
				if(i!=tuple.size()-1)
				filewriter.append(",");
			}
			filewriter.append("\n");
			filewriter.close();
			//rows_used++;
			// file.csv is created only when the first page to the first table is created 
//			if(t.page_number ==1){
//				info = new Page_information();
//			}
			rows_used++;
			if(this.rows_used==DBApp.MaximumRowsCountinTablePage) {
				this.capacity_reached = true;
			}
			// adding recent created pages to arraylist of pages of the table
			// set current page of the table to be the recent created page
			table.current_page = this;
			filewriter.close();
			table.created_Pages.add(this);
			this.writing_page_information();
			}catch(IOException e) {
				e.printStackTrace();
			}
			}
	public boolean update_occurred(String key_value,Hashtable<String,Object>htblColNameValue){
		boolean row_found = false;
		String csvFile = "src/resources/"+this.page_name;
		int location;
		try {
			 List<String[]> lines = new ArrayList<>();
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			String line;
			while((line = br.readLine())!=null){
				String[]data = line.split(",");
				lines.add(data);
				}
			br.close();
			// identify rows to update
			int updating_index = -1;
		     location = table.col_location(table.Clustering_key); // id
			// get location of key column of the table
			for (int i = 0; i < lines.size(); i++) {
	            String[] fields = lines.get(i);
	            if (fields[location].equals(key_value)) { // when primary key of string array equals key_value
	            	// set updating index to be this array of strings
	                updating_index = i;
	                break; // stop searching after first match
		}
			}
			// if row not found in the list of strings 
			if(updating_index==-1) {
				return false;
			}
			String[]row_to_update = lines.get(updating_index);
			ArrayList <String> keys = Collections.list(htblColNameValue.keys());
			for(String key:keys) {
				location = table.col_location(key);
				row_to_update[location]= htblColNameValue.get(key).toString();
			}
			BufferedWriter bw = new BufferedWriter(new FileWriter(csvFile));
	            for (String[] fields : lines) {
	                String line_updated = String.join(",", fields);
	                bw.write(line_updated);
	                bw.newLine();
	            }
	            bw.close();
	            row_found = true;
		}catch(IOException e) {
			e.printStackTrace();
		}
		return row_found;
		// if updating row does not exist in the page, return -1
	}
	public ArrayList<Object> get_pk_values(Hashtable<String,Object>htblColNameValue,String key,ArrayList<Object> values) {
		String csvFile = "src/resources/"+this.page_name;
		try {
			int col_location = table.col_location(key);
			int pk_location = table.col_location(table.Clustering_key);
			String col_datatype = table.get_datatype(key);
			BufferedReader br = new BufferedReader(new FileReader(csvFile)); 
				String line;
				// iterate through every row to check if value of key col equals to key value in the hashtable
				while((line = br.readLine())!=null){
					String[]data = line.split(",");
					if(col_datatype.equals("java.lang.Integer")) {
						if(Integer.parseInt(data[col_location]) == (Integer)htblColNameValue.get(key)) {
							values.add(data[pk_location]);
						}
					}
					else if(col_datatype.equals("java.lang.Double")) {
						if(Double.parseDouble(data[col_location]) == (Double)htblColNameValue.get(key)) {
							values.add(data[pk_location]);
						}
					}
					else if(col_datatype.equals("java.util.Date")){
						try {
						Date date = new SimpleDateFormat("YYYY-MM-DD").parse(data[col_location]);
						if(date.equals((Date)htblColNameValue.get(key))){
							values.add(data[pk_location]);
						}
						}catch(ParseException p) {
							p.printStackTrace();
						}
				}
					else { 
						if((data[col_location]).equals((String)htblColNameValue.get(key))){
						// adding key value of this row to the array list of primary keys  
						values.add(data[pk_location]);
					}
				}
				}
				     br.close();
				}catch(IOException e) {
					e.printStackTrace();
				}
		return values;
	}
	public ArrayList<Object> deleting_rows(ArrayList<Object>primary_values) {
		String csvFile = "src/resources/"+this.page_name;
		File file = new File(csvFile);
		for(int j = 0;j < primary_values.size();j++){
			// iterate through arraylist to check if exists any pk_value in this page
			Object pk_value = primary_values.get(j);
			try {
				int key_location = table.col_location(table.Clustering_key);
					List<String[]> lines = new ArrayList<>();
					BufferedReader br = new BufferedReader(new FileReader(csvFile));
					String line;
					while((line = br.readLine())!=null){
						String[]data = line.split(",");
						lines.add(data);
						}
					br.close();
					int deleting_index = -1;
					for (int i = 0; i < lines.size(); i++) {
			            String[] fields = lines.get(i);
			            if (fields[key_location].equals(pk_value)) { // when primary key of string array equals key_value
			            	// set updating index to be this array of strings
			                deleting_index = i;
			                break; // stop searching after first match
				}
					}
					    if(deleting_index==-1){
					    	// if primary key value not found in the specific page 
			            continue;
					    // get to the next primary key in the array List
					    }
					    else{
					    	 lines.remove(deleting_index);
					         // deleting the row from arraylist of array of strings
							    primary_values.remove(j);
							    // to iterate from the beginning arraylist 
							    j--;
					    	// no rows found equal to key_value in the page
					    // decrement 
					    this.rows_used--;
					    this.capacity_reached = false;
					    if(this.rows_used==0){
					    	// if row removed is the last row in the page 
					    	// delete the page from file.csv and from created_pages arraylist
					    //	t.created_Pages.remove(this);
					    	this.removing_page_information();
					    	file.delete();
					    	break;
					    }else {
			            BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile));
			            for (String[] row : lines) {
			                String line1 = String.join(",", row);
			                writer.write(line1);
			                writer.newLine();
			            }
			            writer.close();     
					    }
					    }
			}catch(IOException e) {
				e.printStackTrace();
			}
			
		}
		return primary_values;
	}
	public String[] get_first_row(){
		String []value = null;
		String csvFile = "src/resources/"+this.page_name;
		try{BufferedReader br = new BufferedReader(new FileReader(csvFile));
		String line;
		int location = table.col_location(table.Clustering_key);
		line = br.readLine();
			String[]data = line.split(",");
			value = data;
			br.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		return value;
	}
	public String[] get_specified_pk_row(String s){
		String[] value = null;
		String csvFile = "src/resources/"+this.page_name;
		try{BufferedReader br = new BufferedReader(new FileReader(csvFile));
		int location = table.col_location(table.Clustering_key);
		String line = "";
		while((line = br.readLine())!=null){
			String[]data = line.split(",");
			if(data[location].equals(s)){
				value = data;
				break;
			}
		}
		br.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
		return value;
	}
	public String get_last_row_pk(){
		Stack<String>stk = new Stack<String>();
		String csvFile = "src/resources/"+this.page_name;
		try{BufferedReader br = new BufferedReader(new FileReader(csvFile));
		int location = table.col_location(table.Clustering_key);
		String line = "";
		while((line = br.readLine())!=null){
			String[]data = line.split(",");
			stk.push(data[location]);
		}
		br.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		return stk.pop();
	}
	public ArrayList<String[]> page_rows_of_column(String col_name){
		ArrayList<String[]> lines = new ArrayList<>();
		String csvFile = "src/resources/"+this.page_name;
		try {
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			String line;
			int row_number=1;
			int location = table.col_location(col_name);
			while((line = br.readLine())!=null){
				String[]data = line.split(",");
				String[]row = {data[location],this.page_name,row_number + ""}; 
				lines.add(row);
				row_number++;
			}
			br.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		return lines;
		}
	public void delete_specific_key(String s){
		String csvFile = "src/resources/"+this.page_name;
		try {
			int location = table.col_location(table.Clustering_key);
			 List<String[]> lines = new ArrayList<>();
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			String line;
			while((line = br.readLine())!=null){
				String[]data = line.split(",");
				lines.add(data);
				}
			br.close();
			for(int i = 0;i<lines.size();i++) {
				String[] s1= lines.get(i);
				if(s.equals(s1[location])) {
					lines.remove(s1);
					break;
				}
			}
			BufferedWriter bw = new BufferedWriter(new FileWriter(csvFile));
            for (String[] fields : lines) {
                String line_updated = String.join(",", fields);
                bw.write(line_updated);
                bw.newLine();
            }
            bw.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	public ArrayList<String[]> delete_specified_row(int row_number,ArrayList<String[]> remain) {
		String csvFile = "src/resources/"+this.page_name;
		File file = new File(csvFile);
		 ArrayList<String[]> lines = new ArrayList<String[]>();
		try {
	//		int location = table.col_location(table.Clustering_key);
			if(remain.size()==0) {
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			String line;
			while((line = br.readLine())!=null){
				String[]data = line.split(",");
				lines.add(data);
				}
			br.close();
			}else {
				lines = remain;
			}
			for(int i = 0;i<lines.size();i++){
				if(i == row_number-1) {
				lines.set(i, null);
				}
			}
			 this.rows_used--;
			 this.capacity_reached = false;
			    if(this.rows_used==0){
			    	// if row removed is the last row in the page 
			    	// delete the page from file.csv and from created_pages arraylist
			    //	t.created_Pages.remove(this);
			    	this.removing_page_information();
			    	file.delete();
	}
//			    BufferedWriter bw = new BufferedWriter(new FileWriter(csvFile));
//	            for (String[] fields : lines) {
//	            	if(fields==null) {
//	            		continue;
//	            	}
//	                String line_updated = String.join(",", fields);
//	                bw.write(line_updated);
//	                bw.newLine();
//	            }
//	            bw.close();
         }catch(IOException e) {
		e.printStackTrace();
	}
		 return lines;
		
	}
	public void write_to_page(ArrayList<String[]> lines) {
		String csvFile = "src/resources/"+this.page_name;
		try { BufferedWriter bw = new BufferedWriter(new FileWriter(csvFile));
         for (String[] fields : lines) {
         	if(fields==null) {
         		continue;
         	}
             String line_updated = String.join(",", fields);
             bw.write(line_updated);
             bw.newLine();
         }
         bw.close();
  }catch(IOException e) {
	e.printStackTrace();
}
	}
	public ArrayList<String[]> selecting_from_pages(SQLTerm sqlterm,ArrayList<String[]> result){
		ArrayList<String[]> lines = new ArrayList<String[]>();
		String csvFile = "src/resources/"+ this.page_name; 
		try{
			String datatype = table.get_datatype(sqlterm._strColumnName);
			int location = table.col_location(sqlterm._strColumnName);
			BufferedReader br = new BufferedReader(new FileReader(csvFile));
			String line;
		while((line = br.readLine())!=null){
			String[]data = line.split(",");
			lines.add(data);
			}
		for(int i = 0;i<lines.size();i++){
			String[] s1 = lines.get(i);
			switch(sqlterm._strOperator){
			case"=":
				if(datatype.equals("java.lang.Integer")) {
					Integer value = Integer.parseInt(s1[location]);
					if(value.compareTo((Integer)sqlterm._objValue)==0){
						if(sqlterm._strColumnName.equals(table.Clustering_key)) {
							result.add(s1);
							return result;
						}else {
							result.add(s1);
						}
					}
				}
				else if(datatype.equals("java.lang.String")) {
					if(s1[location].compareTo((String)sqlterm._objValue)==0){
						if(sqlterm._strColumnName.equals(table.Clustering_key)) {
							result.add(s1);
							return result;
						}else {
							result.add(s1);
						}
					}
				}
				else if(datatype.equals("java.lang.Double")) {
					Double value = Double.parseDouble(s1[location]);
					if(value.compareTo((Double)sqlterm._objValue)==0){
						if(sqlterm._strColumnName.equals(table.Clustering_key)) {
							result.add(s1);
							return result;
						}else {
							result.add(s1);
						}
					}
				}
//				else if(datatype.equals("java.util.Date")) {
//					Date value = Date.parse(s1[location]);
//					if(value.compareTo((Double)sqlterm.objValue)==0){
//						if(sqlterm.strColumnName.equals(table.Clustering_key)) {
//							result.add(s1);
//							return result;
//						}else {
//							result.add(s1);
//						}
//					}
//				}
				break;
			case">=":
				if(datatype.equals("java.lang.Integer")) {
					Integer value = Integer.parseInt(s1[location]);
					if(value.compareTo((Integer)sqlterm._objValue)>=0){
							result.add(s1);
						}
					}
				else if(datatype.equals("java.lang.String")) {
					if(s1[location].compareTo((String)sqlterm._objValue)>=0){
							result.add(s1);
						}
					}
				else if(datatype.equals("java.lang.Double")) {
					Double value = Double.parseDouble(s1[location]);
					if(value.compareTo((Double)sqlterm._objValue)>=0){
							result.add(s1);
					}
				}
//				else if(datatype.equals("java.util.Date")) {
//					Date value = Date.parse(s1[location]);
//					if(value.compareTo((Double)sqlterm.objValue)==0){
//						if(sqlterm.strColumnName.equals(table.Clustering_key)) {
//							result.add(s1);
//							return result;
//						}else {
//							result.add(s1);
//						}
//					}
//				}
				break;
			case"<":
				if(datatype.equals("java.lang.Integer")) {
					Integer value = Integer.parseInt(s1[location]);
					if(value.compareTo((Integer)sqlterm._objValue)<0){
							result.add(s1);
						}
					}
				else if(datatype.equals("java.lang.String")) {
					if(s1[location].compareTo((String)sqlterm._objValue)<0){
							result.add(s1);
						}
					}
				else if(datatype.equals("java.lang.Double")) {
					Double value = Double.parseDouble(s1[location]);
					if(value.compareTo((Double)sqlterm._objValue)<0){
							result.add(s1);
					}
				}
//				else if(datatype.equals("java.util.Date")) {
//					Date value = Date.parse(s1[location]);
//					if(value.compareTo((Double)sqlterm.objValue)==0){
//						if(sqlterm.strColumnName.equals(table.Clustering_key)) {
//							result.add(s1);
//							return result;
//						}else {
//							result.add(s1);
//						}
//					}
//				}
				break;
			case"<=":
				if(datatype.equals("java.lang.Integer")) {
					Integer value = Integer.parseInt(s1[location]);
					if(value.compareTo((Integer)sqlterm._objValue)<=0){
							result.add(s1);
						}
					}
				else if(datatype.equals("java.lang.String")) {
					if(s1[location].compareTo((String)sqlterm._objValue)<=0){
							result.add(s1);
						}
					}
				else if(datatype.equals("java.lang.Double")) {
					Double value = Double.parseDouble(s1[location]);
					if(value.compareTo((Double)sqlterm._objValue)<=0){
							result.add(s1);
					}
				}
//				else if(datatype.equals("java.util.Date")) {
//					Date value = Date.parse(s1[location]);
//					if(value.compareTo((Double)sqlterm.objValue)==0){
//						if(sqlterm.strColumnName.equals(table.Clustering_key)) {
//							result.add(s1);
//							return result;
//						}else {
//							result.add(s1);
//						}
//					}
//				}
				break;
			case">":
				if(datatype.equals("java.lang.Integer")) {
					Integer value = Integer.parseInt(s1[location]);
					if(value.compareTo((Integer)sqlterm._objValue)>0){
							result.add(s1);
						}
					}
				else if(datatype.equals("java.lang.String")) {
					if(s1[location].compareTo((String)sqlterm._objValue)>0){
							result.add(s1);
						}
					}
				else if(datatype.equals("java.lang.Double")) {
					Double value = Double.parseDouble(s1[location]);
					if(value.compareTo((Double)sqlterm._objValue)>0){
							result.add(s1);
					}
				}
//				else if(datatype.equals("java.util.Date")) {
//					Date value = Date.parse(s1[location]);
//					if(value.compareTo((Double)sqlterm.objValue)==0){
//						if(sqlterm.strColumnName.equals(table.Clustering_key)) {
//							result.add(s1);
//							return result;
//						}else {
//							result.add(s1);
//						}
//					}
//				}
				break;
			}
			}
		br.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	
//		return this.sort_page(lines,col_name);
		
//	public List<String> sort_rows(List<String[]> s,String col){
//		List<String> obj = new ArrayList<>();
//		for(String[] i:s){
//			obj.add(i[0]);
//		}
//		Collections.sort(obj);
//		return obj;
//	}
//		String datatype = "";
//		try {
//		 datatype = table.get_datatype(col);
//	}catch(IOException e) {
//		e.printStackTrace();
//		for(int i = 0;i<s.size();i++){
//			String[]row = s.get(i);
//			Collections.sort(s,);
//			switch(datatype) {
//			case"java.lang.Integer":
//				break;
//			case"java.lang.String":
//				break;
//			case"java.lang.Double":
//				break;
//			case"java.util.Date":
//				break;
//			}
//		}
//		return s;
//	}
	public void writing_page_information(){
		try{FileWriter filewriter = new FileWriter("src/resources/files.csv",true);
		filewriter.write(table.name);
		filewriter.append(",");
		filewriter.write("Table");
		filewriter.append(",");
		filewriter.write(this.page_name);
		filewriter.append(",");
		filewriter.write("src/resources/");
		filewriter.write("\n");
		filewriter.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	public void removing_page_information(){
		try{
			int removing_row = 0;
			List<String[]> rows = new ArrayList<>();
			BufferedReader br = new BufferedReader(new FileReader("src/resources/files.csv"));
			String line;
			while((line = br.readLine())!=null){
				String[]data = line.split(",");
				rows.add(data);
		}
			br.close();
			for(int i = 0;i<rows.size();i++){
				String[]row = rows.get(i);
				if(row[2].equals(this.page_name)) {
					removing_row = i;
					break;
				}
			}
			 BufferedWriter writer = new BufferedWriter(new FileWriter("src/resources/files.csv"));
			rows.remove(removing_row);
			 for (String[] row : rows) {
	                String line1 = String.join(",", row);
	                writer.write(line1);
	                writer.newLine();
	            }
	            writer.close();     
	}catch(IOException e) {
		e.printStackTrace();
	}
	}
	public static void write_pages_file(){
		try {
			FileWriter writer = new FileWriter("src/resources/files.csv"); 
		    writer.append("Name,Type,FileName,FileLocationonHarddisk");
		    writer.append("\n");
		    writer.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	
	public void shifting_rows(String[]shifting_row,int page_location){
//		if(row_reached == true) {
//			row_reached = false;
//		}
		// iterator over array list of pages to show which page the new value
		// will be inserted into
		try { 
			    String csvFile = "src/resources/"+this.page_name;
				BufferedReader br = new BufferedReader(new FileReader(csvFile));
				String line = "";
				ArrayList<String[]> lines = new ArrayList<String[]>();
				// adding shifting row to the beginning of the array list
				lines.add(shifting_row);
				while((line = br.readLine())!=null){
					String[]data = line.split(",");
					lines.add(data);
				}
				br.close();
				this.rows_used++;
				if(this.capacity_reached==true){
					// condition met when capacity reached
					// remove the out of bound row from array list
					String[]out_of_capacity_row;
					out_of_capacity_row = lines.remove(lines.size()-1);
					this.rows_used--;
				if(page_location!=table.created_Pages.size()-1) {
		            page_location++;
		            table.current_page = table.created_Pages.get(page_location); 
					table.current_page.shifting_rows(out_of_capacity_row,page_location);
				}else if(page_location==table.created_Pages.size()-1){
					table.current_page = new Page(table);
					ArrayList<Object> outrow = new ArrayList<Object>();
					for(String value:out_of_capacity_row){
						outrow.add(value);
					}
					table.current_page.create_new_page(outrow);
				}
				BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile));
	            for (String[] row : lines) {
	                String line1 = String.join(",", row);
	                writer.write(line1);
	                writer.newLine();
	            }
	            writer.close();
				}else{
					BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile));
		            for (String[] row : lines) {
		                String line1 = String.join(",", row);
		                writer.write(line1);
		                writer.newLine();
		            }
		            writer.close();
		            if(rows_used==DBApp.MaximumRowsCountinTablePage) {
		            	this.capacity_reached=true;
		            }else {
		            	return;
		            }
				}
				}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public void sorted_insertion(ArrayList<Object> input_row,int page_location){
		boolean row_reached = false;
		// will be compared to rows_used of this page 
		// turns to true when value in file is bigger than input value 
		// array list containing every column after bigger than 
		String csvFile = "src/resources/" +this.page_name;
		try { 
			int key_loc = table.col_location(table.Clustering_key);
			String key_type = table.get_datatype(table.Clustering_key);
				List<String[]> lines = new ArrayList<>();
				List<String[]> temp = new ArrayList<>();
				BufferedReader br = new BufferedReader(new FileReader(csvFile));
				String line;
				while((line = br.readLine())!=null){
					String[]data = line.split(",");
					lines.add(data);
				}
				br.close();
			//	int size = lines.size();
				for(int i = 0;i<lines.size();i++){
					String[]row = lines.get(i);
					if(row_reached==true){
						// storing shifting rows in a temp arraylist
						temp.add(row);
						lines.remove(row);
						i--;
					}else {
				switch(key_type){
				// checks the datatype of the primary key field
					case"java.lang.Integer":
						int int_value_1 = (Integer)input_row.get(key_loc);
					    int int_value_2 = Integer.parseInt(row[key_loc]);
						if(int_value_1 < int_value_2){
							row_reached = true;
							temp.add(row);
							lines.remove(row);
							i--;
							break;
							// comparing two primary key values
							// if value in the file bigger than input_value
							// shift file row , first adding file row to the array list shifting row
						}else{
							continue;
						}
					case"java.lang.String":
						String string_value = (String)input_row.get(key_loc);
						if(string_value.compareToIgnoreCase(row[key_loc])<0){
							row_reached = true;
							temp.add(row);
							lines.remove(row);
							i--;
							break;
						}else {
							continue;
						}
					case"java.util.Date":
						try {
						Date date_value1 = (Date)input_row.get(key_loc);
						Date date_value2 = new SimpleDateFormat("YYYY-MM-DD").parse(row[key_loc]);
						if(date_value1.compareTo(date_value2)<0){
							row_reached = true;
							temp.add(row);
							lines.remove(row);
							i--;
							break;
						}else {
							continue;
						}
						}catch(ParseException e) {
							e.printStackTrace();
						}
					case"java.lang.Double":
						Double double_value1 = (Double) input_row.get(key_loc);
						Double double_value2 = Double.parseDouble(row[key_loc]);
						if(double_value1.compareTo(double_value2)<0){
							row_reached = true;
							temp.add(row);
							lines.remove(row);
							i--;
							break;
						}else {
							continue;
						}
						}
				}
				}
				if(row_reached==false) {
					if(page_location!=table.created_Pages.size()-1){ // when we are not in the last page and 
						//want to go to another page to compare primary keys
						page_location++;
						table.current_page = table.created_Pages.get(page_location);
						table.current_page.sorted_insertion(input_row, page_location);
					}else {
						this.addrow(input_row);
					}
				}
				// adding rows back after shift
				else {
						StringBuffer sb = new StringBuffer();
						for(Object o:input_row) {
						sb.append(o);
						sb.append(",");
					}
						String str = sb.toString();
						String[]input = str.split(",");
						lines.add(input);
				for(String[]value:temp){
					lines.add(value);
				}
				this.rows_used++;
				if(this.capacity_reached==true){
					// condition met when capacity reached
					// remove the out of bound row from array list
					String[]out_of_capacity_row;
					out_of_capacity_row = lines.remove(lines.size()-1);
					this.rows_used--;
				if(page_location<table.created_Pages.size()-1){
					page_location++;
		            table.current_page = table.created_Pages.get(page_location);
					table.current_page.shifting_rows(out_of_capacity_row,page_location);
				}else if(page_location==table.created_Pages.size()-1) {
					table.current_page = new Page(table);
					ArrayList<Object> outrow = new ArrayList<Object>();
					for(String value:out_of_capacity_row) {
						outrow.add(value);
					}
					table.current_page.create_new_page(outrow);
				}
				BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile));
	            for (String[] row : lines) {
	                String line1 = String.join(",", row);
	                writer.write(line1);
	                writer.newLine();
	            }
	            writer.close();
				}else if(rows_used<=DBApp.MaximumRowsCountinTablePage){
					BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile));
		            for (String[] row : lines) {
		                String line1 = String.join(",", row);
		                writer.write(line1);
		                writer.newLine();
		            }
		            writer.close();
		            if(rows_used==DBApp.MaximumRowsCountinTablePage) {
		            	this.capacity_reached = true;
		            }
				}
				}
	}catch(IOException e){
		e.printStackTrace();
	}
	}

		}
		// for sorting csv lines ArrayList of ArrayList of String would be used
		// Each entry is one line, list of strings
//		To sort you write a custom Comparator. 
//		In the Constructor of that comparator you can pass the 
//		field position used to sort
//		ArrayList<ArrayList<String>> csvLines = new ArrayList<ArrayList<String>>();
//		Comparator<ArrayList<String>> comp = new Comparator<ArrayList<String>>() {
//
//			@Override
//			public int compare(ArrayList<String> csvline1, ArrayList<String> csvline2) {
//				// TODO Auto-generated method stub
//				return 0;
//			}
//		}
//		}
//		
//	}
	
	// recursive method to shift every row one row down in the csv files
//	public void shifting_rows_after_sorting(Table t,ArrayList<Object> a1,ArrayList<Object>a2){
//		
//	}
//	public int min_row(){
//		return 0;
//	}
//	public int max_row(){
//		int rows_read;
//		
//		return 0;
//	}
	
	
	

