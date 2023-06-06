package engine;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Comparator;
import java.util.Hashtable;

public class Sparseindex{
	// no paging for sparse index
	String index_name;
	static ArrayList<Sparseindex> created_indices = new ArrayList<Sparseindex>();
	String column_name;
	Table table;
	Indexlevel level;
	boolean isEmpty;
	boolean isEmpty_for_first;
	boolean isEmpty_for_second;
	public Sparseindex(Table t,String col_name){
		this.table = t;
		this.column_name = col_name;
		if(this.column_name.equals(table.Clustering_key)){
			this.level = Indexlevel.ONE;
			isEmpty = true;
		}else {
			this.level = Indexlevel.TWO;
			isEmpty_for_first = true;
			isEmpty_for_second = true;
		}
	}
	public void create_index() {
		// method is used to seperate between one level index for primary key col
		// and second level index for other columns
		// fetch the name vod
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/resources/metadata.csv"));
				 List<String[]> lines = new ArrayList<>();
				String line;
				while((line = br.readLine())!=null){
					String[]data = line.split(",");
					lines.add(data);
					}
				br.close();
				for(int i = 0;i<lines.size();i++) {
					String[]row = lines.get(i);
					if(row[0].equals(this.table.name) && row[1].equals(this.column_name)){
						this.index_name = this.table.name + this.column_name;
						row[4] = this.index_name +"_" +  "Index" ;
						row[5] = "SparseIndex";
						break;
					}
				}
				BufferedWriter bw = new BufferedWriter(new FileWriter("src/resources/metadata.csv"));
	            for (String[] fields : lines) {
	                String line_updated = String.join(",", fields);
	                bw.write(line_updated);
	                bw.newLine();
	            }
	            bw.close();
				created_indices.add(this);
				table.tables_indices.add(this);
				this.write_to_index();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	public void write_to_index() {
		if(this.level==Indexlevel.ONE) {
			write_one_level();
		}else {
			write_first_level();
		}
	}
	public void insert_to_index(ArrayList<Object> tuples) {
//		if(this.level==Indexlevel.ONE) {
			insert_one_level(tuples);
//		}else {
//			insert_second_level(tuples);
//		}
	}
	public void write_one_level() {
		// for primary key column
		// write in the index csv file the first row in each page of the table
		// by iterating through every page
		String csvFile = this.index_name+".csv";
		if(this.isEmpty==true){
			this.writing_index_information(csvFile);
			this.isEmpty = false;
		}
			try {
				FileWriter filewriter = new FileWriter("src/resources/"+csvFile);
				if(!table.created_Pages.isEmpty()) {
				for(Page p: table.created_Pages){ 
					filewriter.write(p.get_first_row()[table.col_location(this.column_name)]);
					filewriter.append("\n");
				}
				}
				filewriter.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	public void insert_one_level(ArrayList<Object> tuples){
		
		String csvFile = this.index_name+".csv";
		ArrayList<String> pk_values = new ArrayList<String>();
		try {
			String datatype = table.get_datatype(table.Clustering_key);
			int location = table.col_location(table.Clustering_key);
			BufferedReader br = new BufferedReader(new FileReader("src/resources/"+csvFile));
			String line = "";
			while((line = br.readLine())!=null){
				String data = line;
				pk_values.add(data);
				}
			br.close();
			if (pk_values.size()==0) {
				this.write_one_level();
			}
			if(pk_values.size()==1){
				table.current_page = table.created_Pages.get(0);
				table.current_page.sorted_insertion(tuples, 0);
			}
			for(int i = 0;i<pk_values.size()-1 ;i++) {
				String s1 = pk_values.get(i);
				String s2 = pk_values.get(i+1);
				if(datatype.equals("java.lang.String")){
					String v = (String)tuples.get(location);
					if(v.compareTo(s1)>0 && v.compareTo(s2)<0) {
						table.current_page = table.created_Pages.get(i);
						table.current_page.sorted_insertion(tuples, i);
					}else if(v.compareTo(s1)<0 && i==0) {
						table.current_page = table.created_Pages.get(0);
						table.current_page.sorted_insertion(tuples, 0);
					}else if(v.compareTo(s2)>0 && i+1==pk_values.size()-1) {
						table.current_page = table.created_Pages.get(table.created_Pages.size()-1);
						table.current_page.sorted_insertion(tuples, table.created_Pages.size()-1);
					}else {
						continue;
					}
					
				}else if(datatype.equals("java.lang.Integer")){
					Integer v = (Integer)tuples.get(location);
					Integer int_s1 = Integer.parseInt(s1);
					Integer int_s2 = Integer.parseInt(s2);
					if(v.compareTo(int_s1)>0 && v.compareTo(int_s2)<0) {
						table.current_page = table.created_Pages.get(i);
						table.current_page.sorted_insertion(tuples, i);
					}else if(v.compareTo(int_s1)<0 && i==0){
						table.current_page = table.created_Pages.get(0);
						table.current_page.sorted_insertion(tuples, 0);
					}else if(v.compareTo(int_s2)>0 && i+1==pk_values.size()-1) {
						table.current_page = table.created_Pages.get(table.created_Pages.size()-1);
						table.current_page.sorted_insertion(tuples, table.created_Pages.size()-1);
					}else {
						continue;
					}
				}else if(datatype.equals("java.lang.Double")) {
					Double v = (Double)tuples.get(location);
					Double double_s1 = Double.parseDouble(s1);
					
					Double double_s2 = Double.parseDouble(s2);
					if(v.compareTo(double_s1)>0 && v.compareTo(double_s2)<0) {
						table.current_page = table.created_Pages.get(i);
						table.current_page.sorted_insertion(tuples, i);
					}else if(v.compareTo(double_s1)<0 && i==0) {
						table.current_page = table.created_Pages.get(0);
						table.current_page.sorted_insertion(tuples, 0);
					}else if(v.compareTo(double_s2)>0 && i+1==pk_values.size()-1) {
						table.current_page = table.created_Pages.get(table.created_Pages.size()-1);
						table.current_page.sorted_insertion(tuples, table.created_Pages.size()-1);
					}else {
						continue;
					}
					
				}else {
					
				}
			}
		}catch(IOException e) {
			e.printStackTrace();
		}
		this.write_one_level();
	}
public void update_one_level(String key_value,Hashtable<String,Object> htblColNameValue){
		String csvFile = this.index_name+".csv";
		ArrayList<String> pk_values = new ArrayList<String>();
		try {
			String datatype = table.get_datatype(table.Clustering_key);
			BufferedReader br = new BufferedReader(new FileReader("src/resources/"+csvFile));
			String line = "";
			while((line = br.readLine())!=null){
				String data = line;
				pk_values.add(data);
				}
			br.close();
			if(pk_values.size()==1) {
				table.current_page = table.created_Pages.get(0);
				table.current_page.update_occurred(key_value,htblColNameValue);
			}
			for(int i = 0;i<pk_values.size()-1 ;i++) {
				String s1 = pk_values.get(i);
				String s2 = pk_values.get(i+1);
				if(datatype.equals("java.lang.String")){
					String v = key_value;
					if(v.compareTo(s1)>0 && v.compareTo(s2)<0) {
						table.current_page = table.created_Pages.get(i);
						table.current_page.update_occurred(key_value,htblColNameValue);
						;
					}else if(v.compareTo(s1)<0 && i==0) {
						table.current_page = table.created_Pages.get(0);
						table.current_page.update_occurred(key_value,htblColNameValue);
					}else if(v.compareTo(s2)>0 && i+1==pk_values.size()-1) {
						table.current_page = table.created_Pages.get(table.created_Pages.size()-1);
						table.current_page.update_occurred(key_value,htblColNameValue);					
					}else {
						continue;
					}
					
				}else if(datatype.equals("java.lang.Integer")){
					Integer v = Integer.parseInt(key_value);
					Integer int_s1 = Integer.parseInt(s1);
					Integer int_s2 = Integer.parseInt(s2);
					if(v.compareTo(int_s1)>=0 && v.compareTo(int_s2)<0) {
						table.current_page = table.created_Pages.get(i);
						table.current_page.update_occurred(key_value,htblColNameValue);
					}else if(v.compareTo(int_s1)<=0 && i==0){
						table.current_page = table.created_Pages.get(0);
						table.current_page.update_occurred(key_value,htblColNameValue);
					}else if(v.compareTo(int_s2)>=0 && i+1==pk_values.size()-1) {
						table.current_page = table.created_Pages.get(table.created_Pages.size()-1);
						table.current_page.update_occurred(key_value,htblColNameValue);
					}else {
						continue;
					}
				}else if(datatype.equals("java.lang.Double")) {
					Double v = Double.parseDouble(key_value);
					Double double_s1 = Double.parseDouble(s1);
					
					Double double_s2 = Double.parseDouble(s2);
					if(v.compareTo(double_s1)>0 && v.compareTo(double_s2)<0) {
						table.current_page = table.created_Pages.get(i);
						table.current_page.update_occurred(key_value,htblColNameValue);
					}else if(v.compareTo(double_s1)<0 && i==0) {
						table.current_page = table.created_Pages.get(0);
						table.current_page.update_occurred(key_value,htblColNameValue);
					}else if(v.compareTo(double_s2)>0 && i+1==pk_values.size()-1) {
						table.current_page = table.created_Pages.get(table.created_Pages.size()-1);
						table.current_page.update_occurred(key_value,htblColNameValue);
					}else {
						continue;
					}
					
				}else {
					
				}
			}
		}catch(IOException e) {
			e.printStackTrace();
		}
		this.write_one_level();
}
public void deleting_rows(Object o) {
	if(this.level==Indexlevel.ONE) {
		deleting_one_level(o);
	}else {
		deleting_second_level(o);
	}
}
public void deleting_one_level(Object o) {
	
}

public void deleting_first_level(String o,int pos ){
	 String csvFile = this.index_name+"1"+".csv";
	 ArrayList<String[]> lines = new ArrayList<String[]>();
	 ArrayList<String[]> deleted_rows = new ArrayList<String[]>();
	 try {
//		 String datatype = table.get_datatype(this.column_name);
			BufferedReader br = new BufferedReader(new FileReader("src/resources/"+csvFile));
			String line = "";
			while((line = br.readLine())!=null){
				String[] data = line.split(",");
				lines.add(data);
				}
			br.close();
			for(int i = pos;i<lines.size();i++) {
				String[] a = lines.get(i);
				while(a[0].equals(o)){
					for(Page p: table.created_Pages){
						a = lines.get(i);
						ArrayList<String[]> remaining_array = new ArrayList<String[]>();
						while(p.page_name.equals(a[1]) && a[0].equals(o)){
						remaining_array = p.delete_specified_row(Integer.parseInt(a[2]),remaining_array);
						deleted_rows.add(a);
						p.write_to_page(remaining_array);
						i++;
						if(i==lines.size()){
							break;
						}
						a = lines.get(i);
						}
						
						if(p.rows_used==0) {
							continue;
						}
						
					}
//					this.write_first_level();
					i++;
					if(i>=lines.size())
						break ;
					a = lines.get(i);
				}
			}
			table.deleting_empty_pages();
//		this.write_first_level();
			update_first_level_after_deletion(deleted_rows);
			update_second_level_after_deletion(deleted_rows);
	 }catch(IOException e){
		 e.printStackTrace();
	 }
	
}
public void update_first_level_after_deletion(ArrayList<String[]> a){
	String csvFile = this.index_name+"1"+".csv";
	try{BufferedReader br = new BufferedReader(new FileReader("src/resources/"+csvFile));
	String line = "";
	ArrayList<String[]> lines = new ArrayList<String[]>() ;
	while((line = br.readLine())!=null){
		String[] data = line.split(",");
		lines.add(data);
	}
	br.close();
	for(int i = 0;i<a.size();i++){
		String[] s1 = a.get(i);
		for(int j = 0;j<lines.size();j++) {
		String[] s2 = lines.get(j);
		if(s1[0].equals(s1[0])) {
			lines.remove(j);
			j--;
		}
		}
	}
	BufferedWriter bw = new BufferedWriter(new FileWriter("src/resources/"+csvFile));
    for (String[] fields : lines) {
        String line_updated = String.join(",", fields);
        bw.write(line_updated);
        bw.newLine();
    }
}catch(IOException e) {
	e.printStackTrace();
}
}
public void update_second_level_after_deletion(ArrayList<String[]>a){
	String csvFile = this.index_name+"2"+".csv";
	try{BufferedReader br = new BufferedReader(new FileReader("src/resources/"+csvFile));
	String line = "";
	ArrayList<String[]> lines = new ArrayList<String[]>() ;
	while((line = br.readLine())!=null){
		String[] data = line.split(",");
		lines.add(data);
	}
	br.close();
	for(int i = 0;i<a.size();i++){
		String[] s1 = a.get(i);
		for(int j = 0;j<lines.size();j++) {
		String[] s2 = lines.get(j);
		if(s1[0].equals(s1[0])) {
			lines.remove(j);
			j--;
		}
		}
	}
	BufferedWriter bw = new BufferedWriter(new FileWriter("src/resources/"+csvFile));
    for (String[] fields : lines) {
        String line_updated = String.join(",", fields);
        bw.write(line_updated);
        bw.newLine();
    }
}catch(IOException e) {
	e.printStackTrace();
}
}
public void deleting_second_level(Object o){
    this.write_first_level();
	String csvFile = this.index_name+"2"+".csv";
	ArrayList<String[]> lines = new ArrayList<String[]>();
	try {
		String datatype = table.get_datatype(this.column_name);
		BufferedReader br = new BufferedReader(new FileReader("src/resources/"+csvFile));
		String line = "";
		while((line = br.readLine())!=null){
			String[] data = line.split(",");
			lines.add(data);
			}
		br.close();
		if(lines.size()==1) {
			this.deleting_first_level(o.toString(),0);
		}else {
		for(int i = 0;i<lines.size()-1 ;i++) {
			String[] a1 = lines.get(i);
			String[] a2 = lines.get(i+1);
			String s1 = a1[0];
			String s2 = a2[0];
			if(datatype.equals("java.lang.String")){
				String v = (String)o;
				if(v.compareTo(s1)>=0 && v.compareTo(s2)<0) {
					this.deleting_first_level(o.toString(),i);
				}else if(v.compareTo(s2)>=0 && i+1==lines.size()-1) {
					this.deleting_first_level(o.toString(),i+1);					
				}else{
					continue;
				}
				
			}else if(datatype.equals("java.lang.Integer")){
				Integer v = (Integer)o;
				Integer int_s1 = Integer.parseInt(s1);
				Integer int_s2 = Integer.parseInt(s2);
				if(v.compareTo(int_s1)>=0 && v.compareTo(int_s2)<0) {
					this.deleting_first_level(o.toString(),i);
				}else if(v.compareTo(int_s2)>=0 && i+1==lines.size()-1) {
					this.deleting_first_level(o.toString(),i+1);
				}else {
					continue;
				}
			}else if(datatype.equals("java.lang.Double")) {
				Double v = (Double)o;
				Double double_s1 = Double.parseDouble(s1);				
				Double double_s2 = Double.parseDouble(s2);
				if(v.compareTo(double_s1)>=0 && v.compareTo(double_s2)<0) {
					this.deleting_first_level(o.toString(),i);
				}else if(v.compareTo(double_s2)>0 && i+1==lines.size()-1) {
					this.deleting_first_level(o.toString(),i+1);
				}else {
					continue;
				}
				
			}else {
				
			}
		}
		}
	}catch(IOException e) {
		e.printStackTrace();
	}
	
}
public  ArrayList<String[]> selecting_from_index(SQLTerm sqlterm,ArrayList<String[]> result) {
	if(this.level==Indexlevel.ONE) {
	result = selecting_one_level(sqlterm,result);
	}else {
//	result = selecting_second_level(sqlterm,result);
	}
	return result;
}
//public ArrayList<String[]> selecting_first_level(SQLTerm sqlterm,ArrayList<String[]> result){
//	String csvFile = this.index_name+"1"+".csv";
//	ArrayList<String> pk_values = new ArrayList<String>();
//	try {
//		String datatype = table.get_datatype(table.Clustering_key);
//		BufferedReader br = new BufferedReader(new FileReader("src/resources/"+csvFile));
//		String line = "";
//		while((line = br.readLine())!=null){
//			String data = line;
//			pk_values.add(data);
//			}
//		br.close();
//	}catch(IOException e) {
//		e.printStackTrace();
//	}
//}
public ArrayList<String[]> selecting_one_level(SQLTerm sqlterm,ArrayList<String[]> result){
	String csvFile = this.index_name+".csv";
	ArrayList<String> pk_values = new ArrayList<String>();
	try {
		String datatype = table.get_datatype(this.column_name);
		BufferedReader br = new BufferedReader(new FileReader("src/resources/"+csvFile));
		String line = "";
		while((line = br.readLine())!=null){
			String data = line;
			pk_values.add(data);
			}
		br.close();
		for(int i = 0;i<pk_values.size()-1;i++){
			String s1 = pk_values.get(i);
			String s2 = pk_values.get(i+1);
			switch(sqlterm._strOperator){
			case"=":
				if(datatype.equals("java.lang.Integer")) {
					Integer pk_value1 = Integer.parseInt(s1);
					Integer pk_value2 = Integer.parseInt(s2);
					if(((Integer) sqlterm._objValue).compareTo(pk_value1)>=0 && ((Integer)sqlterm._objValue).compareTo(pk_value2)<0){
						for(Page p: table.created_Pages){
							if(p.get_first_row()[table.col_location(this.column_name)].equals(s1)) {
								result.add(p.get_first_row());
								return result;
							}else if(p.get_first_row()[table.col_location(this.column_name)].equals(s2)){
								result.add(p.get_first_row());
								return result;
							}else {
								result.add(p.get_specified_pk_row(sqlterm._objValue.toString()));
								return result;
							}
						}
				}
				}
				else if(datatype.equals("java.lang.String")) {
					if(((String) sqlterm._objValue).compareTo(s1)>=0 && ((String)sqlterm._objValue).compareTo(s2)<0){
						for(Page p: table.created_Pages){
							if(p.get_first_row()[table.col_location(this.column_name)].equals(s1)) {
								result.add(p.get_first_row());
								return result;
							}else if(p.get_first_row()[table.col_location(this.column_name)].equals(s2)){
								result.add(p.get_first_row());
								return result;
							}else {
								result.add(p.get_specified_pk_row(sqlterm._objValue.toString()));
								return result;
							}
						}
				}
				}
					
				else if(datatype.equals("java.lang.Double")) {
					Double pk_value1 = Double.parseDouble(s1);
					Double pk_value2 = Double.parseDouble(s2);
					if(((Double) sqlterm._objValue).compareTo(pk_value1)>=0 && ((Double)sqlterm._objValue).compareTo(pk_value2)<0){
						for(Page p: table.created_Pages){
							if(p.get_first_row()[table.col_location(this.column_name)].equals(s1)) {
								result.add(p.get_first_row());
								return result;
							}else if(p.get_first_row()[table.col_location(this.column_name)].equals(s2)){
								result.add(p.get_first_row());
								return result;
							}else {
								result.add(p.get_specified_pk_row(sqlterm._objValue.toString()));
								return result;
							}
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
			}
			}
	}catch(IOException e) {
		e.printStackTrace();
	}
	return result;
}
//			case">=":
//				if(datatype.equals("java.lang.Integer")) {
//					Integer pk_value1 = Integer.parseInt(s1);
//					Integer pk_value2 = Integer.parseInt(s2);
//					if(((Integer) sqlterm.objValue).compareTo(pk_value1)>=0 && ((Integer)sqlterm.objValue).compareTo(pk_value2)<0){
//						for(Page p: table.created_Pages){
//							if(p.get_first_row()[table.col_location(this.column_name)].equals(s1)) {
//								result.add(p.get_first_row());
//								return result;
//							}else if(p.get_first_row()[table.col_location(this.column_name)].equals(s2)){
//								result.add(p.get_first_row());
//								return result;
//							}else {
//								result.add(p.get_specified_pk_row(sqlterm.objValue.toString()));
//								return result;
//							}
//						}
//				}
//					}
//				else if(datatype.equals("java.lang.String")) {
//					if(s1[location].compareTo((String)sqlterm.objValue)>=0){
//							result.add(s1);
//						}
//					}
//				else if(datatype.equals("java.lang.Double")) {
//					Double value = Double.parseDouble(s1[location]);
//					if(value.compareTo((Double)sqlterm.objValue)>=0){
//							result.add(s1);
//					}
//				}
////				else if(datatype.equals("java.util.Date")) {
////					Date value = Date.parse(s1[location]);
////					if(value.compareTo((Double)sqlterm.objValue)==0){
////						if(sqlterm.strColumnName.equals(table.Clustering_key)) {
////							result.add(s1);
////							return result;
////						}else {
////							result.add(s1);
////						}
////					}
////				}
//				break;
//			case"<":
//				if(datatype.equals("java.lang.Integer")) {
//					Integer value = Integer.parseInt(s1[location]);
//					if(value.compareTo((Integer)sqlterm.objValue)<0){
//							result.add(s1);
//						}
//					}
//				else if(datatype.equals("java.lang.String")) {
//					if(s1[location].compareTo((String)sqlterm.objValue)<0){
//							result.add(s1);
//						}
//					}
//				else if(datatype.equals("java.lang.Double")) {
//					Double value = Double.parseDouble(s1[location]);
//					if(value.compareTo((Double)sqlterm.objValue)<0){
//							result.add(s1);
//					}
//				}
////				else if(datatype.equals("java.util.Date")) {
////					Date value = Date.parse(s1[location]);
////					if(value.compareTo((Double)sqlterm.objValue)==0){
////						if(sqlterm.strColumnName.equals(table.Clustering_key)) {
////							result.add(s1);
////							return result;
////						}else {
////							result.add(s1);
////						}
////					}
////				}
//				break;
//			case"<=":
//				if(datatype.equals("java.lang.Integer")) {
//					Integer value = Integer.parseInt(s1[location]);
//					if(value.compareTo((Integer)sqlterm.objValue)<=0){
//							result.add(s1);
//						}
//					}
//				else if(datatype.equals("java.lang.String")) {
//					if(s1[location].compareTo((String)sqlterm.objValue)<=0){
//							result.add(s1);
//						}
//					}
//				else if(datatype.equals("java.lang.Double")) {
//					Double value = Double.parseDouble(s1[location]);
//					if(value.compareTo((Double)sqlterm.objValue)<=0){
//							result.add(s1);
//					}
//				}
////				else if(datatype.equals("java.util.Date")) {
////					Date value = Date.parse(s1[location]);
////					if(value.compareTo((Double)sqlterm.objValue)==0){
////						if(sqlterm.strColumnName.equals(table.Clustering_key)) {
////							result.add(s1);
////							return result;
////						}else {
////							result.add(s1);
////						}
////					}
////				}
//				break;
//			case">":
//				if(datatype.equals("java.lang.Integer")) {
//					Integer value = Integer.parseInt(s1[location]);
//					if(value.compareTo((Integer)sqlterm.objValue)>0){
//							result.add(s1);
//						}
//					}
//				else if(datatype.equals("java.lang.String")) {
//					if(s1[location].compareTo((String)sqlterm.objValue)>0){
//							result.add(s1);
//						}
//					}
//				else if(datatype.equals("java.lang.Double")) {
//					Double value = Double.parseDouble(s1[location]);
//					if(value.compareTo((Double)sqlterm.objValue)>0){
//							result.add(s1);
//					}
//				}
////				else if(datatype.equals("java.util.Date")) {
////					Date value = Date.parse(s1[location]);
////					if(value.compareTo((Double)sqlterm.objValue)==0){
////						if(sqlterm.strColumnName.equals(table.Clustering_key)) {
////							result.add(s1);
////							return result;
////						}else {
////							result.add(s1);
////						}
////					}
////				}
//				break;
//			}
//	}catch(IOException e) {
//		e.printStackTrace();
//	}
//}
//public ArrayList<String[]> selecting_second_level(SQLTerm sqlterm,ArrayList<String[]> result){
//	String csvFile = this.index_name+"2"+".csv";
//	ArrayList<String[]> lines = new ArrayList<String[]>();
//	try {
//		String datatype = table.get_datatype(this.column_name);
//		BufferedReader br = new BufferedReader(new FileReader("src/resources/"+csvFile));
//		String line = "";
//		while((line = br.readLine())!=null){
//			String[] data = line.split(",");
//			lines.add(data);
//			}
//		br.close();
//	}catch(IOException e) {
//		e.printStackTrace();
//	}
//}
		
//	public void insert_first_level() {
//		try {
//		 	BufferedReader br = new BufferedReader(new FileReader("src/resources/"+this.index_name+"1"+".csv"));
//					ArrayList<String[]> lines = new ArrayList<>();
//					ArrayList<String[]> newlines = new ArrayList<>();
//		}catch(IOException e){
//			e.printStackTrace();
//		}
//	}
//	public void insert_second_level(ArrayList<Object> tuples) {
//		String csvFile = this.index_name+"2"+".csv";
//		try {
//		 	BufferedReader br = new BufferedReader(new FileReader("src/resources/"+this.index_name+"1"+".csv"));
//					ArrayList<String[]> lines = new ArrayList<>();
//					ArrayList<String[]> newlines = new ArrayList<>();
//		}catch(IOException e) {
//			e.printStackTrace();
//		}
//	}
	
	public void write_first_level() {
		String csvFile = this.index_name+"1"+".csv";
		if(this.isEmpty_for_first==true){
			if(Table.total_no_pages==0) {
				Page.write_pages_file();
			}
			this.writing_index_information(csvFile);
			this.isEmpty_for_first = false;
		}
			ArrayList<ArrayList<String[]>> allrows = new ArrayList<ArrayList<String[]>>();
			List<String[]> sorting_rows = new ArrayList<String[]>(); 
			for(Page p:table.created_Pages) {
				 	ArrayList<String[]> lines = p.page_rows_of_column(this.column_name);
				 	allrows.add(lines);
			}
			for(int i = 0;i<allrows.size();i++){
				ArrayList<String[]> r = allrows.get(i);
				for(int j = 0;j<r.size();j++) {
					sorting_rows.add(r.get(j));
				}
			}
			String datatype = "";
			try {
			datatype = table.get_datatype(column_name);
			}catch(IOException e) {
				e.printStackTrace();
			}
			if(datatype.equals("java.lang.String")) {
			Comparator<String[]> comparator = new Comparator<String[]>() {
				@Override
				public int compare(String[] o1, String[] o2) {
					// TODO Auto-generated method stub
					return o1[0].compareTo(o2[0]);
				}
			};
			sorting_rows.sort(comparator);
			}
			else if(datatype.equals("java.lang.Integer")) {
				Comparator<String[]> comparator = new Comparator<String[]>() {
					@Override
					public int compare(String[] o1, String[] o2) {
						// TODO Auto-generated method stub
						 return Integer.compare(Integer.parseInt(o1[0]), Integer.parseInt(o2[0]));
					}
				};
				sorting_rows.sort(comparator);
			}
			else if(datatype.equals("java.lang.Double")) {
				Comparator<String[]> comparator = new Comparator<String[]>() {
					@Override
					public int compare(String[] o1, String[] o2) {
						// TODO Auto-generated method stub
						 return Double.compare(Double.parseDouble(o1[0]), Double.parseDouble(o2[0]));
					}
				};
				sorting_rows.sort(comparator);
			}
			try {
				 	BufferedWriter bw = new BufferedWriter(new FileWriter("src/resources/"+csvFile));
            for (String[] fields : sorting_rows) {
                String line_updated = String.join(",", fields);
                bw.write(line_updated);
                bw.newLine();
            }
           bw.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
			this.write_second_level();
	}
	public void write_second_level(){
		String csvFile = this.index_name+"2"+".csv";
		if(this.isEmpty_for_second==true) {
			this.writing_index_information(csvFile);
			this.isEmpty_for_second = false;
		}
		try {
		 	BufferedReader br = new BufferedReader(new FileReader("src/resources/"+this.index_name+"1"+".csv"));
					ArrayList<String[]> lines = new ArrayList<>();
					ArrayList<String[]> newlines = new ArrayList<>();
					String line;
					boolean list_ended = false;
					int rows_to_add = 0;
					while((line = br.readLine())!=null){
						String[]data = line.split(",");
						lines.add(data);
						}
					br.close();
					for(int i = 0;i<lines.size();i++){
						String[] s = lines.get(i);
						if(rows_to_add%DBApp.MaximumRowsCountinTablePage==0){
							while(contains_duplicates(newlines,s)==true){
								i++;
								if(i==lines.size()) {
									list_ended = true;
									break;
								}
								s = lines.get(i);
								rows_to_add++;
							}
							if(list_ended==true)
								break;
							rows_to_add++;
							newlines.add(s);
						}
					else{
							rows_to_add++;
						}
					}
					 	BufferedWriter bw = new BufferedWriter(new FileWriter("src/resources/"+csvFile));
	            for (String[] fields : newlines) {
	                String line_updated = String.join(",", fields);
	                bw.write(line_updated);
	                bw.newLine();
	            }
	           bw.close();
	}catch(IOException e) {
		e.printStackTrace();
	}
	}
	public  boolean contains_duplicates(ArrayList<String[]> newlines,String[] s){
		String datatype = "";
		try{
			 datatype = table.get_datatype(column_name);
		}catch(IOException e) {
			e.printStackTrace();
		}
		for(int i = 0;i<newlines.size();i++){
			String[] s1 = newlines.get(i);
			if(datatype.equals("java.lang.Integer")) {
				if(Integer.parseInt(s1[0]) == Integer.parseInt(s[0])){
					return true;
				}
			}else if(datatype.equals("java.lang.String")){
					if(s1[0].compareTo(s[0])==0) {
						return true;
					}
				}else if(datatype.equals("java.lang.Double")) {
					if(Double.parseDouble(s1[0])== Double.parseDouble(s[0])) {
						return true;
					}
				}else if(datatype.equals("java.util.Date")) {
					
				}
			}			
		return false;
	}
	public void writing_index_information(String csvFile) {
		try{FileWriter filewriter = new FileWriter("src/resources/files.csv",true);
		filewriter.write(this.index_name);
		filewriter.append(",");
		filewriter.write("SparseIndex");
		filewriter.append(",");
		filewriter.write(csvFile);
		filewriter.append(",");
		filewriter.write("src/resources/");
		filewriter.write("\n");
		filewriter.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	
}
