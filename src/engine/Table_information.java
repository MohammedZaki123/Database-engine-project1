package engine;
import java.awt.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
public class Table_information {
	public Table_information() {
		try {
		FileWriter writer = new FileWriter("src/resources/metadata.csv"); 
	    writer.append("Table Name,Column Name,Column Type,ClusteringKey,IndexName,IndexType,min,max");
	    writer.append("\n");
	    writer.close();
		}catch(IOException e) {
	    	e.printStackTrace();
	    }
	}
//	public static void main(String[] args) throws IOException {
//		Table_information info = new Table_information();
//	}
//	public static void main(String[]args) throws IOException{
//		FileWriter writer = new FileWriter("D:\\DB_project\\metadata.csv"); 
//	    writer.append("TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType,min,max");
//	    writer.append("\n");
//	    writer.close();
//		  System.out.println("starting write metadata.csv file: ");
////		  writeCsv(1);
//		  
//		  System.out.println("starting read user.csv file");
//		  readCsv(filePath);
		 
// 
//         public void write_meta_data(){
//        	  when table is created, the columns of table is added to meta data csv file
//        	  Writing a csv file to disk containing the following information of each column of table:
//        	  Table Name  
//        	  Column Name  
//        	  Column Type
//        	  Clustering key boolean
//        	  IndexName
//        	  IndexType
//        	  min(minimum value accepted in the column)
//        	  max
//      	 
       }
//		 public  void writeCsv(int maxNumber) {
//		  ArrayList<User> users = new ArrayList<User>();
//		  
//		  //create demo Users
//		  User user = new User();
//		  user.setId(40-3333);
//		  user.setName("Ahmed Nour");
//		  user.setAge(20);
//		  user.setMajor("CSEN");
//		  user.setGpa(1.1);
//		  users.add(user);
//		  user = new User();
//		  user.setId(40-4454);
//		  user.setName("John Peter");
//		  user.setAge(21);
//		  user.setMajor("CSEN");
//		  user.setGpa(1.4);
//		  users.add(user);
//		  FileWriter fileWriter = null;
//		  try {
//		   String filePath = "D:\\csv\\users1.csv";
//		   fileWriter = new FileWriter(filePath);
//		   fileWriter.append("Id,Name,age,Major,gpa\n");
//		   int rowCounter = 0;
//		    while(rowCounter<maxNumber){
//		   for(User u: users){
//			fileWriter.append(String.valueOf(u.getId()));
//		    fileWriter.append(",");
//		    fileWriter.append(u.getName());
//		    fileWriter.append(",");
//		    fileWriter.append(String.valueOf(u.getAge()));
//		    fileWriter.append(",");
//		    fileWriter.append(u.getMajor());
//		    fileWriter.append(",");
//		    fileWriter.append(String.valueOf(u.getGpa()));
//		    fileWriter.append("\n");
//		    rowCounter++;
//		   }
//		   }
//		    new Page(maxNumber);
//		  } catch (Exception ex) {
//		   ex.printStackTrace();
//		  } finally {
//		   try {
//		    fileWriter.flush();
//		    fileWriter.close();
//		   } catch (Exception e) {
//		    e.printStackTrace();
//		   }
//		  }
//		 }
//		
//		
		 
//		 public static void readCsv(String filePath) {
//		  BufferedReader reader = null;
//		  
//		  try {
//		   ArrayList<User> users = new ArrayList<User>();
//		   String line = "";
//		   reader = new BufferedReader(new FileReader(filePath));
//		   reader.readLine();
//		   
//		   while((line = reader.readLine()) != null) {
//		    String[] fields = line.split(",");
//		    
//		    if(fields.length > 0) {
//		     User user = new User();
//		     user.setId(Integer.parseInt(fields[0]));
//		     user.setFirstName(fields[1]);
//		     user.setLastName(fields[2]);
//		     users.add(user);
//		    }
//		   }
//		   
//		   for(User u: users) {
//		    System.out.printf("[userId=%d, firstName=%s, lastName=%s]\n", u.getId(), u.getFirstName(), u.getLastName());
//		   }
//		   
//		  } catch (Exception ex) {
//		   ex.printStackTrace();
//		  } finally {
//		   try {
//		    reader.close();
//		   } catch (Exception e) {
//		    e.printStackTrace();
//		   }
//		  }
//		  
//		 }
	
