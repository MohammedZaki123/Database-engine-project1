package engine;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class Page_information{
	public static void main(String[] args) {
			new Page_information();
		
	}
	public Page_information(){
		try {
		FileWriter writer = new FileWriter("src/resources/files.csv"); 
	    writer.append("Name,Type,FileName,FileLocationonHarddisk");
	    writer.append("\n");
	    writer.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
	}
	public  void writing_page_information(String table_name,String page_name) throws IOException{
		FileWriter filewriter = new FileWriter("src/resources/files.csv",true);
		filewriter.write(table_name);
		filewriter.append(",");
		filewriter.write("Table");
		filewriter.append(",");
		filewriter.write(page_name);
		filewriter.append(",");
		filewriter.write("D:\\DB_project\\");
		filewriter.write("\n");
		filewriter.close();
	}
//	public  void writeCsv(int maxNumber) {
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
//		@Override
//		public void writeCSV() {
//			// TODO Auto-generated method stub
//			
//		}
//		@Override
//		public void readCSV() {
//			// TODO Auto-generated method stub
//			
//		}
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
			
	

}
