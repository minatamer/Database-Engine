import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;
import java.io.*;

public class DBApp  {
	private int maximumRowsCountinTablePage;
	private int maximumEntriesinOctreeNode;
	
	public DBApp() {
	    init();
	}	
	public int getMaximumRowsCountinTablePage() {
		return maximumRowsCountinTablePage;
	}


	public int getMaximumEntriesinOctreeNode() {
		return maximumEntriesinOctreeNode;
	}
	
	
	public void init( ) {
		Properties prop=new Properties();

		try {
			FileInputStream ip= new FileInputStream("src/resources/DBApp.config");
			prop.load(ip);
			maximumRowsCountinTablePage = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
			maximumEntriesinOctreeNode = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 
	
	
	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax )  throws DBAppException {
		
		//CHECKING IF TABLE ALREADY EXISTS
		try {
	        
        	BufferedReader br = new BufferedReader(new FileReader("src/metadata.csv"));
        	String line;
        	while ((line = br.readLine()) != null) {
               String[] values = line.split(",");
               if (values[0].equals(strTableName))
            	   throw new DBAppException("Table Already Exists");
        	}
        }
        	catch (IOException e) {
                e.printStackTrace();
            }
		
		
		
		//ADD VALUES TO METADATA.CSV IF TABLE DOES NOT ALREADY EXIST
        try {
        	FileWriter csvWriter = new FileWriter("src/metadata.csv");
        	Enumeration<String> enumerator = htblColNameType.keys();
        	while (enumerator.hasMoreElements()) {
        		 
                String key = enumerator.nextElement();
               
                	csvWriter.append(strTableName).append(",");
                	csvWriter.append(key).append(",");
                	csvWriter.append(htblColNameType.get(key)).append(",");
                	if (strClusteringKeyColumn.equals(key)) {
                		csvWriter.append("True").append(",");
                	}
                	else {
                		csvWriter.append("False").append(",");
                	}
                	
                	//HANDLE IN MILESTONE 2 FOR INDICES
                	csvWriter.append("null").append(",");
                	csvWriter.append("null").append(",");
                	//HANDLE IN MILESTONE 2 FOR INDICES
                
                	csvWriter.append(htblColNameMin.get(key)).append(",");
                	csvWriter.append(htblColNameMax.get(key));
                	csvWriter.append("\n");
                	
                	csvWriter.flush();
                    
           }
        	
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        
        
        
        //CREATING NEW TABLE IF TABLE DOES NOT ALREADY EXIST AND MAKING A SERIALIZABLE FILE
        
        Page page= new Page(1, strClusteringKeyColumn, strTableName , htblColNameType , htblColNameMin , htblColNameMax );
        Table table = new Table ();
        String pageDirectory = "src/resources/" + strTableName + 1 + ".class";
        table.getPageDirectories().add(pageDirectory);
        
        try {
            File file = new File("src/resources/" + strTableName +  1 + ".class");
            file.createNewFile();
            FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + 1 + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(page);
            out.close();
            fileOut.close();
            
            File tablefile = new File("src/resources/" + strTableName + "Table.class");
            tablefile.createNewFile();
            FileOutputStream tablefileOut = new FileOutputStream("src/resources/" + strTableName + "Table.class");
            ObjectOutputStream tableout = new ObjectOutputStream(tablefileOut);
            tableout.writeObject(table);
            tableout.close();
            tablefileOut.close();
            
        
        }
        catch (IOException e) {
            e.printStackTrace();
        } 
        
        
        
	}

	
	
public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException, EOFException {
		
			int counter=1;
			boolean pageFound=false;
			while(pageFound==false) {
				//what if it can not be inputted in any of the pages? NEW PAGE.
				 File tmpDir = new File("src/resources/" + strTableName + counter + ".class"); 
				 boolean exists = tmpDir. exists();
				 if (exists==false) {
					 File file = new File("src/resources/" + strTableName +  counter + ".class");
			            file.createNewFile();
			            FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + counter + ".class");
			            ObjectOutputStream out = new ObjectOutputStream(fileOut);
			            //GET PAGE ATTRIBUTES FROM ANOTHER PAGE
			            FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + (counter-1) + ".class");
			            ObjectInputStream in = new ObjectInputStream(fileIn);
			            Page p = (Page) in.readObject();
			            
			         
			            
			            Page newPage = new Page(counter,p.getClusteringKey() , strTableName, p.getHtblColNameType() , p.getHtblColNameMin(), p.getHtblColNameMax());
			            Row newRow = new Row(p.getTableName()+p.getPageNumber(), htblColNameValue);
			            newPage.getRows().add(newRow);
			            
			            
			            FileInputStream tableFileIn = new FileInputStream("src/resources/" + strTableName + "Table.class");
						 ObjectInputStream tableIn = new ObjectInputStream(tableFileIn);
						 Table t = (Table) in.readObject(); 
						 
						 t.getPageDirectories().add(strTableName+newPage.getPageNumber());
						 
						 FileOutputStream tableFileOut = new FileOutputStream("src/resources/" + strTableName + "Table.class");
				         ObjectOutputStream tableOut = new ObjectOutputStream(tableFileOut);
				         tableOut.writeObject(p);
				         tableOut.close();
				         tableFileOut.close();
			            
			            
			            
			            out.writeObject(newPage);
			            out.close();
			            fileOut.close();
			            
			            FileOutputStream oldfileOut = new FileOutputStream("src/resources/" + strTableName + (counter-1) + ".class");
				         ObjectOutputStream oldOut = new ObjectOutputStream(oldfileOut);
				         oldOut.writeObject(p);
				         oldOut.close();
				         oldfileOut.close();
				         
				         pageFound=true;
				         
				         
				         
				         
				         
				         
				         
				         
				         break;
			            
			            
			            
				 }
			 FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + counter + ".class");
			 try (ObjectInputStream in = new ObjectInputStream(fileIn)) {
			    
				 Page p = (Page) in.readObject();
				 String clusteringKey = p.getClusteringKey();
				 Object valueToCompareWith = htblColNameValue.get(clusteringKey);
				 //IF EMPTY PAGE, INSERT IMMEDIATELY
				 if(p.getRows().size()==0) {
					 Row newRow = new Row(p.getTableName()+p.getPageNumber(), htblColNameValue);
					 p.getRows().add(newRow);
					 FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + counter + ".class");
			         ObjectOutputStream out = new ObjectOutputStream(fileOut);
			         out.writeObject(p);
			         out.close();
			         fileOut.close();
					 
					 
					 pageFound=true;
					 break;
				 }
				 
				 
				 Object lastValueFromRows =p.getRows().lastElement().getColNameValue().get(clusteringKey);
				 if(compare(valueToCompareWith,lastValueFromRows)>=0 && p.getRows().size()==200) {
					 //if true, it cannot be inserted, so serialize this page and increase counter
					 FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + counter + ".class");
			         ObjectOutputStream out = new ObjectOutputStream(fileOut);
			         out.writeObject(p);
			         out.close();
			         fileOut.close();
					 counter++; 
					 } 
				
				 else  {
					 pageFound=true; //this page should definitely have room for insertion}
					 binarySearchInsert(strTableName,p,htblColNameValue, htblColNameValue.get(p.getClusteringKey()));
			
			    
				
					 
					 FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + counter + ".class");
			         ObjectOutputStream out = new ObjectOutputStream(fileOut);
			         out.writeObject(p);
			         out.close();
			         fileOut.close();
			         break;
				 }
					 
				 
			 }
		}
		FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + counter + ".class");
		 ObjectInputStream in = new ObjectInputStream(fileIn);
		  Page p = (Page) in.readObject(); 
		 if(p.getRows().size()>maximumRowsCountinTablePage) {
				
			 Row extraRow = p.getRows().lastElement();
			 p.getRows().remove(p.getRows().size()-1);
			 insertIntoTable(strTableName, extraRow.getColNameValue());
			 FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + counter + ".class");
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(p);
	         out.close();
	         fileOut.close(); 
	         
	         
	         
	        
	         
			 
			 
		 }	 
			//call the binary search + insert method to insert in the right place
		
		
		
		
		
		
	}
	public void updateTable(String strTableName, String strClusteringKeyValue,Hashtable<String,Object> htblColNameValue )throws DBAppException, ClassNotFoundException, IOException, ParseException {
		
		//SEARCH FOR PAGE INDEX AND ROW INDEX
		int pageIndex = binarySearchPage(strTableName,strClusteringKeyValue) ;
		Object keyValue= stringConverter(strClusteringKeyValue,strTableName); //CONVERTS KEY STRING TO AN OBJECT TO BE COMPARABLE
		int rowIndex = binarySearchRow(strTableName, pageIndex, keyValue);
		//UPDATE IT
		FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + pageIndex + ".class");
		 ObjectInputStream in = new ObjectInputStream(fileIn) ;
		 Page p = (Page) in.readObject();
		 htblColNameValue.put(p.getClusteringKey(), keyValue); //MILESTONE SAYS THAT THE KEY WILL NOT BE PART OF THE HASHTABLE
		 Row newRow = new Row(strTableName + pageIndex,htblColNameValue);
		 p.getRows().set(rowIndex, newRow);
		
		//PUT IT BACK
		    FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + pageIndex + ".class");
	        ObjectOutputStream out = new ObjectOutputStream(fileOut);
	        out.writeObject(p);
	        out.close();
	        fileOut.close();
		
	}
	
	public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
		
		
	}
	
	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
		
	}
	
	//public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)throws DBAppException {}
	
	
 	public static int findLatestPage (String tableName) {
 		int num = 1;
		File file = new File("src/resources/");
		File[] list = file.listFiles();
		for (File fil : list) {
			if (fil.getName().equals(tableName + num + ".class")){
				num++;
			}		
		}
		num--;
		return num;
		
	}  
 	
 	public static void binarySearchInsert(String strTableName, Page p, Hashtable<String,Object> htblColNameValue, Object valueToCompareWith){
 		String clusteringKey = p.getClusteringKey();
 		if(compare(valueToCompareWith,p.getRows().firstElement().getColNameValue().get(clusteringKey))<0) {
 			//shift down and insert at first slot
 			Row row = new Row (p.getTableName()+p.getPageNumber(), htblColNameValue);
 			p.getRows().add(0, row);
 		}
 		else if(compare(valueToCompareWith,p.getRows().lastElement().getColNameValue().get(clusteringKey) )>0){
 			Row row = new Row (p.getTableName()+p.getPageNumber(), htblColNameValue);
 			p.getRows().add(row);
 		}
 		else { //binary searching
 		int first=0;
 		int last=p.getRows().size();
 		int mid=(first + last)/2;
 		
 		while(first<=last) {
 		if(compare(valueToCompareWith,p.getRows().get(mid).getColNameValue().get(clusteringKey))>0) {
 			first = mid + 1;
 		}else if((compare(valueToCompareWith, p.getRows().get(mid).getColNameValue().get(clusteringKey))==0) 
 				||(compare(valueToCompareWith, p.getRows().get(mid).getColNameValue().get(clusteringKey))<0 && compare(valueToCompareWith,p.getRows().get(mid-1).getColNameValue().get(clusteringKey))>0) ){
 			//we found the index, shift down then insert here.
 			Row row = new Row (p.getTableName()+p.getPageNumber(), htblColNameValue);
 			p.getRows().add(mid, row);
 			break;
 			
 		}else {
 			last = mid - 1;
 		}
 		    mid = (first + last)/2;
 		}	
 		
 		}
 	}
 	
 	public static int binarySearchPage(String strTableName, String key) throws IOException, ParseException, ClassNotFoundException {
 		
 		String type = "";
 		Object keyValue = null;
 		int pageIndex =0;
 		
	try {
	        
        	BufferedReader br = new BufferedReader(new FileReader("src/metadata.csv"));
        	String line;
        	while ((line = br.readLine()) != null) {
               String[] values = line.split(",");
               if (values[0].equals(strTableName))
            	   if (values[3].equals("TRUE"))
            		   type = values[2];
        	}
        }
        	catch (IOException e) {
                e.printStackTrace();
            }
 		if(type.equals("java.lang.Integer")) {
 			keyValue = Integer.parseInt(key);
 		}
 		else
 		if(type.equals("java.lang.Double")) {
 			keyValue = Double.parseDouble(key);
 		}
 		else
 		if(type.equals("java.util.Date")) {
 			Date date1=new SimpleDateFormat("YYYY-MM-DD").parse(key);  
 			keyValue = date1;
 		}
 		else{
 			keyValue = key;
 		}
 		
	
	
 		
 		int counter=1;
		boolean pageFound=false;
		while(pageFound==false) {
			 File tmpDir = new File("src/resources/" + strTableName + counter + ".class"); 
			 boolean exists = tmpDir. exists();
			 if (exists==true) {
		     FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + counter + ".class");
		     try (ObjectInputStream in = new ObjectInputStream(fileIn)) {
		    
			 Page p = (Page) in.readObject();
			 String clusteringKey = p.getClusteringKey();
			 if(p.getRows().size()==0) {
				 FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + counter + ".class");
		         ObjectOutputStream out = new ObjectOutputStream(fileOut);
		         out.writeObject(p);
		         out.close();
		         fileOut.close();
		         counter++;
			 }
			 
			 else {
				 Object lastValueFromRows =p.getRows().lastElement().getColNameValue().get(clusteringKey);
				 if(compare(keyValue,lastValueFromRows)>=0 ) {
					 
					 FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + counter + ".class");
			         ObjectOutputStream out = new ObjectOutputStream(fileOut);
			         out.writeObject(p);
			         out.close();
			         fileOut.close();
					 counter++; 
					 } 
				
				 else  {
					 pageFound=true; 
					 pageIndex=counter;
					 FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + counter + ".class");
			         ObjectOutputStream out = new ObjectOutputStream(fileOut);
			         out.writeObject(p);
			         out.close();
			         fileOut.close();
			         break;
				 }
				 
			 }
			 
				 
			 
		 }
	   } 
	}
		
		return pageIndex;
 }

 	public static int binarySearchRow(String strTableName, int pageIndex, Object key) throws IOException, ClassNotFoundException {
 		
 		 FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + pageIndex + ".class");
		 ObjectInputStream in = new ObjectInputStream(fileIn) ;
		 Page p = (Page) in.readObject();
		 int rowIndex =0;
 		String clusteringKey = p.getClusteringKey();
 		//binary searching
 		int first=0;
 		int last=p.getRows().size();
 		int mid=(first + last)/2;
 		
 		while(first<=last) {
 		if(compare(key,p.getRows().get(mid).getColNameValue().get(clusteringKey))>0) {
 			first = mid + 1;
 		}else if((compare(key, p.getRows().get(mid).getColNameValue().get(clusteringKey))==0)){
 			rowIndex=mid;
 			break;
 			
 		}else {
 			last = mid - 1;
 		}
 		    mid = (first + last)/2;
 		}	
 		
 		FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + pageIndex + ".class");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(p);
        out.close();
        fileOut.close();
        return rowIndex;
 }
 		
 	
 	public static Object stringConverter(String key, String table) throws ParseException {
 
 		String type="";
 		Object keyValue= null;
	try {
	        
        	BufferedReader br = new BufferedReader(new FileReader("src/metadata.csv"));
        	String line;
        	while ((line = br.readLine()) != null) {
               String[] values = line.split(",");
               if (values[0].equals(table))
            	   if (values[3].equals("TRUE"))
            		   type = values[2];
        	}
        }
        	catch (IOException e) {
                e.printStackTrace();
            }
 		if(type.equals("java.lang.Integer")) {
 			keyValue = Integer.parseInt(key);
 		}
 		else
 		if(type.equals("java.lang.Double")) {
 			keyValue = Double.parseDouble(key);
 		}
 		else
 		if(type.equals("java.util.Date")) {
 			Date date1=new SimpleDateFormat("YYYY-MM-DD").parse(key);  
 			keyValue = date1;
 		}
 		else{
 			keyValue = key;
 		}
 		return keyValue;
	
 	}
 	
 	public static int compare(Object o1, Object o2) {
 		if(o1 instanceof Integer) {
 			int num1 = (Integer) o1;
 			int num2 = (Integer) o2;
 			if(num1>num2)
 				return 1;
 			if(num1<num2)
 				return -1;
 			else
 				return 0;
 			
 		}
 		if (o1 instanceof Double) {
 			double num1 = (Double) o1;
 			double num2 = (Double) o2;
 			if(num1>num2)
 				return 1;
 			if(num1<num2)
 				return -1;
 			else
 				return 0;
 		}
 			
 		if(o1 instanceof String) {
 			String num1 = (String) o1;
 			String num2 = (String) o2;
 			return num1.compareTo(num2);
 		}
 			     
 		if(o1 instanceof Date) {
 			Date num1 = (Date) o1;
 			Date num2 = (Date) o2;
 			return num1.compareTo(num2);
 		}
 		
 		return 0;
 	}
	
	public static void main(String[] args) throws DBAppException, ClassNotFoundException, IOException, ParseException
    {
  
	/*	String strTableName = "Student";
		DBApp dbApp = new DBApp( );
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");
		
		Hashtable htblColNameMin = new Hashtable( );
		htblColNameMin.put("id", "0");
		htblColNameMin.put("name", "Z");
		htblColNameMin.put("gpa", "0");
		
		Hashtable htblColNameMax = new Hashtable( );
		htblColNameMax.put("id", "10000");
		htblColNameMax.put("name",  "ZZZZZZZZZZZ");
		htblColNameMax.put("gpa", "10000");
		dbApp.createTable( strTableName, "id", htblColNameType , htblColNameMin , htblColNameMax);   
		
		//DBApp dbApp = new DBApp( );
		Hashtable htblColNameValue1 = new Hashtable();
		htblColNameValue1.put("id", 1);
		htblColNameValue1.put("name", "mina");
		htblColNameValue1.put("gpa", 0); 
		dbApp.insertIntoTable("Student", htblColNameValue1);
		
		Hashtable htblColNameValue2 = new Hashtable();
		htblColNameValue2.put("id", 2);
		htblColNameValue2.put("name", "mina");
		htblColNameValue2.put("gpa", 0); 
		dbApp.insertIntoTable("Student", htblColNameValue2);
		
		Hashtable htblColNameValue3 = new Hashtable();
		htblColNameValue3.put("id", 3);
		htblColNameValue3.put("name", "mina");
		htblColNameValue3.put("gpa", 0); 
		dbApp.insertIntoTable("Student", htblColNameValue3);
		
		Hashtable htblColNameValue4 = new Hashtable();
		htblColNameValue4.put("id", 4);
		htblColNameValue4.put("name", "mina");
		htblColNameValue4.put("gpa", 0); 
		dbApp.insertIntoTable("Student", htblColNameValue4);
		
		Hashtable htblColNameValue5 = new Hashtable();
		htblColNameValue5.put("id", 5);
		htblColNameValue5.put("name", "mina");
		htblColNameValue5.put("gpa", 0); 
		dbApp.insertIntoTable("Student", htblColNameValue5);  */
		
	  /*  DBApp dbApp = new DBApp( );
		Hashtable htblColNameValue6 = new Hashtable();
		htblColNameValue6.put("id", 4);
		htblColNameValue6.put("name", "mina");
		htblColNameValue6.put("gpa", 0); 
		dbApp.insertIntoTable("Student", htblColNameValue6); */
		
		
		
/*	 try {
		 
			FileInputStream fileIn;
			fileIn = new FileInputStream("src/resources/Student2.class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
	         Page p = (Page) in.readObject();
	         in.close();
	         fileIn.close();
	         System.out.println(p.getRows()); 
	         
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    
		*/
		

		DBApp dbApp = new DBApp( );
		Hashtable htblColNameValue6 = new Hashtable();
		htblColNameValue6.put("name", "rawanz");
		htblColNameValue6.put("gpa", 1); 
		//dbApp.updateTable("Student", "1",htblColNameValue6 );
		//System.out.println(htblColNameValue6);
		
		
		 try {
			 
				FileInputStream fileIn;
				fileIn = new FileInputStream("src/resources/Student2.class");
				ObjectInputStream in = new ObjectInputStream(fileIn);
		         Page p = (Page) in.readObject();
		         in.close();
		         fileIn.close();
		         System.out.println(p.getRows()); 
		         
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
	 
	  
		
		  
		
	
		
		
    }
	


}
	