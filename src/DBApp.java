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
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;
import java.io.*;

public class DBApp {
	
	
	public void init( ) {
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
        String pageName = strTableName + 1 + ".class";
        String pageDirectory = "src/resources/" + strTableName + 1 + ".class";
        table.getPageNameDirectory().put(pageName, pageDirectory);
        
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

	public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
		
		try {
			 
			 int latestPage = findLatestPage(strTableName);
	         FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + latestPage + ".class");
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         Page p = (Page) in.readObject();
	         String pageName = p.getTableName() + p.getPageNumber();
	         
	      
	         if(p.getColNameValue().size()<2) {
	        	 Row row = new Row(pageName, htblColNameValue);
	        	 p.getColNameValue().add(row);  //INSERTION SORT??
	        	 System.out.println(p.getColNameValue().size());
	        	 
	        	 FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + 1 + ".class");
	             ObjectOutputStream out = new ObjectOutputStream(fileOut);
	             out.writeObject(p);
	             out.close();
	             fileOut.close();
	        	 
	        	 
	         }
	        	
	         else {
	        	 File file = new File("src/resources/" + strTableName +  (latestPage+1) + ".class");
	             file.createNewFile();
	             FileOutputStream newFileIn = new FileOutputStream("src/resources/" + strTableName + (latestPage+1) + ".class");
		         ObjectOutputStream newIn = new ObjectOutputStream(newFileIn);  
	             Page newPage = new Page((latestPage+1), p.getClusteringKey(), strTableName , p.getHtblColNameType() , p.getHtblColNameMin() , p.getHtblColNameMax() );
	             String newPageName = newPage.getTableName() + newPage.getPageNumber();
	             Row newRow = new Row(newPageName, htblColNameValue);
	             newPage.getColNameValue().add(newRow);
	             newIn.writeObject(newPage);
	             newIn.close();
	             newFileIn.close();
	             
	             
	         }
	         in.close();
	         fileIn.close();
	         
	         
	      } 
		catch (Exception i) {
	         i.printStackTrace(); } 

		
	}
	
	public void updateTable(String strTableName, String strClusteringKeyValue,Hashtable<String,Object> htblColNameValue )throws DBAppException {
		
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
	
	public static void main(String[] args) throws DBAppException
    {
  
		String strTableName = "Student";
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
		
	/*	DBApp dbApp = new DBApp( );
		Hashtable htblColNameValue = new Hashtable();
		htblColNameValue.put("id", 1);
		htblColNameValue.put("name", "mina");
		htblColNameValue.put("gpa", 0); 
		dbApp.insertIntoTable("Student", htblColNameValue);  */
		
	/*	Hashtable htblColNameValue = new Hashtable();
		htblColNameValue.put("id", 1);
		htblColNameValue.put("name", "mina");
		htblColNameValue.put("gpa", 0); 
		Row row = new Row("Student1", htblColNameValue); */
		
		
		
		
	/* try {
		 
			FileInputStream fileIn;
			fileIn = new FileInputStream("src/resources/Student2.class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
	         Page p = (Page) in.readObject();
	         in.close();
	         fileIn.close();
	         System.out.println(p.getColNameValue().size()); 
	         
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}     	
		*/
		
	 
	  
		
		  
		
	
		
		
    }
	


}
	