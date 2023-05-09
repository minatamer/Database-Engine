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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

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
        	br.close();
        }
        	catch (IOException e) {
                e.printStackTrace();
            }
		
		
		//ADD VALUES TO METADATA.CSV IF TABLE DOES NOT ALREADY EXIST
        try {
        	FileWriter csvWriter = new FileWriter("src/metadata.csv", true);
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
        
        Table table = new Table (strClusteringKeyColumn, htblColNameType , htblColNameMin , htblColNameMax );
        
        try {
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

	
	
public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException, EOFException, ParseException {
		//MAKE SURE THAT THE TABLE EXISTS -> VALIDATION
	 File f = new File("src/resources/" + strTableName + "Table.class"); 
	 boolean e = f. exists();
	 
	 if(e==false) {
		 throw new DBAppException("Table does not exist");
	 }
	
	 FileInputStream tableFileIn2 = new FileInputStream("src/resources/" + strTableName + "Table.class");
	 ObjectInputStream tableIn2 = new ObjectInputStream(tableFileIn2);
	 Table t2 = (Table) tableIn2.readObject();
	 
	 FileOutputStream tableFileOut2 = new FileOutputStream("src/resources/" + strTableName + "Table.class");
     ObjectOutputStream tableOut2 = new ObjectOutputStream(tableFileOut2);
     tableOut2.writeObject(t2);
     tableOut2.close();
     tableFileOut2.close();
	 
	 String primaryKey= t2.getClusteringKey();
	 Hashtable<String,String> hashtableColumns = t2.getHtblColNameType();
	 Hashtable<String,String> hashtableMin= t2.getHtblColNameMin();
	 Hashtable<String,String> hashtableMax= t2.getHtblColNameMax();
	 
	 //VALIDTIONS ON THE HASHTABLE, CHECK IF PRIMARY KEY EXIST, CHECK IF NO VALUE IS HIGHER THAN MAX, SMALLER THAN MIN
	 if(htblColNameValue.get(primaryKey)==null)
		 throw new DBAppException("Cannot insert a tuple without a clustering key");
	 Enumeration<String> enumerator = htblColNameValue.keys();
	 while (enumerator.hasMoreElements()) {
            String key = enumerator.nextElement();
            Object o = htblColNameValue.get(key);
            String min = hashtableMin.get(key);
            String max = hashtableMax.get(key);
            
            String type = hashtableColumns.get(key);
            if(type.equals("java.lang.Integer")) {
            	if (!(o instanceof Integer))
            		throw new DBAppException(key + " has a wrong type");
            	
            	int num = (Integer) o;
            	if(num < Integer.parseInt(min))
            		throw new DBAppException(key + " is smaller than the Minimum requirement");
            	if(num > Integer.parseInt(max))
            		throw new DBAppException(key + " is bigger than the Maximum requirement");
            }
            
            if(type.equals("java.lang.Double")) {
            	if (!(o instanceof Double))
            		throw new DBAppException(key + " has a wrong type");
            	
            	double num = (Double) o;
            	if(num < Double.parseDouble(min))
            		throw new DBAppException(key + " is smaller than the Minimum requirement");
            	if(num > Double.parseDouble(max))
            		throw new DBAppException(key + " is bigger than the Maximum requirement");
            }
            
            if(type.equals("java.lang.String")) {
            	if (!(o instanceof String))
            		throw new DBAppException(key + " has a wrong type");
            	
            	String s = (String) o;
            	if(s.length()<min.length())
            		throw new DBAppException(key + " is smaller than the Minimum requirement");
            	if(s.length() > max.length())
            		throw new DBAppException(key + " is bigger than the Maximum requirement");
            }
            
            if(type.equals("java.util.Date")) {
            	if (!(o instanceof Date))
            		throw new DBAppException(key + " has a wrong type");
            	
            	Date d = (Date) o;
            	Date minDate=new SimpleDateFormat("yyyy-MM-dd").parse(min);  
            	Date maxDate=new SimpleDateFormat("yyyy-MM-dd").parse(max);  
            	if(d.compareTo(minDate)<0)
            		throw new DBAppException(key + " is smaller than the Minimum requirement");
            	if(d.compareTo(maxDate)>0)
            		throw new DBAppException(key + " is bigger than the Maximum requirement");
            }
            	
            
   
            
            
           
            	
            	
	 }
	 
	
	
	//CASE THE TABLE DOES NOT HAVE ANY PAGES AS IT IS A NEW TABLE 
	if (findLatestPage(strTableName)==0) {
		 FileInputStream tableFileIn = new FileInputStream("src/resources/" + strTableName + "Table.class");
		 ObjectInputStream tableIn = new ObjectInputStream(tableFileIn);
		 Table t = (Table) tableIn.readObject();
		 Page page= new Page(1, t.getClusteringKey(), strTableName , t.getHtblColNameType() , t.getHtblColNameMin() , t.getHtblColNameMax() );
		 t.getPageDirectories().add("src/resources/" + strTableName + 1 + ".class");
		 Row newRow = new Row(page.getTableName()+page.getPageNumber(), htblColNameValue);
		 page.getRows().add(newRow);
		 FileOutputStream pagefileOut = new FileOutputStream("src/resources/" + strTableName + 1 + ".class");
         ObjectOutputStream pageout = new ObjectOutputStream(pagefileOut);
	     pageout.writeObject(page);
	     pageout.close();
	     pagefileOut.close();
			
		 
		 FileOutputStream tableFileOut = new FileOutputStream("src/resources/" + strTableName + "Table.class");
	     ObjectOutputStream tableOut = new ObjectOutputStream(tableFileOut);
	     tableOut.writeObject(t);
	     tableOut.close();
	     tableFileOut.close();
	     
	     
	     
	     return;
		
	}
	 
     
	
	
	
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
						 Table t = (Table) tableIn.readObject(); 
						 
						 t.getPageDirectories().add("src/resources/" + strTableName+newPage.getPageNumber() + ".class");
						 
						 FileOutputStream tableFileOut = new FileOutputStream("src/resources/" + strTableName + "Table.class");
				         ObjectOutputStream tableOut = new ObjectOutputStream(tableFileOut);
				         tableOut.writeObject(t);
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
				 fileIn.close();
				 in.close();
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
				 if(compare(valueToCompareWith,lastValueFromRows)>=0 && p.getRows().size()==maximumRowsCountinTablePage) {
					 //if true, it cannot be inserted, so serialize this page and increase counter
					 FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + counter + ".class");
			         ObjectOutputStream out = new ObjectOutputStream(fileOut);
			         out.writeObject(p);
			         out.close();
			         fileOut.close();
					 counter++; 
					 } 
				
				 else  {
					 pageFound=true; //this page should definitely have room for insertion so binary search the page and insert row
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
				  fileIn.close();
				  in.close();
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
				
		
	}
	public void updateTable(String strTableName, String strClusteringKeyValue,Hashtable<String,Object> htblColNameValue )throws DBAppException, ClassNotFoundException, IOException, ParseException {
		
		//MAKE SURE THAT THE TABLE EXISTS -> VALIDATION
		File f = new File("src/resources/" + strTableName + "Table.class"); 
		 boolean e = f. exists();
		 if(e==false) {
			 throw new DBAppException("Table does not exist");
		 }
		 //VALIDATIONS ON THE HASHTABLE
		 FileInputStream tableFileIn2 = new FileInputStream("src/resources/" + strTableName + "Table.class");
		 ObjectInputStream tableIn2 = new ObjectInputStream(tableFileIn2);
		 Table t2 = (Table) tableIn2.readObject();
		 
		 FileOutputStream tableFileOut2 = new FileOutputStream("src/resources/" + strTableName + "Table.class");
	     ObjectOutputStream tableOut2 = new ObjectOutputStream(tableFileOut2);
	     tableOut2.writeObject(t2);
	     tableOut2.close();
	     tableFileOut2.close();
		 
		 String primaryKey= t2.getClusteringKey();
		 Hashtable<String,String> hashtableColumns = t2.getHtblColNameType();
		 Hashtable<String,String> hashtableMin= t2.getHtblColNameMin();
		 Hashtable<String,String> hashtableMax= t2.getHtblColNameMax();
		 
		 Object keyValue2= stringConverter(strClusteringKeyValue,strTableName);
		 
		 String primaryKeyType = hashtableColumns.get(primaryKey);
		 String primaryKeyMin = hashtableMin.get(primaryKey);
		 String primaryKeyMax = hashtableMax.get(primaryKey);
		 
		 if(primaryKeyType.equals("java.lang.Integer")) {
         	if (!(keyValue2 instanceof Integer))
         		throw new DBAppException("The clustering key has a wrong type");
         	
         	int num = (Integer) keyValue2;
         	if(num < Integer.parseInt(primaryKeyMin))
         		throw new DBAppException("The clustering key is smaller than the Minimum requirement");
         	if(num > Integer.parseInt(primaryKeyMax))
         		throw new DBAppException("The clustering key is bigger than the Maximum requirement");
         }
         
         if(primaryKeyType.equals("java.lang.Double")) {
         	if (!(keyValue2 instanceof Double))
         		throw new DBAppException("The clustering key has a wrong type");
         	
         	double num = (Double) keyValue2;
         	if(num < Double.parseDouble(primaryKeyMin))
         		throw new DBAppException("The clustering key is smaller than the Minimum requirement");
         	if(num > Double.parseDouble(primaryKeyMax))
         		throw new DBAppException("The clustering key is bigger than the Maximum requirement");
         }
         
         if(primaryKeyType.equals("java.lang.String")) {
         	if (!(keyValue2 instanceof String))
         		throw new DBAppException("The clustering key has a wrong type");
         	
         	String s = (String) keyValue2;
         	if(s.length()<primaryKeyMin.length())
         		throw new DBAppException("The clustering key is smaller than the Minimum requirement");
         	if(s.length() > primaryKeyMax.length())
         		throw new DBAppException("The clustering key is bigger than the Maximum requirement");
         }
         
         if(primaryKeyType.equals("java.util.Date")) {
         	if (!(keyValue2 instanceof Date))
         		throw new DBAppException("The clustering key has a wrong type");
         	
         	Date d = (Date) keyValue2;
         	Date minDate=new SimpleDateFormat("yyyy-MM-dd").parse(primaryKeyMin);  
         	Date maxDate=new SimpleDateFormat("yyyy-MM-dd").parse(primaryKeyMax);  
         	if(d.compareTo(minDate)<0)
         		throw new DBAppException("The clustering key is smaller than the Minimum requirement");
         	if(d.compareTo(maxDate)>0)
         		throw new DBAppException("The clustering key is bigger than the Maximum requirement");
         }
         	
		 
		 
		 Enumeration<String> enumerator = htblColNameValue.keys();
		 while (enumerator.hasMoreElements()) {
	            String key = enumerator.nextElement();
	            Object o = htblColNameValue.get(key);
	            String min = hashtableMin.get(key);
	            String max = hashtableMax.get(key);
	            
	            String type = hashtableColumns.get(key);
	            if(type.equals("java.lang.Integer")) {
	            	if (!(o instanceof Integer))
	            		throw new DBAppException(key + " has a wrong type");
	            	
	            	int num = (Integer) o;
	            	if(num < Integer.parseInt(min))
	            		throw new DBAppException(key + " is smaller than the Minimum requirement");
	            	if(num > Integer.parseInt(max))
	            		throw new DBAppException(key + " is bigger than the Maximum requirement");
	            }
	            
	            if(type.equals("java.lang.Double")) {
	            	if (!(o instanceof Double))
	            		throw new DBAppException(key + " has a wrong type");
	            	
	            	double num = (Double) o;
	            	if(num < Double.parseDouble(min))
	            		throw new DBAppException(key + " is smaller than the Minimum requirement");
	            	if(num > Double.parseDouble(max))
	            		throw new DBAppException(key + " is bigger than the Maximum requirement");
	            }
	            
	            if(type.equals("java.lang.String")) {
	            	if (!(o instanceof String))
	            		throw new DBAppException(key + " has a wrong type");
	            	
	            	String s = (String) o;
	            	if(s.length()<min.length())
	            		throw new DBAppException(key + " is smaller than the Minimum requirement");
	            	if(s.length() > max.length())
	            		throw new DBAppException(key + " is bigger than the Maximum requirement");
	            }
	            
	            if(type.equals("java.util.Date")) {
	            	if (!(o instanceof Date))
	            		throw new DBAppException(key + " has a wrong type");
	            	
	            	Date d = (Date) o;
	            	Date minDate=new SimpleDateFormat("yyyy-MM-dd").parse(min);  
	            	Date maxDate=new SimpleDateFormat("yyyy-MM-dd").parse(max);  
	            	if(d.compareTo(minDate)<0)
	            		throw new DBAppException(key + " is smaller than the Minimum requirement");
	            	if(d.compareTo(maxDate)>0)
	            		throw new DBAppException(key + " is bigger than the Maximum requirement");
	            }
	            	
	            
	   
	            
	            
	           
	            	
	            	
		 }
		 ArrayList<OctreePoint> points= createOctreePoints(strTableName, htblColNameValue);
		 int pageIndex =0;
		 if(points.size()==0) { //no index
			//SEARCH FOR PAGE INDEX AND ROW
				pageIndex = searchForPage(strTableName,strClusteringKeyValue) ;
				
				
		 }
		 else {
			 for(OctreePoint point : points) {
			 ArrayList<OctreeNode> nodes =updateIndices(strTableName, point, htblColNameValue);
			     for(OctreeNode node : nodes) {
			    	 OctreePoint thePoint = comparePoints(point, node);
			    	 pageIndex= thePoint.getPage().getPageNumber();
			    	 
			     }
			 }
		 }
		
		//UPDATE IT
		Object keyValue= stringConverter(strClusteringKeyValue,strTableName); //CONVERTS KEY STRING TO AN OBJECT TO BE COMPARABLE 
		int rowIndex = binarySearchRow(strTableName, pageIndex, keyValue);
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
		//MAKE SURE THAT THE TABLE EXISTS -> VALIDATION
		File f = new File("src/resources/" + strTableName + "Table.class"); 
		 boolean e2 = f. exists();
		 if(e2==false) {
			 throw new DBAppException("Table does not exist");
		 }
		 
		 //MAKE SURE THERE IS ATLEAST ONE PAGE OF TABLE
		 File f2 = new File("src/resources/" + strTableName + "1.class"); 
		 boolean e3 = f2. exists();
		 if(e3==false) {
			 throw new DBAppException("Table does not contain any tuples to delete");
		 }
		 
		//CASE 1: WE WANT TO DELETE JUST ONE ROW AND THE INPUT GAVE US THE PRIMARY KEY VALUE
			//GET THE KEY FIRST
		String keyName = "";
		try {
	        
        	BufferedReader br = new BufferedReader(new FileReader("src/metadata.csv"));
        	String line="";
        	while ((line = br.readLine()) != null) {
               String[] values = line.split(",");
               if (values[0].equals(strTableName)) {	
            	   if (values[3].equals("True")) {
            		   keyName = values[1];
            		   
            	   }
               }
            	   
            		   
               
               
        	}
        	
        }
        	catch (IOException e) {
                e.printStackTrace();
             
            }
		
		if (htblColNameValue.get(keyName) !=null) {
			Object key = htblColNameValue.get(keyName);
			String keyValue = "";    
	 		if(key instanceof Date) {  
	 			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");  
	 			keyValue = dateFormat.format(key);  
	 		}
	 		else {
	 			keyValue=key.toString();
	 		}
	 		try {
	 			
	 			 ArrayList<OctreePoint> points= createOctreePoints(strTableName, htblColNameValue);
	 			 int pageIndex =0;
	 			 if(points.size()==0) { //no index
	 				//SEARCH FOR PAGE INDEX AND ROW
	 					pageIndex = searchForPage(strTableName,keyValue) ;
	 					
	 					
	 			 }
	 			 else {
	 				 for(OctreePoint point : points) {
	 					 //MAKE A HELPER METHOD FOR DELETING INDICES AND GETTING THE ARRAYLIST OF NODES
	 				 /*ArrayList<OctreeNode> nodes =updateIndices(strTableName, point);
	 				     for(OctreeNode node : nodes) {
	 				    	 OctreePoint thePoint = comparePoints(point, node);
	 				    	 pageIndex= thePoint.getPage().getPageNumber();
	 				    	 node.getPoints().remove(thePoint);
	 				    	 
	 				     }*/
	 				 }
	 			 }
	 			
	 			
	 			
	 			
	 			
	 			
	 			
				Object keyValueObject= stringConverter(keyValue,strTableName); //CONVERTS KEY STRING TO AN OBJECT TO BE COMPARABLE
				int rowIndex = binarySearchRow(strTableName, pageIndex, keyValueObject);
				//I FOUND THE KEY 
				FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + pageIndex + ".class");
				 ObjectInputStream in = new ObjectInputStream(fileIn) ;
				 Page p = (Page) in.readObject();
				 
				
				 p.getRows().remove(rowIndex);
				 int numberofRows = p.getRows().size();
				 FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + pageIndex + ".class");
			     ObjectOutputStream out = new ObjectOutputStream(fileOut);
			     out.writeObject(p);
			     out.close();
			     fileOut.close(); 
			     fileIn.close();
				 in.close();
 
				 int latestPage = findLatestPage(strTableName);
				 int originalPageIndex = pageIndex;
        
			     if (numberofRows==0) {	
			    	 FileInputStream tableFileIn = new FileInputStream("src/resources/" + strTableName + "Table.class");
					 ObjectInputStream tableIn = new ObjectInputStream(tableFileIn);
					 Table t = (Table) tableIn.readObject();
					 t.getPageDirectories().remove("src/resources/" + strTableName + p.getPageNumber() + ".class");
					 FileOutputStream tablefileOut = new FileOutputStream("src/resources/" + strTableName + "Table.class");
			            ObjectOutputStream tableout = new ObjectOutputStream(tablefileOut);
			            tableout.writeObject(t);
			            tableout.close();
			            tablefileOut.close();
			            tableFileIn.close();
			            tableIn.close();
					 
			    	 Files.delete(Paths.get("src/resources/" + strTableName + originalPageIndex + ".class"));
					  	 }   
			     
			   

			     
				 while (latestPage > pageIndex) {
					//IF TRUE, IT MEANS WE HAVE TO DO THE PAGE SHIFTING 
					 FileInputStream fileIn2 = new FileInputStream("src/resources/" + strTableName + (pageIndex+1) + ".class");
					 ObjectInputStream in2 = new ObjectInputStream(fileIn2) ;
					 Page p2 = (Page) in2.readObject(); 
					 Row row = p2.getRows().get(0);
					 p2.getRows().remove(0);
					 FileOutputStream fileOut2 = new FileOutputStream("src/resources/" + strTableName + (pageIndex+1) + ".class");
				        ObjectOutputStream out2 = new ObjectOutputStream(fileOut2);
				        out2.writeObject(p2);
				        out2.close();
				        fileOut2.close();
				        fileIn2.close();
				        in2.close();
					 if (p2.getRows().size()==0) {
						 FileInputStream tableFileIn = new FileInputStream("src/resources/" + strTableName + "Table.class");
						 ObjectInputStream tableIn = new ObjectInputStream(tableFileIn);
						 Table t = (Table) tableIn.readObject();
						 t.getPageDirectories().remove("src/resources/" + strTableName + p2.getPageNumber() + ".class");
						 FileOutputStream tablefileOut = new FileOutputStream("src/resources/" + strTableName + "Table.class");
				            ObjectOutputStream tableout = new ObjectOutputStream(tablefileOut);
				            tableout.writeObject(t);
				            tableout.close();
				            tablefileOut.close();
				            tableFileIn.close();
				            tableIn.close();
						 Files.delete(Paths.get("src/resources/" + strTableName + p2.getPageNumber()  + ".class"));
						 
					 }
					 insertIntoTable(strTableName, row.getColNameValue());
					
					 pageIndex++;
					 
				 } 
				  
	 		}
			catch(DBAppException app) {
				app.printStackTrace();
				
				
			}
	 		catch(Exception e){
	 			e.printStackTrace();
	 		}
		}
		
		//CASE 2: WE WANT TO DELETE MULTIPLE ROWS 
		else {
			int counter=1;
			int latest = findLatestPage(strTableName);
			if(latest==0)
				return;
			
		
				while(counter<=latest){
					if(findLatestPage(strTableName)==0)
						return;
					try {
						FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + counter + ".class");
						 ObjectInputStream in = new ObjectInputStream(fileIn) ;
						 Page p = (Page) in.readObject();
						 FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + counter + ".class");
					        ObjectOutputStream out = new ObjectOutputStream(fileOut);
					        out.writeObject(p);
					        out.close();
					        fileOut.close();  
					        fileIn.close();
					        in.close();
					       
						 for(Row row : p.getRows()) {
							 String key = "";
							 Enumeration<String> enumerator = htblColNameValue.keys();
							 boolean conditionSatisfied = false;
							 while (enumerator.hasMoreElements()) {
					                key = enumerator.nextElement();
					                if (row.getColNameValue().get(key).equals(htblColNameValue.get(key))) {
					                	conditionSatisfied = true;
					                	
					                }
					                else {
					                	conditionSatisfied = false;
					                	break;
					                }
					                	
					                	
							 }

							 if(conditionSatisfied) {
								
								 
								 counter=0;
								 deleteFromTable(strTableName, row.getColNameValue());
								 //break;
								 
							
								 
							 }
						 }
						 
					        
					}
					catch(Exception e) {
						
					}
					counter++;
				}
				
		
			
		}
		
	}
	
	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException, IOException, ClassNotFoundException, ParseException {
		//MAKE SURE THAT THE TABLE EXISTS -> VALIDATION
		File f = new File("src/resources/" + strTableName + "Table.class"); 
		boolean e2 = f. exists();
		if(e2==false) {
			throw new DBAppException("Table does not exist");
		}
		//MAKE SURE THAT THE COLUMNS ARE THREE
		if (strarrColName.length != 3) {
			throw new DBAppException("You need to insert 3 column names");
		}
		//MAKE SURE THE COLUMNS EXIST
		/////////////////////////////
		
		//Create index
		 FileInputStream tableFileIn = new FileInputStream("src/resources/" + strTableName + "Table.class");
		 ObjectInputStream tableIn = new ObjectInputStream(tableFileIn);
		 Table t = (Table) tableIn.readObject();
		 FileOutputStream tableFileOut = new FileOutputStream("src/resources/" + strTableName + "Table.class");
	     ObjectOutputStream tableOut = new ObjectOutputStream(tableFileOut);
	     tableOut.writeObject(t);
	     tableOut.close();
	     tableFileOut.close();
	     
	     String xMin = t.getHtblColNameMin().get(strarrColName[0]);
	     String xMax = t.getHtblColNameMax().get(strarrColName[0]);
	     String yMin = t.getHtblColNameMin().get(strarrColName[1]);
	     String yMax = t.getHtblColNameMax().get(strarrColName[1]);
	     String zMin = t.getHtblColNameMin().get(strarrColName[2]);
	     String zMax = t.getHtblColNameMax().get(strarrColName[2]);
	     
		 OctreeNode root = new OctreeNode(xMin, yMin, zMin, xMax, yMax, zMax, maximumEntriesinOctreeNode);
		 OctreeIndex index = new OctreeIndex(root,strarrColName[0],strarrColName[1],strarrColName[2]);
		 
		 int num = findLatestIndex(strTableName);
		 num++;
		 
		 try {
			 File indexfile = new File("src/resources/" + strTableName + "Index" +  num +  ".class");
	         indexfile.createNewFile();
	         FileOutputStream indexfileOut = new FileOutputStream("src/resources/" + strTableName + "Index" +  num +  ".class");
	         ObjectOutputStream indexout = new ObjectOutputStream(indexfileOut);
	         indexout.writeObject(index);
	         indexout.close();
	         indexfileOut.close();
		 }
		 catch(Exception e) {
			 
		 }
		 
         
         if (!(t.getPageDirectories().isEmpty())) {
        	 populateIndex(strTableName, index,num);
         }
         
         ///UPDATE METADATA FILE
         String indexName = strarrColName[0] + strarrColName[1] + strarrColName[2] + "Index";
         try {
 	        
         	BufferedReader br = new BufferedReader(new FileReader("src/metadata.csv"));
         	File tempFile = new File("metadata.csv");
            BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
         	String line;
         	while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values[0].equals(strTableName))
             	   for (int i =0; i<strarrColName.length ; i++) {
             		  if (values[1].equals(strarrColName[i])) {
             			  values[4] = indexName;
             			  values[5] = "Octree";
             			  
             			  break;
             		  }
             	   }
                writer.write(String.join(",", values));
                writer.newLine();
               
         	}
         	br.close();
            writer.close();
            tempFile.createNewFile();
            //File originalFile = new File("src/metadata.csv");
            //tempFile.renameTo(originalFile);
           // Path source = Paths.get("src/metadata.csv");
            //Path target = Paths.get("metadata.csv");
            //Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
                
            
            
         }
         	catch (IOException e) {
                 e.printStackTrace();
             }
         
	}
	
	
	public void populateIndex(String strTableName, OctreeIndex index, int indexNum) throws IOException, ClassNotFoundException, ParseException , FileNotFoundException{
		//int num = 1;
		File file = new File("src/resources/");
		File[] list = file.listFiles();
		for (int i =1;i<=findLatestPage(strTableName) ;i++)
		for (File fil : list) { //to loop on all files looking for the pages of that table
			if (fil.getName().equals(strTableName + i + ".class")){
				FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + i + ".class");
				ObjectInputStream in = new ObjectInputStream(fileIn) ;
				Page p = (Page) in.readObject();
				FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + i + ".class");
			    ObjectOutputStream out = new ObjectOutputStream(fileOut);
			    out.writeObject(p);
			    out.close();
			    fileOut.close();
			    fileIn.close();
			    in.close();
			    Vector<Row> rows = p.getRows();
			    for(Row r : rows) { //to loop on all rows in each page
			    	OctreePoint newPoint = new OctreePoint(index.getCol1(),index.getCol2(),index.getCol3(),p);
			    	index.getRoot().addPoint(newPoint);
			    }
			}	
			
			 
			
		}
		 try {
			 File indexfile = new File("src/resources/" + strTableName + "Index" +  indexNum +  ".class");
	         FileOutputStream indexfileOut = new FileOutputStream("src/resources/" + strTableName + "Index" +  indexNum +  ".class");
	         ObjectOutputStream indexout = new ObjectOutputStream(indexfileOut);
	         indexout.writeObject(index);
	         indexout.close();
	         indexfileOut.close();
		 }
		 catch(Exception e) {
			 
		 }
		
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
 	
	public static int findLatestIndex (String tableName) {
 		int num = 1;
		File file = new File("src/resources/");
		File[] list = file.listFiles();
		for (File fil : list) {
			if (fil.getName().equals(tableName + "Index" + num + ".class")){
				num++;
			}		
		}
		num--;
		return num;
		
	} 
 	
 	public static void binarySearchInsert(String strTableName, Page p, Hashtable<String,Object> htblColNameValue, Object valueToCompareWith) throws DBAppException{
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
 		if(p.getRows().size()>mid &&compare(valueToCompareWith,p.getRows().get(mid).getColNameValue().get(clusteringKey))>0) {
 			first = mid + 1;
 			
 		}
 		else if(p.getRows().size()>mid &&(compare(valueToCompareWith, p.getRows().get(mid).getColNameValue().get(clusteringKey))==0) ) {
 			throw new DBAppException("Cannot insert a value with a duplicate clustering key");
 			
 		}
 		else if(p.getRows().size()>mid &&(compare(valueToCompareWith, p.getRows().get(mid).getColNameValue().get(clusteringKey))==0) 
 				||p.getRows().size()>mid &&(compare(valueToCompareWith, p.getRows().get(mid).getColNameValue().get(clusteringKey))<0 && compare(valueToCompareWith,p.getRows().get(mid-1).getColNameValue().get(clusteringKey))>0) ){
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
 	
 	public static int searchForPage(String strTableName, String key) throws IOException, ParseException, ClassNotFoundException, DBAppException {
 		
 		String type = "";
 		Object keyValue = null;
 		int pageIndex =0;
 		
	try {
	        
        	BufferedReader br = new BufferedReader(new FileReader("src/metadata.csv"));
        	String line;
        	while ((line = br.readLine()) != null) {
               String[] values = line.split(",");
               if (values[0].equals(strTableName))
            	   if (values[3].equals("True"))
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
 			Date date1=new SimpleDateFormat("yyyy-MM-dd").parse(key);  
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
			 fileIn.close();
	         in.close();
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
				 if(compare(keyValue,lastValueFromRows)>0 ) {
					 
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
			 
				 
			 
		 }  //TRY 
	   } 
			 else {
				 throw new DBAppException("Row does not exist");
			 }
	}
		
		return pageIndex;
 }

 	public static int binarySearchRow(String strTableName, int pageIndex, Object key) throws IOException, ClassNotFoundException, DBAppException {
 		
 		 FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + pageIndex + ".class");
		 ObjectInputStream in = new ObjectInputStream(fileIn) ;
		 Page p = (Page) in.readObject();
		 FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + pageIndex + ".class");
	        ObjectOutputStream out = new ObjectOutputStream(fileOut);
	        out.writeObject(p);
	        out.close();
	        fileOut.close();
	        fileIn.close();
	        in.close();
		 int rowIndex =0;
 		String clusteringKey = p.getClusteringKey();
 		
 		int first=0;
 		int last=p.getRows().size();
 		int mid=(first + last)/2;
 	
 		
 		while(first<=last) {
 		if(p.getRows().size()>mid && compare(key,p.getRows().get(mid).getColNameValue().get(clusteringKey))>0) {
 			first = mid + 1;
 		}else if(p.getRows().size()>mid &&(compare(key, p.getRows().get(mid).getColNameValue().get(clusteringKey))==0)){
 			rowIndex=mid;
 	
 			break;
 			
 		}else {
 			last = mid - 1;
 		}
 		    mid = (first + last)/2;
 		}	
 		
 		
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
            	   if (values[3].equals("True"))
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
 			Date date1=new SimpleDateFormat("yyyy-MM-dd").parse(key);  
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
 	
 	public ArrayList<OctreeNode> updateIndices(String strTableName, OctreePoint point, Hashtable<String,Object> htblColNameValue) throws IOException, ClassNotFoundException, ParseException {
 		//this method takes an octree point with a dummy reference 
 		int num = 1;
 		ArrayList<OctreeNode> nodes = null;
		File file = new File("src/resources/");
		File[] list = file.listFiles();
		//loop to get all indices
		for (File fil : list) {
			if (fil.getName().equals(strTableName + "Index" + num + ".class")){
				//we found an index, unserialise it and get root node
				FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + "Index" + num + ".class");
				ObjectInputStream in = new ObjectInputStream(fileIn) ;
				OctreeIndex o = (OctreeIndex) in.readObject();
				OctreeNode currentNode = o.getRoot();
				FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + "Index" + num + ".class");
		        ObjectOutputStream out = new ObjectOutputStream(fileOut);
		        out.writeObject(o);
		        out.close();
		        fileOut.close();
		        fileIn.close();
		        in.close();
				//get the node where this point should be located in
				boolean nodeFound = false;
				//if there are no children, then we found the node
				if(currentNode.getChildren().length==0) {
					nodeFound = true;
					nodes.add(currentNode);
					
				}
				else { //find the child node where the point should be located in
					while (nodeFound == false) {
						int newNodeIndex = currentNode.getIndex(point);
						OctreeNode[] children = currentNode.getChildren();
						currentNode = children[newNodeIndex];
					}
				}
				OctreePoint thePoint = comparePoints(point, currentNode);
				thePoint.setX(htblColNameValue.get(o.getCol1()));
				thePoint.setY(htblColNameValue.get(o.getCol2()));
				thePoint.setZ(htblColNameValue.get(o.getCol3()));
				
				FileOutputStream fileOut2 = new FileOutputStream("src/resources/" + strTableName + "Index" + num + ".class");
		        ObjectOutputStream out2 = new ObjectOutputStream(fileOut2);
		        out2.writeObject(o);
		        out2.close();
		        fileOut2.close();
				
				
				num++; //to look for the nodes of other indices
			}		
		}
		
		
		
		
		
		return nodes;
 	}
 	
 	
 	public ArrayList<OctreePoint> createOctreePoints(String strTableName, Hashtable<String,Object> htblColNameValue) throws IOException, ClassNotFoundException{
 		ArrayList<OctreePoint> points=null;
 		int n = findLatestIndex(strTableName);
 		for(int i=1;i<=n;i++) {
 			FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + "Index" + i + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn) ;
			OctreeIndex o = (OctreeIndex) in.readObject();
			Object col1 = htblColNameValue.get(o.getCol1());
			Object col2 = htblColNameValue.get(o.getCol2());
			Object col3 = htblColNameValue.get(o.getCol3());
			OctreePoint p = new OctreePoint(col1,col2,col3,null);
			points.add(p);
			FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + "Index" + i + ".class");
	        ObjectOutputStream out = new ObjectOutputStream(fileOut);
	        out.writeObject(o);
	        out.close();
	        fileOut.close();
	        fileIn.close();
	        in.close();
 		}
 		return points;
 	}
 	
 	public OctreePoint comparePoints(OctreePoint dummyPoint, OctreeNode node) {
 		for(OctreePoint p: node.getPoints()) {
 			if( (compare(p.getX(),dummyPoint.getX())==0)  &&  (compare(p.getY(),dummyPoint.getY())==0) && (compare(p.getZ(),dummyPoint.getZ())==0) ) {
 				return p;
 			}
 		}
 		return null;
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
		dbApp.createTable( strTableName, "id", htblColNameType , htblColNameMin , htblColNameMax); */
		
	/*	DBApp dbApp = new DBApp( );
		String[] columns = {"id","gpa", "name"};
		
		Hashtable table = new Hashtable( );
		table.put("id",0);
		table.put("name","mina");
		table.put("gpa",0);
		dbApp.insertIntoTable("Student", table);
		
		Hashtable table2 = new Hashtable( );
		table2.put("id",1);
		table2.put("name","shahd");
		table2.put("gpa",0);
		dbApp.insertIntoTable("Student", table2);
		
		Hashtable table3 = new Hashtable( );
		table3.put("id",2);
		table3.put("name","bosbos");
		table3.put("gpa",0);
		dbApp.insertIntoTable("Student", table3);
		
		
	    dbApp.createIndex("Student", columns);*/
		DBApp dbApp = new DBApp( );
		String[] columns = {"id","gpa", "name"};
		dbApp.createIndex("Student", columns);
		 
		
		try {
			 
				FileInputStream fileIn;
				fileIn = new FileInputStream("src/resources/StudentIndex1.class");
				ObjectInputStream in = new ObjectInputStream(fileIn);
		         OctreeIndex o = (OctreeIndex) in.readObject();
		         in.close();
		         fileIn.close();
		         System.out.println(o.getRoot().getPoints().size());
		         
		         
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
		
	
	 
		
		
		
		
    }
	


}
	