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
                		csvWriter.append("TRUE").append(",");
                	}
                	else {
                		csvWriter.append("FALSE").append(",");
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
            
            if (o==null || min==null || max==null || type==null ) {
            	throw new DBAppException("This column does not exist in the table: " + key);
            }
            
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
	 
	 
	 Enumeration<String> enumerator2 = hashtableColumns.keys();
	 while (enumerator2.hasMoreElements()) {
         String key = enumerator2.nextElement();
         if (htblColNameValue.get(key)==null)
        	 throw new DBAppException("You are missing a column value at column " + key);
	 
	 }
	 
	 Page pageReferenceForIndex = null;
	
	//CASE THE TABLE DOES NOT HAVE ANY PAGES AS IT IS A NEW TABLE 
	if (findLatestPage(strTableName)==0) {
		 FileInputStream tableFileIn = new FileInputStream("src/resources/" + strTableName + "Table.class");
		 ObjectInputStream tableIn = new ObjectInputStream(tableFileIn);
		 Table t = (Table) tableIn.readObject();
		 Page page= new Page(1, t.getClusteringKey(), strTableName , t.getHtblColNameType() , t.getHtblColNameMin() , t.getHtblColNameMax() );
		 t.getPageDirectories().add("src/resources/" + strTableName + 1 + ".class");
		 Row newRow = new Row(page.getTableName()+page.getPageNumber(), htblColNameValue);
		 page.getRows().add(newRow);
		 pageReferenceForIndex= page;
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
	     int numOfIndices = findLatestIndex(strTableName); 
		 ArrayList<OctreePoint> dummyPoints= createOctreePoints(strTableName, htblColNameValue);
		 if(numOfIndices!=0) { //this means that there are indices
				int numOfPointsPerIndex = dummyPoints.size()/numOfIndices;
				updateIndicesUponInsert(dummyPoints, numOfPointsPerIndex, strTableName, pageReferenceForIndex);
			}	 
	     return;
		
	}
	 
     
	
	
	
			int counter=1;
			boolean pageFound=false;
			while(pageFound==false) {
				//what if it can not be inputed in any of the pages? NEW PAGE.
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
			            pageReferenceForIndex= newPage;
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
					 pageReferenceForIndex= p;
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
					//CHECK FIRST IF IT IS A DUPLICATE KEY WITHIN THIS PAGE BEFORE MOVING ON TO ANOTHER PAGE
					 Row tempRow = new Row(p.getTableName()+p.getPageNumber(), htblColNameValue);
					 if(p.getRows().contains(tempRow))
						 throw new DBAppException("Cannot insert a value with a duplicate clustering key");
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
					 pageReferenceForIndex= p;
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
					 int numOfIndices = findLatestIndex(strTableName);
					 ArrayList<OctreePoint> dummyPoints= createOctreePoints(strTableName, htblColNameValue);
					 
					 //
					 
					 if(numOfIndices!=0) { //this means that there are indices
							int numOfPointsPerIndex = dummyPoints.size()/numOfIndices;
							updateIndicesUponDelete(dummyPoints, numOfPointsPerIndex, strTableName);
						}
					 insertIntoTable(strTableName, extraRow.getColNameValue());
					 FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + counter + ".class");
			         ObjectOutputStream out = new ObjectOutputStream(fileOut);
			         out.writeObject(p);
			         out.close();
			         fileOut.close(); 
			  }
				 
				 int numOfIndices = findLatestIndex(strTableName); 
				 ArrayList<OctreePoint> dummyPoints= createOctreePoints(strTableName, htblColNameValue);
				 if(numOfIndices!=0) { //this means that there are indices
						int numOfPointsPerIndex = dummyPoints.size()/numOfIndices;
						updateIndicesUponInsert(dummyPoints, numOfPointsPerIndex, strTableName, pageReferenceForIndex);
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
		 

		 int numOfIndices = findLatestIndex(strTableName);
		 ArrayList<OctreePoint> oldValuePoints= getOldValuePoints(strTableName, strClusteringKeyValue);
		 ArrayList<OctreePoint> newValuePoints= createOctreePoints(strTableName, htblColNameValue);
		 Object keyValue= stringConverter(strClusteringKeyValue,strTableName); //CONVERTS KEY STRING TO AN OBJECT TO BE COMPARABLE 
		 
		 //PUT THE CLUSTERING KEY IN THE POINT 
		 newValuePoints.get(0).setX(keyValue);
		 int pageIndex =1;
		 
		 if(numOfIndices!=0) { //this means that there are indices ,, get page index using index and update index aswell
				pageIndex = updateIndicesUponUpdate(strTableName, oldValuePoints , newValuePoints, numOfIndices);
			}
		 else
			 pageIndex = searchForPage(strTableName,strClusteringKeyValue) ;
		 
		 
		//UPDATE TABLE
	    //int pageIndex =0; 
	    //pageIndex = searchForPage(strTableName,strClusteringKeyValue) ;
		//Object keyValue= stringConverter(strClusteringKeyValue,strTableName); //CONVERTS KEY STRING TO AN OBJECT TO BE COMPARABLE 
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
	
	public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException, ParseException {
		//MAKE SURE THAT THE TABLE EXISTS -> VALIDATION
		File f = new File("src/resources/" + strTableName + "Table.class"); 
		 boolean e2 = f. exists();
		 if(e2==false) {
			 throw new DBAppException("Table does not exist");
		 }
		 

		 
		 //MAKE SURE THERE IS AT LEAST ONE PAGE OF TABLE
		 File f2 = new File("src/resources/" + strTableName + "1.class"); 
		 boolean e3 = f2. exists();
		 if(e3==false) {
			 throw new DBAppException("Table does not contain any tuples to delete");
		 }
		 
		 int pageIndex =1;
		//delete the point(s) from index nodes, if there are indices
		    int numOfIndices = findLatestIndex(strTableName);
		    ArrayList<OctreePoint> dummyPoints= createOctreePoints(strTableName, htblColNameValue);
			if(numOfIndices!=0) { //this means that there are indices	
				int numOfPointsPerIndex = dummyPoints.size()/numOfIndices;
				pageIndex = updateIndicesUponDelete(dummyPoints, numOfPointsPerIndex, strTableName);
			}
		
			
		//CASE 1: WE WANT TO DELETE JUST ONE ROW AND THE INPUT GAVE US THE PRIMARY KEY VALUE
			//GET THE KEY FIRST
		String keyName = "";
		boolean singleDelete = false;
		try {
	        
        	BufferedReader br = new BufferedReader(new FileReader("src/metadata.csv"));
        	String line="";
        	while ((line = br.readLine()) != null) {
               String[] values = line.split(",");
               if (values[0].equals(strTableName)) {	
            	   if (values[3].equals("TRUE")) {
            		   keyName = values[1];
            		   singleDelete = true;
            		   
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
	 			
	 			 
	 			//delete the row from the page 
	 		    //int pageIndex =0;
	 			if(singleDelete == true && numOfIndices==0)
	 				pageIndex = searchForPage(strTableName,keyValue) ;
	 			
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
					 
					 //REMOVE IT FROM INDEX
					
					 ArrayList<OctreePoint> dummyPoints2= createOctreePoints(strTableName, row.getColNameValue());
					 if(numOfIndices!=0) { //this means that there are indices
							int numOfPointsPerIndex = dummyPoints2.size()/numOfIndices;
							updateIndicesUponDelete(dummyPoints2, numOfPointsPerIndex, strTableName);
						}	 
					 
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
	     
	    /* String xMin = t.getHtblColNameMin().get(strarrColName[0]);
	     String xMax = t.getHtblColNameMax().get(strarrColName[0]);
	     String yMin = t.getHtblColNameMin().get(strarrColName[1]);
	     String yMax = t.getHtblColNameMax().get(strarrColName[1]);
	     String zMin = t.getHtblColNameMin().get(strarrColName[2]);
	     String zMax = t.getHtblColNameMax().get(strarrColName[2]); */
	     
	     
	     Object xMin = convertStringToObject(t.getHtblColNameMin().get(strarrColName[0]), t.getHtblColNameType().get(strarrColName[0]));
	     Object xMax = convertStringToObject(t.getHtblColNameMax().get(strarrColName[0]), t.getHtblColNameType().get(strarrColName[0]));
	     Object yMin = convertStringToObject(t.getHtblColNameMin().get(strarrColName[1]), t.getHtblColNameType().get(strarrColName[1]));
	     Object yMax= convertStringToObject(t.getHtblColNameMax().get(strarrColName[1]), t.getHtblColNameType().get(strarrColName[1]));
	     Object zMin= convertStringToObject(t.getHtblColNameMin().get(strarrColName[2]), t.getHtblColNameType().get(strarrColName[2]));
	     Object zMax= convertStringToObject(t.getHtblColNameMax().get(strarrColName[2]), t.getHtblColNameType().get(strarrColName[2]));
	     
	     
	     
		 OctreeNode root = new OctreeNode(xMin, yMin, zMin, xMax, yMax, zMax, maximumEntriesinOctreeNode);
		 OctreeIndex index = new OctreeIndex(root,strarrColName[0],strarrColName[1],strarrColName[2]);
		 
		 int num = findLatestIndex(strTableName);
		 num++;
		 
         
         if (!(t.getPageDirectories().isEmpty())) {
        	 populateIndex(strTableName, index,num);
         }
         
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
            File old = new File("src/metadata.csv");
            old.delete();
            tempFile.renameTo(old);   
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
			    	OctreePoint newPoint = new OctreePoint(r.getColNameValue().get(index.getCol1()),r.getColNameValue().get(index.getCol2()),r.getColNameValue().get(index.getCol3()),p);
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
	
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)throws DBAppException, IOException, ClassNotFoundException, ParseException {
		
		Vector<Row> resultSet = new Vector<Row>();
		Vector<Vector<Row>> setOfSets = new Vector<Vector<Row>>();
		//CHECK SQL TERM SIZE
		if (arrSQLTerms.length ==0 ) {
			throw new DBAppException("no sql terms were added");
		}
		//CHECK IF OPERATORS ARE >, >=, <, <=, != or = ONLY 
		for(SQLTerm sql : arrSQLTerms) {
			String operator = sql._strOperator;
			if (!(operator.equals(">") || operator.equals(">=") || operator.equals("<") || operator.equals("<=") || operator.equals("!=") || operator.equals("=")))
				throw new DBAppException("invalid operator used");	
		}
		
		//CHECK IF strarrOperators are AND OR OR XOR ONLY
		if (strarrOperators[0] != null) {
			for(String operator : strarrOperators) {
				if (!(operator.equals("AND") || operator.equals("OR") || operator.equals("XOR") ))
					throw new DBAppException("invalid operator used");	
			}
			
		}
		
		
		//CHECK TABLE(S) EXISTS AND THEIR PAGE DIRECTORIES > 0
		 
		 for(SQLTerm sql : arrSQLTerms) {
			 String strTableName = sql._strTableName;
			 File f = new File("src/resources/" + strTableName + "Table.class"); 
			 boolean e = f. exists();
			 
			 if(e==false) {
				 throw new DBAppException("Table does not exist");
			 }
			 else {
				 File f2 = new File("src/resources/" + strTableName + "1.class");
				 boolean e2 = f2.exists();
				 if(e2==false) {
					 throw new DBAppException("Table does not have pages");
				 }
			 }
		 }
		
		
		//CHECK IF COLUMN NAMES EXIST
		 for(SQLTerm sql : arrSQLTerms) {
			 String strTableName = sql._strTableName;
			 FileInputStream tableFileIn = new FileInputStream("src/resources/" + strTableName + "Table.class");
			 ObjectInputStream tableIn = new ObjectInputStream(tableFileIn);
			 Table t = (Table) tableIn.readObject();
			 tableFileIn.close();
			 tableIn.close();
			 
			 if (t.getHtblColNameType().get(sql._strColumnName) == null) 
				 throw new DBAppException("invalid column names"); 
		 }
		 
		 //CHECK IF THEY ALL HAVE SAME TABLE NAME
		 String tableName = arrSQLTerms[0]._strTableName;
		 for(SQLTerm sql : arrSQLTerms) {
			 if (!(sql._strTableName.equals(tableName)))
				 throw new DBAppException("not all queries are from the same table"); 
		 }
		
		
		
		//CASE IT IS JUST ONE QUERY
		if (arrSQLTerms.length == 1) {
			//CASE QUERY IS ON CLUSTERING KEY --> BINARY SEARCH IT 
			FileInputStream tableFileIn = new FileInputStream("src/resources/" + arrSQLTerms[0]._strTableName + "Table.class");
			 ObjectInputStream tableIn = new ObjectInputStream(tableFileIn);
			 Table t = (Table) tableIn.readObject();
			 tableFileIn.close();
			 tableIn.close();
			 String strKey = ""; 
			 if (arrSQLTerms[0]._strColumnName.equals(t.getClusteringKey()) && arrSQLTerms[0]._strOperator.equals("=")) {
				    
			 		if(arrSQLTerms[0]._objValue instanceof Date) {  
			 			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");  
			 			strKey = dateFormat.format(arrSQLTerms[0]._objValue);  
			 		}
			 		else {
			 			strKey=arrSQLTerms[0]._objValue.toString();
			 		}
				 	int pageIndex = searchForPage(arrSQLTerms[0]._strTableName,strKey) ;
					Object keyValue= stringConverter(strKey,arrSQLTerms[0]._strTableName); //CONVERTS KEY STRING TO AN OBJECT TO BE COMPARABLE 
					int rowIndex = binarySearchRow(arrSQLTerms[0]._strTableName, pageIndex, keyValue);
					
					FileInputStream fileIn = new FileInputStream("src/resources/" + arrSQLTerms[0]._strTableName + pageIndex + ".class");
					ObjectInputStream in = new ObjectInputStream(fileIn) ;
					Page p = (Page) in.readObject();
				    fileIn.close();
				    in.close();
				    Row row = p.getRows().get(rowIndex);
				    resultSet.add(row);
				 
			 }
			 else {
				//CASE QUERY NOT ON KEY
					for (int i=1; i<=findLatestPage(arrSQLTerms[0]._strTableName) ; i++) {
						FileInputStream fileIn = new FileInputStream("src/resources/" + arrSQLTerms[0]._strTableName + i + ".class");
						ObjectInputStream in = new ObjectInputStream(fileIn) ;
						Page p = (Page) in.readObject();
					    fileIn.close();
					    in.close();
					    Vector<Row> rows = p.getRows();
					    for(Row r : rows) {
					    	if(compareQuery(arrSQLTerms[0]._strOperator, r.getColNameValue().get(arrSQLTerms[0]._strColumnName),arrSQLTerms[0]._objValue))
					    		resultSet.add(r);
					    }
						
					}
				 
			 }
			
		}
		//CASE IT IS MORE THAN ONE QUERY
		else {
			//CHECK FIRST IF THERE IS AN INDEX
			int numOfIndices = findLatestIndex(arrSQLTerms[0]._strTableName); 
			 if(numOfIndices!=0) { //this means that there are indices
				 
					//CASE THERE ARE 3 COLUMNS OF INDEX IN THE QUEREIES WITH ANDS BETWEEN THEM, GET THEIR SPECIAL VECTOR OF ROWS AND STORE IT IN SET OF SETS, AND REMOVE THEM FROM arrSQLTerms
					for(int i=1; i<=numOfIndices ; i++) {
						
						FileInputStream fileIn = new FileInputStream("src/resources/" + arrSQLTerms[0]._strTableName + "Index" + i + ".class");
			 			ObjectInputStream in = new ObjectInputStream(fileIn) ;
			 			OctreeIndex index = (OctreeIndex) in.readObject();
			 			OctreeNode root = index.getRoot(); 
			 			
			 			String col1 = index.getCol1();
			 			String col2 = index.getCol2();
			 			String col3 = index.getCol3();
			 			
			 			int startOfIndex = 0;
			 			
			 			//making the Octreepoint and removing sqlQueries of the index
			 			Vector<SQLTerm> terms = new Vector<SQLTerm>();
			 			for(int j=0; j<arrSQLTerms.length ; j++ ) {	
			 				
			 				if (arrSQLTerms.length - j >= 3) {
			 					if (  ((arrSQLTerms[j]._strColumnName.equals(col1)) ||  (arrSQLTerms[j]._strColumnName.equals(col2)) ||  (arrSQLTerms[j]._strColumnName.equals(col3))) && strarrOperators[j].equals("AND") && arrSQLTerms[j]._strOperator.equals("=")  ){
									terms.add(arrSQLTerms[j]);	
									if (((arrSQLTerms[j+1]._strColumnName.equals(col1)) || (arrSQLTerms[j+1]._strColumnName.equals(col2)) ||  
											(arrSQLTerms[j+1]._strColumnName.equals(col3))) && !(arrSQLTerms[j+1]._strColumnName.equals(arrSQLTerms[j]._strColumnName)) && strarrOperators[j+1].equals("AND") && arrSQLTerms[j+1]._strOperator.equals("=")  ){
										terms.add(arrSQLTerms[j+1]);
										if (((arrSQLTerms[j+2]._strColumnName.equals(col1)) || (arrSQLTerms[j+2]._strColumnName.equals(col2)) ||  
												(arrSQLTerms[j+2]._strColumnName.equals(col3))) && !(arrSQLTerms[j+2]._strColumnName.equals(arrSQLTerms[j]._strColumnName))  && !(arrSQLTerms[j+2]._strColumnName.equals(arrSQLTerms[j+1]._strColumnName)) && arrSQLTerms[j+2]._strOperator.equals("=") ) {
											terms.add(arrSQLTerms[j+2]);
											startOfIndex = j;
											break;
											
										}else {
					 						terms.clear();}
									}else {
				 						terms.clear();}
										
		
								}
			 					else {
			 						terms.clear();
			 						
			 					}
			 					
			 				}
			 							
						}
			 			
			 	  //i have a vector of all the index sql terms , make the point
			 			if (terms.size()==3) {
			 				
			 				Hashtable table = new Hashtable();
				 			table.put(terms.get(0)._strColumnName, terms.get(0)._objValue);
				 			table.put(terms.get(1)._strColumnName, terms.get(1)._objValue);
				 			table.put(terms.get(2)._strColumnName, terms.get(2)._objValue);
				 			
				 			ArrayList<OctreePoint> pointList = createOctreePoints(terms.get(0)._strTableName, table);
				 			//point made, get the row
				 			OctreeNode node = searchForNode(pointList.get(0),root);
				 			OctreePoint foundPoint =  comparePoints(pointList.get(0), node);
				 			Page page = foundPoint.getPage();
				 			Vector<Row> tempSet = new Vector<Row>();
				 			for(Row row : foundPoint.getPage().getRows()) {
				 				if (compare(row.getColNameValue().get(col1), pointList.get(0).getX())==0 && compare(row.getColNameValue().get(col2), pointList.get(0).getY())==0 && compare(row.getColNameValue().get(col3), pointList.get(0).getZ())==0) {
				 					tempSet.add(row);
				 					break;
				 				
				 				}
				 						
				 			}
				 			setOfSets.add(tempSet);
				 			
				 			//REMOVE THESE SQL TERMS AND OPERATORS FROM THE ORIGINAL ARRAYS using startOfIndex j j+1 j+2 for terms ... j j+1 for operators
				 			Vector<SQLTerm> tempTerms = new Vector<SQLTerm>();
				 			Vector<String> tempOperator = new Vector<String>();
				 			
				 			for(int j=0; j<arrSQLTerms.length ; j++) {
				 				if(j!=startOfIndex && j!= startOfIndex+1 && j != startOfIndex+2)
				 					tempTerms.add(arrSQLTerms[j]);
				 						
				 			}
				 			
				 			for(int j=0; j<strarrOperators.length ; j++) {
				 				if(j!=startOfIndex && j!= startOfIndex+1)
				 					tempOperator.add(strarrOperators[j]);
				 						
				 			}
				 			
				 			SQLTerm[] tempTerms2 = new SQLTerm[arrSQLTerms.length-3];
				 			String[] tempOperators2 = new String[strarrOperators.length-2];
				 			
				 			for(int j=0; j<tempTerms.size() ; j++) {
				 				tempTerms2[j] = tempTerms.get(j);
				 			}
				 			
				 			for(int j=0; j<tempOperator.size() ; j++) {
				 				tempOperators2[j] = tempOperator.get(j);
				 			}
				 			
				 			
				 			arrSQLTerms = tempTerms2;
				 			strarrOperators = tempOperators2;
		
			 			}
		 			
					}
					
					
				}	
			
		
			//PROCESS EACH QUERY ALONE, GET ITS VECTOR OF ROWS, AND STORE IT IN setOfSets
			
			 
			FileInputStream tableFileIn = new FileInputStream("src/resources/" + tableName + "Table.class");
			 ObjectInputStream tableIn = new ObjectInputStream(tableFileIn);
			 Table t = (Table) tableIn.readObject();
			 tableFileIn.close();
			 tableIn.close();
			
			for(SQLTerm sqlQuery : arrSQLTerms) {
				//CASE QUERY IS ON CLUSTERING KEY --> BINARY SEARCH IT
				 if (sqlQuery._strColumnName.equals(t.getClusteringKey()) && sqlQuery._strOperator.equals("=")) {
					 	Vector<Row> tempSet = new Vector<Row>();
					 	String strKey = "";
					 	if(sqlQuery._objValue instanceof Date) {  
				 			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");  
				 			strKey = dateFormat.format(sqlQuery._objValue);  
				 		}
				 		else {
				 			strKey = sqlQuery._objValue.toString();
				 		}
					 	
					 	int pageIndex = searchForPage(sqlQuery._strTableName,strKey) ;
						Object keyValue= stringConverter(strKey,sqlQuery._strTableName); //CONVERTS KEY STRING TO AN OBJECT TO BE COMPARABLE 
						int rowIndex = binarySearchRow(sqlQuery._strTableName, pageIndex, keyValue);
						
				
						FileInputStream fileIn = new FileInputStream("src/resources/" +sqlQuery._strTableName + pageIndex + ".class");
						ObjectInputStream in = new ObjectInputStream(fileIn) ;
						Page p = (Page) in.readObject();
					    fileIn.close();
					    in.close();
					    Row row = p.getRows().get(rowIndex);
					    tempSet.add(row);
					    setOfSets.add(tempSet);
					    
					 
					   
					 
				 }
				 else {
					//CASE QUERY NOT ON KEY
					 Vector<Row> tempSet = new Vector<Row>();
						for (int i=1; i<=findLatestPage(sqlQuery._strTableName) ; i++) {
							FileInputStream fileIn = new FileInputStream("src/resources/" + sqlQuery._strTableName + i + ".class");
							ObjectInputStream in = new ObjectInputStream(fileIn) ;
							Page p = (Page) in.readObject();
						    fileIn.close();
						    in.close();
						    Vector<Row> rows = p.getRows();
						    for(Row r : rows) {
						    	if(compareQuery(sqlQuery._strOperator, r.getColNameValue().get(sqlQuery._strColumnName),sqlQuery._objValue))
						    		tempSet.add(r);
						    }
						    setOfSets.add(tempSet);
						    
						   
							
						}
					 
				 }
			}
			 
	
			
			// for(int i=0; i<strarrOperators.length ; i++) {
             int i=0;
				 while(i<strarrOperators.length)	{
				 if(strarrOperators[i].equals("XOR")) {
					 Vector<Row> set1 = setOfSets.get(i);
					 Vector<Row> set2 = setOfSets.get(i+1);
					 setOfSets.remove(i+1);
					 setOfSets.remove(i);
					 
					 
					 Vector<Row> result = queryProcessor(set1,set2, "XOR");
					 setOfSets.add(i, result);
					 strarrOperators= removeStringAtIndex(strarrOperators,i);
					
					 
				 }
				 else {
					 i++;
				 }

			 }
			 
			// for(int i=0; i<strarrOperators.length ; i++) {
				int j =0;	
				while(j<strarrOperators.length)	{
				 if(strarrOperators[j].equals("AND")) {
					 Vector<Row> set1 = setOfSets.get(j);
					 Vector<Row> set2 = setOfSets.get(j+1);
					 setOfSets.remove(j+1);
					 setOfSets.remove(j);
					 
					 
					 Vector<Row> result = queryProcessor(set1,set2, "AND");
					
					 
					 setOfSets.add(j, result);
					 strarrOperators= removeStringAtIndex(strarrOperators,j);
					 
					 
				 }
				 else {
					 j++;
				 }

			 }
			 
				
			// for(int i=0; i<strarrOperators.length ; i++) {
				int y=0;
				while(y<strarrOperators.length)	{
			
				 if(strarrOperators[y].equals("OR")) {
					 Vector<Row> set1 = setOfSets.get(y);
					 Vector<Row> set2 = setOfSets.get(y+1);
					 setOfSets.remove(y+1);
					 setOfSets.remove(y);
					 
					 
					 Vector<Row> result = queryProcessor(set1,set2, "OR");
					 setOfSets.add(y, result);
					 strarrOperators= removeStringAtIndex(strarrOperators,y);
					
				 }
				 else {
					 y++;
				 }

			 }
		}
		
		
		
		
		
		if (setOfSets.size()==1) {
			resultSet = setOfSets.get(0);
		}
		
		Iterator<Row> iterator = resultSet.iterator();
		return iterator;
		
	}
	

	

	
	
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
        	String line = "";
        	
        	while ((line = br.readLine()) != null) {
               String[] values = line.split(",");
             
               if (values[0].equals(strTableName)) {
            	   if (values[3].equals("TRUE")) {
            		 
            		   type = values[2];
            	   }
               }
            	  
            		   
               
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
 			Date date1=new SimpleDateFormat("yyyy-MM-dd").parse(key);  
 			keyValue = date1;
 		}
 		else{
 			keyValue = key;
 		}
 		return keyValue;
	
 	}
 	
 	public static int compare(Object o1, Object o2) {
 		
 		if(o1 == null || o2==null) {
 			return 0;
 		}
 		
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
 			if(num1.length()>num2.length()) {
   				return 1;
   				}
   			if(num1.length()<num2.length()) {
   				return -1;
   				}
 			return num1.compareTo(num2);
 		}
 			     
 		if(o1 instanceof Date) {
 		
 			Date num1 = (Date) o1;
 			Date num2 = (Date) o2;
 			return num1.compareTo(num2);
 		}
 		
 		return 0;
 	}
 	
 	
 	public int updateIndicesUponUpdate(String strTableName, ArrayList<OctreePoint> oldValuePoints, ArrayList<OctreePoint> newValuePoints, int numOfIndices) throws IOException, ClassNotFoundException, ParseException {
 		int pageIndex = 1;
 		for(int i=0;i<numOfIndices;i++) { //loop on all indices
 			//1- unserialise root
 			FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + "Index" + (i+1) + ".class");
 			ObjectInputStream in = new ObjectInputStream(fileIn) ;
 			OctreeIndex index = (OctreeIndex) in.readObject();
 			OctreeNode root = index.getRoot(); 
 			
 			//2- reach node
 			OctreeNode currentNode = searchForNode(oldValuePoints.get(i),root);//gets the node where the oldValuePoint should be
 			for(int k=0;k<currentNode.getPoints().size();k++) { //OctreePoint nodePoint: currentNode.getPoints()
 		 		OctreePoint nodePoint = currentNode.getPoints().get(k);
 				if( (compare(nodePoint.getX(),oldValuePoints.get(i).getX())==0) && (compare(nodePoint.getY(),oldValuePoints.get(i).getY())==0) && (compare(nodePoint.getZ(),oldValuePoints.get(i).getZ())==0) )   {
 		 			    //3- update	 point(s)
 					    Page p = nodePoint.getPage();
 					    newValuePoints.get(i).setPage(p);
 		 				currentNode.getPoints().remove(nodePoint);
 		 				root.addPoint(newValuePoints.get(i));
 		 				pageIndex = p.getPageNumber();
 		 				}	
 		 			}
 				
 			//4- serialise the root
 			FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + "Index" + (i+1) + ".class");
		    ObjectOutputStream out = new ObjectOutputStream(fileOut);
		    out.writeObject(index);
		    out.close();
		    fileOut.close();
		    fileIn.close();
		    in.close();
		    
 		}
 		return pageIndex;
 		
 		
 	}
 	
 	
 	public void updateIndicesUponInsert(ArrayList<OctreePoint> dummyPoints, int numOfPointsPerIndex, String strTableName, Page p) throws IOException, ParseException, ClassNotFoundException {
 		//int numOfIndices = dummyPoints.size()/numOfPointsPerIndex;
 		
 		for(int i=0;i<dummyPoints.size();i++) { //loop on all indices
 			//1- unserialise root
 			FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + "Index" + (i+1) + ".class");
 			ObjectInputStream in = new ObjectInputStream(fileIn) ;
 			OctreeIndex index = (OctreeIndex) in.readObject();
 			OctreeNode root = index.getRoot(); 
 			dummyPoints.get(i).setPage(p);
 			root.addPoint(dummyPoints.get(i));
 			//4- serialise the root
 			FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + "Index" + (i+1) + ".class");
		    ObjectOutputStream out = new ObjectOutputStream(fileOut);
		    out.writeObject(index);
		    out.close();
		    fileOut.close();
		    fileIn.close();
		    in.close();
		    
 		}
 		
 	}
 	
 	
 	
 	public int updateIndicesUponDelete(ArrayList<OctreePoint> dummyPoints, int numOfPointsPerIndex, String strTableName) throws IOException, ParseException, ClassNotFoundException {
 		//int numOfIndices = dummyPoints.size()/numOfPointsPerIndex;
 		int pageIndex=1;
 		for(int i=0;i<dummyPoints.size();i++) { //loop on all indices
 			//1- unserialise root
 			FileInputStream fileIn = new FileInputStream("src/resources/" + strTableName + "Index" + (i+1) + ".class");
 			ObjectInputStream in = new ObjectInputStream(fileIn) ;
 			OctreeIndex index = (OctreeIndex) in.readObject();
 			OctreeNode root = index.getRoot(); 
 			//2- reach node
 			for(int j=0;j<numOfPointsPerIndex;j++) {
 				OctreeNode currentNode = searchForNode(dummyPoints.get(j),root);//gets the node where this dummyPoint should be
 				for(int k=0;k<currentNode.getPoints().size();k++) { //OctreePoint nodePoint: currentNode.getPoints()
 		 			OctreePoint nodePoint= currentNode.getPoints().get(k);
 					if( (compare(nodePoint.getX(),dummyPoints.get(j).getX())==0) && (compare(nodePoint.getY(),dummyPoints.get(j).getY())==0) && (compare(nodePoint.getZ(),dummyPoints.get(j).getZ())==0) )   {
 		 			    //3- delete point(s)
 						pageIndex = nodePoint.getPage().getPageNumber();
 		 				currentNode.getPoints().remove(nodePoint);
 		 				}	
 		 			}
 				}
 			
 			for(int k=0;k<numOfPointsPerIndex;k++) {
 				dummyPoints.remove(k); //remove these dummyPoints to avoid looping back on them
 			}
 			//4- serialise the root
 			FileOutputStream fileOut = new FileOutputStream("src/resources/" + strTableName + "Index" + (i+1) + ".class");
		    ObjectOutputStream out = new ObjectOutputStream(fileOut);
		    out.writeObject(index);
		    out.close();
		    fileOut.close();
		    fileIn.close();
		    in.close();
		    
 		}
 		
 		return pageIndex;
 		
 	}
 	
 	
 	public OctreeNode searchForNode(OctreePoint dummyPoint, OctreeNode root) throws ParseException {
 	    //get the node where this point should be located in
 		OctreeNode currentNode = root;
		boolean nodeFound = false;
		//if there are no children, then we found the node
		if(currentNode.getChildren().size()==0) {
			nodeFound = true;
		}
		else { //find the child node where the point should be located in
			while (nodeFound == false) {
				int newNodeIndex = currentNode.getIndex(dummyPoint);
				ArrayList <OctreeNode> children = currentNode.getChildren();
				currentNode = children.get(newNodeIndex);
				return searchForNode(dummyPoint,currentNode);
					
			}
		}
		
		return currentNode;
 	}
 	
 	
 	
 	
 	

	public ArrayList<OctreePoint> createOctreePoints(String strTableName, Hashtable<String,Object> htblColNameValue) throws IOException, ClassNotFoundException{
		ArrayList<OctreePoint> points = new ArrayList<OctreePoint>() ;
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

 	
	public boolean compareQuery(String comparisonOperator, Object o1 , Object o2) {
	

		boolean result = false;

		if (comparisonOperator.equals(">")) {
		    result = compare(o1, o2) > 0;
		} else if (comparisonOperator.equals(">=")) {
		    result = compare(o1, o2) >= 0;
		} else if (comparisonOperator.equals("<")) {
		    result = compare(o1, o2) < 0;
		} else if (comparisonOperator.equals("<=")) {
		    result = compare(o1, o2) <= 0;
		} else if (comparisonOperator.equals("!=")) {
		    result = compare(o1, o2) != 0;
		} else if (comparisonOperator.equals("=")) {
		    result = compare(o1, o2) == 0;
		}
		
		return result;
	}
 	
	
	public Vector<Row> queryProcessor (Vector<Row> rowSet1 , Vector<Row> rowSet2, String operator ){
		
		Vector<Row> resultSet = new Vector<Row>();
			
		if(operator.equals("OR")) {   //name = Noor OR gpa = 1.5
			for(Row row : rowSet1) {
				if (!rowSet2.contains(row))
					rowSet2.add(row);
			}
			resultSet = rowSet2;
			
		}
		
		if(operator.equals("AND")) {  //name = Noor AND gpa = 1.5
			for(Row row : rowSet1) {
				if (rowSet2.contains(row)) {
					resultSet.add(row);
					
					
				}
					
			}
			
			
			
		}
		
		if(operator.equals("XOR")) {  //name = Noor OR gpa = 1.5 BUT NOT BOTH
			
			//MEANING , the ORING of both sets minus the ANDING of both sets
			Vector<Row> andSet = new Vector<Row>();
			
			//Anding
			for(Row row : rowSet1) {
				if (rowSet2.contains(row))
					andSet.add(row);
			}
			
			//rowSet 2 now has the ORing of both sets
			for(Row row : rowSet1) {
				if (!rowSet2.contains(row))
					rowSet2.add(row);
			}

			for(Row row: andSet) {
				if(rowSet2.contains(row)) {
					rowSet2.remove(row);
				}
					
			}
			
			resultSet = rowSet2;
			
		}
		
		return resultSet;
		
		
		
	}
	
	public static Object convertStringToObject(String strValue, String strType) {
	    Object result = null;
	    switch (strType) {
	        case "java.lang.Integer":
	            result = Integer.valueOf(strValue);
	            break;
	        case "java.lang.Double":
	            result = Double.valueOf(strValue);
	            break;
	        case "java.lang.String":
	            result = strValue;
	            break;
	        case "java.util.Date":
	            try {
	                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	                Date date = formatter.parse(strValue);
	                result = date;
	            } catch (ParseException e) {
	                e.printStackTrace();
	            }
	            break;
	        default:
	            System.out.println("Invalid type: " + strType);
	            break;
	    }
	    return result;
	}
	
	public void printBlaBla () {
 		System.out.println("blabla");
 	}
	
	
	public static String[] removeStringAtIndex(String[] originalArray, int indexToRemove) {
	    String[] newArray = new String[originalArray.length - 1];
	    int destIndex = 0;
	    for (int srcIndex = 0; srcIndex < originalArray.length; srcIndex++) {
	        if (srcIndex != indexToRemove) {
	            newArray[destIndex++] = originalArray[srcIndex];
	        }
	    }
	    return newArray;
	}
	
	public ArrayList<OctreePoint> getOldValuePoints(String strTableName, String strClusteringKeyValue) throws ClassNotFoundException, IOException, ParseException, DBAppException{
		int pageIndex=searchForPage(strTableName, strClusteringKeyValue);
		Object keyValue= stringConverter(strClusteringKeyValue,strTableName); //CONVERTS KEY STRING TO AN OBJECT TO BE COMPARABLE 
		int rowIndex = binarySearchRow(strTableName, pageIndex, keyValue);
		FileInputStream fileIn;
		fileIn = new FileInputStream("src/resources/Student" + pageIndex + ".class");
		ObjectInputStream in = new ObjectInputStream(fileIn);
        Page p = (Page) in.readObject();
        in.close();
        fileIn.close();
		return createOctreePoints(strTableName, p.getRows().get(rowIndex).getColNameValue());
		
		
		
	}
	public void printPoints (ArrayList<OctreePoint> points) {
		for(OctreePoint p: points) {
			System.out.println(p.getX().toString() + " , " + p.getY() + " , " + p.getZ().toString());
		}
	}
	
	
	public static void main(String[] args) throws DBAppException, ClassNotFoundException, IOException, ParseException
    {
		
		
		DBApp dbApp = new DBApp( );
	           

		
	/*	Hashtable table1 = new Hashtable( );
		table1.put("id",10);
		table1.put("name","black");
		table1.put("gpa",1.0);
		dbApp.insertIntoTable("Student", table1);
		
		
		Hashtable table2 = new Hashtable( );
		table2.put("id",11);
		table2.put("name","white");
		table2.put("gpa",2.2);
		dbApp.insertIntoTable("Student", table2);  
		
		
		Hashtable table3 = new Hashtable( );
		table3.put("id",12);
		table3.put("name","blue");
		table3.put("gpa", 3.9); 
		dbApp.insertIntoTable("Student", table3);   */
		

   

		//String[] columns = {"id","gpa", "name"};
	   // dbApp.createIndex("Student", columns); 
	
		
		
		
		Hashtable table4 = new Hashtable( );
		//table4.put("id",2);
		//table4.put("name","bassel");
		table4.put("gpa",2.0);
		dbApp.deleteFromTable("Student",  table4);  
		
		

		
		try {
			 
				FileInputStream fileIn;
				fileIn = new FileInputStream("src/resources/StudentIndex1.class");
				ObjectInputStream in = new ObjectInputStream(fileIn);
		        OctreeIndex index = (OctreeIndex) in.readObject();
		        in.close();
		        fileIn.close();
		       // OctreeNode root = index.getRoot();
		        //ArrayList<OctreePoint> points=root.getChildren().get(0).getPoints();
		        //dbApp.printPoints(points);
		        
		        index.printAllPoints();
		     

		         
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}    
		
		
	

	    
		 try {
			 
			 FileInputStream fileIn;
			 fileIn = new FileInputStream("src/resources/Student1.class");
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
	