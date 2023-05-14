import java.util.Hashtable;

public class Row implements java.io.Serializable   {
	
	private static final long serialVersionUID = -6958550135317905168L;
	private String pageName;
	private Hashtable<String, Object> colNameValue;

	
	public Row(String pageName, Hashtable<String, Object> colNameValue) {
		this.pageName = pageName;
		this.colNameValue = colNameValue;
	}
	
	




	public String getPageName() {
		return pageName;
	}






	public void setPageName(String pageName) {
		this.pageName = pageName;
	}






	public Hashtable<String, Object> getColNameValue() {
		return colNameValue;
	}






	public void setColNameValue(Hashtable<String, Object> colNameValue) {
		this.colNameValue = colNameValue;
	}






	public String toString() {
	        return colNameValue.toString();
	    }
	
	  @Override
	    public boolean equals(Object obj) {
		  //System.out.println("im usign this");
		  if (!(obj instanceof Row)) {
	            return false;
	        }
		  Row row = (Row) obj;
	        if (compareHashtable(this.getColNameValue() , row.getColNameValue())) {
	            return true;
	        }
	        else
	        	return false;
	        
	      
	    } 
	  
	  public static boolean compareHashtable(Hashtable<String, Object> ht1, Hashtable<String, Object> ht2) {
		    // Check if the two hash tables have the same keys
		    if (!ht1.keySet().equals(ht2.keySet())) {
		        return false;
		    }
		    
		    // Check the values for each key
		    for (String key : ht1.keySet()) {
		        Object value1 = ht1.get(key);
		        Object value2 = ht2.get(key);
		        if (value1 == null && value2 == null) {
		            continue;
		        }
		        if (value1 == null || value2 == null) {
		            return false;
		        }
		        if (!value1.equals(value2)) {
		            return false;
		        }
		    }
		    
		    // The hash tables are equal
		    return true;
		}
	

}
