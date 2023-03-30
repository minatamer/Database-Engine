import java.util.Hashtable;

public class Row implements java.io.Serializable {
	

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
	

}
