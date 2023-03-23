import java.util.Hashtable;

public class Row implements java.io.Serializable {
	
	//private String clusteringKey;
	//private String tableName;
	private String pageName;
	private Hashtable<String, Object> colNameValue;
	//private Hashtable<String, String> colNameMin;
	//private Hashtable<String, String> colNameMax;
	 
	 
	 
	 
	/* public Row(String clusteringKey, String tableName, Hashtable<String, String> colNameValue,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) {

		this.clusteringKey = clusteringKey;
		this.tableName = tableName;
		this.colNameValue = colNameValue;
		this.colNameMin = colNameMin;
		this.colNameMax = colNameMax;
	} */
	
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
