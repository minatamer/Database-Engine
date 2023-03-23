import java.util.Hashtable;
import java.util.Vector;

public class Page implements java.io.Serializable  {
	
	private int pageNumber;
	private String clusteringKey;
	private String tableName;
	private Hashtable<String,String> htblColNameType;
	private Hashtable<String,String> htblColNameMin;
	private Hashtable<String,String> htblColNameMax;
	private Vector<Row> colNameValue;
	
	
	
	public Page(int pageNumber, String clusteringKey, String tableName, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) {
	    this.pageNumber = pageNumber;
		this.clusteringKey = clusteringKey;
		this.tableName = tableName;
		this.htblColNameType = htblColNameType;
		this.htblColNameMin = htblColNameMin;
		this.htblColNameMax = htblColNameMax;
		colNameValue = new Vector<Row>();
	}


	public int getPageNumber() {
		return pageNumber;
	}


	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}


	public String getClusteringKey() {
		return clusteringKey;
	}


	public void setClusteringKey(String clusteringKey) {
		this.clusteringKey = clusteringKey;
	}


	public String getTableName() {
		return tableName;
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}


	public Hashtable<String, String> getHtblColNameType() {
		return htblColNameType;
	}


	public void setHtblColNameType(Hashtable<String, String> htblColNameType) {
		this.htblColNameType = htblColNameType;
	}


	public Hashtable<String, String> getHtblColNameMin() {
		return htblColNameMin;
	}


	public void setHtblColNameMin(Hashtable<String, String> htblColNameMin) {
		this.htblColNameMin = htblColNameMin;
	}


	public Hashtable<String, String> getHtblColNameMax() {
		return htblColNameMax;
	}


	public void setHtblColNameMax(Hashtable<String, String> htblColNameMax) {
		this.htblColNameMax = htblColNameMax;
	}


	public Vector<Row> getColNameValue() {
		return colNameValue;
	}


	public void setColNameValue(Vector<Row> colNameValue) {
		this.colNameValue = colNameValue;
	}
	
	
	


	

}
