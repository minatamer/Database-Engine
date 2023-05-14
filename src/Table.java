

import java.util.Hashtable;
import java.util.Vector;

public class Table implements java.io.Serializable {
	 
	private Vector<String> pageDirectories;
	private Hashtable<String,String> htblColNameType;
	private Hashtable<String,String> htblColNameMin;
	private Hashtable<String,String> htblColNameMax;
	private String clusteringKey;
     
     public Table(){
    	 pageDirectories = new Vector<String>();
    	 
     }
     
     

	public Table(String clusteringKey, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) {
		
		pageDirectories = new Vector<String>();
		this.htblColNameType = htblColNameType;
		this.htblColNameMin = htblColNameMin;
		this.htblColNameMax = htblColNameMax;
		this.clusteringKey = clusteringKey;
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



	public String getClusteringKey() {
		return clusteringKey;
	}



	public void setClusteringKey(String clusteringKey) {
		this.clusteringKey = clusteringKey;
	}



	public Vector<String> getPageDirectories() {
		return pageDirectories;
	}

	public void setPageDirectories(Vector<String> pageDirectories) {
		this.pageDirectories = pageDirectories;
	}

    

}
                   