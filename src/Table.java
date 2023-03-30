import java.util.Hashtable;
import java.util.Vector;

public class Table implements java.io.Serializable {
	 
	private Vector<String> pageDirectories;
     
     public Table(){
    	 pageDirectories = new Vector<String>();
    	 
     }

	public Vector<String> getPageDirectories() {
		return pageDirectories;
	}

	public void setPageDirectories(Vector<String> pageDirectories) {
		this.pageDirectories = pageDirectories;
	}

    

}
                   