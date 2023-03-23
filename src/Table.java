import java.util.Hashtable;
import java.util.Vector;

public class Table implements java.io.Serializable {
	 
	private Hashtable<String, String> pageNameDirectory;
     
     public Table(){
    	 pageNameDirectory = new Hashtable<String, String>();
    	 
     }

	public Hashtable<String, String> getPageNameDirectory() {
		return pageNameDirectory;
	}

	public void setPageNameDirectory(Hashtable<String, String> pageNameDirectory) {
		this.pageNameDirectory = pageNameDirectory;
	}
     
     
    

}
                   