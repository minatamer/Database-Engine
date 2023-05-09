public class OctreeIndex implements java.io.Serializable {
    private OctreeNode root;
    private String col1;
    private String col2;
    private String col3;

	public OctreeIndex(OctreeNode root, String col1, String col2, String col3) {
		this.root = root;
		this.col1 = col1;
		this.col2 = col2;
		this.col3 = col3;
	}
	
    public OctreeNode getRoot() {
		return root;
	}

	public void setRoot(OctreeNode root) {
		this.root = root;
	}

	public String getCol1() {
		return col1;
	}

	public void setCol1(String col1) {
		this.col1 = col1;
	}

	public String getCol2() {
		return col2;
	}

	public void setCol2(String col2) {
		this.col2 = col2;
	}

	public String getCol3() {
		return col3;
	}

	public void setCol3(String col3) {
		this.col3 = col3;
	}

	
    
   

}


