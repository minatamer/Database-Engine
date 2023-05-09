public class OctreePoint implements java.io.Serializable {
    private Object x;
    private Object y;
    private Object z;
    private Page page;
	public OctreePoint(Object x, Object y, Object z, Page page) {
		this.x = x;
		this.y = y;
		this.z = z;
		this.page = page;
	}
	public Object getX() {
		return x;
	}
	public void setX(Object x) {
		this.x = x;
	}
	public Object getY() {
		return y;
	}
	public void setY(Object y) {
		this.y = y;
	}
	public Object getZ() {
		return z;
	}
	public void setZ(Object z) {
		this.z = z;
	}
	public Page getPage() {
		return page;
	}
	public void setPage(Page page) {
		this.page = page;
	}


}