import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OctreeNode implements java.io.Serializable {
	
	  private Object xMin;
      private Object yMin;
      private Object zMin;
      private Object xMax;
      private Object yMax;
      private Object zMax;
      private ArrayList<OctreePoint> points;
      private OctreeNode[] children;
      private int maximumEntriesinOctreeNode ;

      
      public OctreeNode(Object xMin, Object yMin, Object zMin, Object xMax, Object yMax, Object zMax, int maximumEntriesinOctreeNode) {
          this.xMin = xMin;
          this.yMin = yMin;
          this.zMin = zMin;
          this.xMax = xMax;
          this.yMax = yMax;
          this.zMax = zMax;
          this.maximumEntriesinOctreeNode = maximumEntriesinOctreeNode;
          this.points = new ArrayList<OctreePoint>();
          this.children = new OctreeNode[8];
      }

      
      public Object getxMin() {
		return xMin;
	}


	public void setxMin(Object xMin) {
		this.xMin = xMin;
	}


	public Object getyMin() {
		return yMin;
	}


	public void setyMin(Object yMin) {
		this.yMin = yMin;
	}


	public Object getzMin() {
		return zMin;
	}


	public void setzMin(Object zMin) {
		this.zMin = zMin;
	}


	public Object getxMax() {
		return xMax;
	}


	public void setxMax(Object xMax) {
		this.xMax = xMax;
	}


	public Object getyMax() {
		return yMax;
	}


	public void setyMax(Object yMax) {
		this.yMax = yMax;
	}


	public Object getzMax() {
		return zMax;
	}


	public void setzMax(Object zMax) {
		this.zMax = zMax;
	}


	public ArrayList<OctreePoint> getPoints() {
		return points;
	}


	public void setPoints(ArrayList<OctreePoint> points) {
		this.points = points;
	}


	public OctreeNode[] getChildren() {
		return children;
	}


	public void setChildren(OctreeNode[] children) {
		this.children = children;
	}


	public int getMaximumEntriesinOctreeNode() {
		return maximumEntriesinOctreeNode;
	}


	public void setMaximumEntriesinOctreeNode(int maximumEntriesinOctreeNode) {
		this.maximumEntriesinOctreeNode = maximumEntriesinOctreeNode;
	}


	public void addPoint(OctreePoint point) throws ParseException {
          if (children[0] == null && points.size() < maximumEntriesinOctreeNode) {
              points.add(point);
          } else {
              if (children[0] == null) {
                  subdivide();
              }
              int index = getIndex(point);
              children[index].addPoint(point);
          }
      }
      
      public void subdivide() throws ParseException {
    	   	  
    	  	Object xMid = getMid(xMin, xMax);
    	  	Object yMid = getMid(yMin, yMax);
  	    	Object zMid = getMid(zMin, zMax);
    	    
    	    children[0] = new OctreeNode(xMin, yMin, zMin, xMid, yMid, zMid, maximumEntriesinOctreeNode);
    	    children[1] = new OctreeNode(xMid, yMin, zMin, xMax, yMid, zMid, maximumEntriesinOctreeNode);
    	    children[2] = new OctreeNode(xMin, yMid, zMin, xMid, yMax, zMid, maximumEntriesinOctreeNode);
    	    children[3] = new OctreeNode(xMid, yMid, zMin, xMax, yMax, zMid, maximumEntriesinOctreeNode);
    	    children[4] = new OctreeNode(xMin, yMin, zMid, xMid, yMid, zMax, maximumEntriesinOctreeNode);
    	    children[5] = new OctreeNode(xMid, yMin, zMid, xMax, yMid, zMax, maximumEntriesinOctreeNode);
    	    children[6] = new OctreeNode(xMin, yMid, zMid, xMid, yMax, zMax, maximumEntriesinOctreeNode);
    	    children[7] = new OctreeNode(xMid, yMid, zMid, xMax, yMax, zMax, maximumEntriesinOctreeNode);
    	    for (OctreePoint point : points) {
    	        int index = getIndex(point);
    	        children[index].addPoint(point);
    	    }
    	    points.clear();
    	}

      public int getIndex(OctreePoint point) throws ParseException {
    	    Object xMid = getMid(xMin, xMax);
    	    Object yMid = getMid(yMin, yMax);
    	    Object zMid = getMid(zMin, zMax);
    	    int index = 0;
    	    if (compare(point.getX(), xMid) >=0) {   
    	        index += 1;
    	    }
    	    if (compare(point.getY(), yMid) >=0) {
    	        index += 2;
    	    }
    	    if (compare(point.getZ(), zMid) >=0) {
    	        index += 4;
    	    }
    	    return index;
    	    
    	    /*Index 0: x-, y-, and z-coordinates are all smaller than their midpoint values.
    	    Index 1: x-coordinates are greater than or equal to the midpoint value of the x-dimension, and whose y- and z-coordinates are smaller than their midpoint values.
    	    Index 2: y-coordinates are greater than or equal to the midpoint value of the y-dimension, and whose x- and z-coordinates are smaller than their respective midpoint values.
    	    Index 3: x- and y-coordinates are greater than or equal to the midpoint values of the x- and y-dimensions, respectively, and whose z-coordinates are smaller than the midpoint value of the z-dimension.
    	    Index 4: z-coordinates are greater than or equal to the midpoint value of the z-dimension, and whose x- and y-coordinates are smaller than their respective midpoint values.
    	    Index 5: x- and z-coordinates are greater than or equal to the midpoint values of the x- and z-dimensions, respectively, and whose y-coordinate is smaller than the midpoint value of the y-dimension.
    	    Index 6: y- and z-coordinates are greater than or equal to the midpoint values of the y- and z-dimensions, respectively, and whose x-coordinate is smaller than the midpoint value of the x-dimension.
    	    Index 7: x-, y-, and z-coordinates are all greater than or equal to the midpoint values of their respective dimensions. */
    	}
      
      public static Object getMid (Object min, Object max) throws ParseException {
    	  if (min instanceof Integer) 
  	    	return ((int)min + (int)max) / 2;
    	  else if (min instanceof Double)
    		  return ((double)min + (double)max) / 2;
    	  else if (min instanceof Date) {
    	        
    	     // calculate duration between dates
    	        long daysBetween = ChronoUnit.DAYS.between(((Date) min).toInstant(), ((Date) max).toInstant());
    	        
    	        // find midpoint between dates
    	        Date middleDate = new Date(((Date) min).getTime() + (daysBetween / 2) * 24 * 60 * 60 * 1000);
    	        
    	        // format midpoint date as string
    	        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    	        String formattedDate = dateFormat.format(middleDate);
    	        Date returnDate = new SimpleDateFormat("yyyy-MM-dd").parse(formattedDate);
    	        return returnDate;
    	  }
    	  else {
    		    int combinedLength = ((String) min).length() + ((String) max).length();
    	        int middle = combinedLength / 2;
    	        if (combinedLength % 2 == 0) 
    	           return ((String) min).substring(((String) min).length() - middle) + ((String) max).substring(0, middle) ;
    	        else
    	           return ((String) min).substring(((String) min).length() - middle - 1, ((String) min).length() - middle) + ((String) max).substring(middle, middle + 1) + ((String) max).substring(0, middle);
    	  }
    		  
      }
      
      public static int compare(Object o1, Object o2) {
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
   			return num1.compareTo(num2);
   		}
   			     
   		if(o1 instanceof Date) {
   		
   			Date num1 = (Date) o1;
   			Date num2 = (Date) o2;
   			return num1.compareTo(num2);
   		}
   		
   		return 0;
   	}
}
