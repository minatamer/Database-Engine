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
      private ArrayList<OctreeNode> children;
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
          this.children = new ArrayList<OctreeNode>();
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


	public ArrayList<OctreeNode> getChildren() {
		return children;
	}


	public void setChildren(ArrayList<OctreeNode> children) {
		this.children = children;
	}

	


	public int getMaximumEntriesinOctreeNode() {
		return maximumEntriesinOctreeNode;
	}


	

	public void setMaximumEntriesinOctreeNode(int maximumEntriesinOctreeNode) {
		this.maximumEntriesinOctreeNode = maximumEntriesinOctreeNode;
	}


	public void addPoint(OctreePoint point) throws ParseException {
          if (children.size()==0 && points.size() < maximumEntriesinOctreeNode) {
              points.add(point);
          } else {
              if (children.size()==0 ) {
                  subdivide();
              }
              int index = getIndex(point);
              children.get(index).addPoint(point);
          }
      }
      
      public void subdivide() throws ParseException {
    	   	  
    	  	Object xMid = getMid(xMin, xMax);
    	  	Object yMid = getMid(yMin, yMax);
  	    	Object zMid = getMid(zMin, zMax);
  	    	 	    	
  	    	children.add(0, new OctreeNode(xMin, yMin, zMin, xMid, yMid, zMid, maximumEntriesinOctreeNode)); 
    	    children.add(1,new OctreeNode(xMid, yMin, zMin, xMax, yMid, zMid, maximumEntriesinOctreeNode)); 
    	    children.add(2, new OctreeNode(xMin, yMid, zMin, xMid, yMax, zMid, maximumEntriesinOctreeNode)); 
    	    children.add(3,new OctreeNode(xMid, yMid, zMin, xMax, yMax, zMid, maximumEntriesinOctreeNode)); 
    	    children.add(4, new OctreeNode(xMin, yMin, zMid, xMid, yMid, zMax, maximumEntriesinOctreeNode));
    	    children.add(5, new OctreeNode(xMid, yMin, zMid, xMax, yMid, zMax, maximumEntriesinOctreeNode));
    	    children.add(6, new OctreeNode(xMin, yMid, zMid, xMid, yMax, zMax, maximumEntriesinOctreeNode));
    	    children.add(7, new OctreeNode(xMid, yMid, zMid, xMax, yMax, zMax, maximumEntriesinOctreeNode)); 
  	    	
    	    for (OctreePoint point : points) {
    	        int index = getIndex(point);
    	        children.get(index).addPoint(point);
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
    		/*  String mid="";
    		  String minString=((String) min).toLowerCase();
    		  String maxString=((String) max).toLowerCase();
    		  int midStringLength = ((minString.length()+(maxString.length())/2));
    			for(int i=0;i<Integer.min(minString.length(),maxString.length());i++) {
    				mid+=(char)((((CharSequence) minString).charAt(i)+((CharSequence) maxString).charAt(i))/2);
    				}
    			for(int i=0;i<Integer.max((minString.length()),(maxString.length())-midStringLength);i++) {
    				mid+="z";
    				}
    			return mid; */
    		  String minString=(String) min;
    		  String maxString=(String) max;
    		  return getMiddleString(minString, maxString);
    		  
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
   			if(num1.length()>num2.length()) {
   				return 1;
   				}
   			if(num1.length()<num2.length()) {
   				return -1;
   				}
   			return num1.compareTo(num2);
   		}
   			     
   		if(o1 instanceof Date) {
   		
   			Date num1 = (Date) o1;
   			Date num2 = (Date) o2;
   			return num1.compareTo(num2);
   		}
   		
   		return 0;
   	}
      
    	
      
      public static String getMiddleString(String S, String T)
      {
    	  S=S.toLowerCase();
    	  T=T.toLowerCase();
          // Calculate the average length of the two strings
          int avgLen = (S.length() + T.length()) / 2;
          int N = avgLen;
          String result="";
   
          // Pad the strings with 'a' characters so that they have the same length
          while (S.length() < avgLen) {
              S = S + 'a';
          }
          while (T.length() < avgLen) {
              T = T + 'a';
          }
   
          // Stores the base 26 digits after addition
          int[] a1 = new int[N + 1];
   
          for (int i = 0; i < N; i++) {
              a1[i + 1] = (int)S.charAt(i) - 97
                          + (int)T.charAt(i) - 97;
          }
   
          // Iterate from right to left
          // and add carry to next position
          for (int i = N; i >= 1; i--) {
              a1[i - 1] += (int)a1[i] / 26;
              a1[i] %= 26;
          }
   
          // Reduce the number to find the middle
          // string by dividing each position by 2
          for (int i = 0; i <= N; i++) {
   
              // If current value is odd,
              // carry 26 to the next index value
              if ((a1[i] & 1) != 0) {
   
                  if (i + 1 <= N) {
                      a1[i + 1] += 26;
                  }
              }
   
              a1[i] = (int)a1[i] / 2;
          }
   
          for (int i = 1; i <= N; i++) {
              result+=(char)(a1[i] + 97);
          }
          
          return result;
      }
      
    	  
      }
      
      
      
