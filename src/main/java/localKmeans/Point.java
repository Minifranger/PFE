package localKmeans;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Point {

    private double x = 0;
    private int cluster_number = 0;
    private String timestamp;

    public Point(double x)
    {
        this.setX(x);
    }
    
    public Point(double x, String t)
    {
        this.setX(x);
        this.timestamp = t;
    }

    public void setX(double x) {
        this.x = x;
    }
    
    public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public double getX()  {
        return this.x;
    }

    
    public void setCluster(int n) {
        this.cluster_number = n;
    }
    
    public int getCluster() {
        return this.cluster_number;
    }
    
    //Calculates the distance between two points.
    protected static double distance(Point p, Point centroid) {
        return Math.abs(centroid.getX() - p.getX());
    }
    
    //Creates random point
    protected static Point createRandomPoint(int min, int max) {
    	Random r = new Random();
    	double x = min + (max - min) * r.nextDouble();
    	return new Point(x);
    }
    
    protected static List createRandomPoints(int min, int max, int number) {
    	List<Point> points = new ArrayList(number);
    	for(int i = 0; i < number; i++) {
    		points.add(createRandomPoint(min,max));
    	}
    	return points;
    }
    
    public String toString() {
    	return "("+x+")";
    }
}