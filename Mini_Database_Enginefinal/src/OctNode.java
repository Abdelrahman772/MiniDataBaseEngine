import com.sun.source.tree.Tree;

import java.awt.*;
import java.io.*;
import java.text.*;
import java.util.*;

public class OctNode implements Serializable{
    //min x
    private Object x1;
    //max x
    private Object x2;
    //min y
    private Object y1;
    //max y
    private Object y2;
    //min z
    private Object z1;
    //max z
    private Object z2;
    private Vector<OctPoint> storedData = new Vector<>();
    private Vector<OctPoint> duplicates = new Vector<>();
    private OctNode[] children = new OctNode[8];
    private boolean isLeaf;
    private OctNode parent;

    public OctNode (Object x1, Object x2, Object y1, Object y2, Object z1, Object z2, boolean isLeaf,OctNode parent)
    {
        this.x1 = x1;
        this.x2 = x2;
        this.y1 = y1;
        this.y2 = y2;
        this.z1 = z1;
        this.z2 = z2;
        this.isLeaf = isLeaf;
        this.parent = parent;
    }

    public Object getX1() {
        return x1;
    }

    public void setX1(Object x1) {
        this.x1 = x1;
    }

    public Object getX2() {
        return x2;
    }

    public void setX2(Object x2) {
        this.x2 = x2;
    }

    public Object getY1() {
        return y1;
    }

    public void setY1(Object y1) {
        this.y1 = y1;
    }

    public Object getY2() {
        return y2;
    }

    public void setY2(Object y2) {
        this.y2 = y2;
    }

    public Object getZ1() {
        return z1;
    }

    public void setZ1(Object z1) {
        this.z1 = z1;
    }

    public Object getZ2() {
        return z2;
    }

    public void setZ2(Object z2) {
        this.z2 = z2;
    }

    public Vector<OctPoint> getStoredData() {
        return storedData;
    }

    public void setStoredData(Vector<OctPoint> storedData) {
        this.storedData = storedData;
    }

    public Vector<OctPoint> getDuplicates() {
        return duplicates;
    }

    public void setDuplicates(Vector<OctPoint> duplicates) {
        this.duplicates = duplicates;
    }

    public OctNode[] getChildren() {
        return children;
    }

    public void setChildren(OctNode[] children) {
        this.children = children;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public void insert (OctPoint point)
    {
        if (this.isLeaf())
        {
            if (containsPointAndPageId(point, this.storedData)) return;

            if (containsPointAndPageId(point, this.duplicates)) return;

            if (containsPoint(point, this.storedData) && !containsPointAndPageId(point, this.duplicates))
            {
                Vector<OctPoint> updatedDuplicates = this.getDuplicates();
                updatedDuplicates.add(point);
                return;
            }
            if (this.isContainsNull(point) || this.getStoredData().size() < DBApp.MaximumEntriesinOctreeNode)
            {
                Vector<OctPoint> updatedData = this.getStoredData();
                updatedData.add(point);
                return;
            }
            // create the children, distribute the data, set the root to non leaf
            this.setLeaf(false);
            Object xMid = getMedian(this.getX1(), this.getX2());
            Object yMid = getMedian(this.getY1(), this.getY2());
            Object zMid = getMedian(this.getZ1(), this.getZ2());
            // create the children of the node
            OctNode[] children = this.getChildren();
            children[0] = new OctNode(this.getX1(), xMid, this.getY1(), yMid, this.getZ1(), zMid,true,this);
            children[1] = new OctNode(this.getX1(), xMid, this.getY1(), yMid, zMid, this.getZ2(),true,this);
            children[2] = new OctNode(this.getX1(), xMid, yMid, this.getY2(), this.getZ1(), zMid,true, this);
            children[3] = new OctNode(this.getX1(), xMid, yMid, this.getY2(), zMid, this.getZ2(),true,this);
            children[4] = new OctNode(xMid, this.getX2(), this.getY1(), yMid, this.getZ1(), zMid,true, this);
            children[5] = new OctNode(xMid, this.getX2(), this.getY1(), yMid, zMid, this.getZ2(),true, this);
            children[6] = new OctNode(xMid, this.getX2(), yMid, this.getY2(), this.getZ1(), zMid,true, this);
            children[7] = new OctNode(xMid, this.getX2(), yMid, this.getY2(), zMid, this.getZ2(),true, this);
            //distribute the octPoints in that node
            this.setChildren(children);
            Vector<OctPoint> distributedData = this.getStoredData();
            distributedData.add(point);
            for (OctPoint p : distributedData)
            {
                if (isContainsNull(p)) this.children[0].insert(p);
                else {
                    int childIndex = childIndex(p, children);
                    this.children[childIndex].insert(p);
                }
            }
            Vector<OctPoint> distributedDuplicates = this.getDuplicates();
            for (OctPoint p : distributedDuplicates)
            {
                if (isContainsNull(p)) this.children[0].insert(p);
                else {
                    int childIndex = childIndex(p, children);
                    this.children[childIndex].insert(p);
                }
            }
            this.setStoredData(new Vector<>());
            this.setDuplicates(new Vector<>());
        }

        else
        {
            OctNode[] children = this.getChildren();
            for (int i = 0; i < children.length; i++)
            {
                Object minX = children[i].getX1(), minY = children[i].getY1(), minZ = children[i].getZ1();
                Object maxX = children[i].getX2(), maxY = children[i].getY2(), maxZ = children[i].getZ2();

                if (isContainsNull(point))
                {
                    children[0].insert(point);
                    return;
                }
                else if (withinTheRange(point,minX,maxX,minY,maxY,minZ,maxZ))
                {
                    children[i].insert(point);
                    return;
                }

            }

        }
    }

    public void delete (OctPoint point, boolean deleteByClusteringKey)
    {
        if (isLeaf)
        {
            if (!deleteByClusteringKey)
            {
                this.storedData = deletePoints(point,this.storedData);
                this.duplicates = deletePoints(point,this.duplicates);
            }
            else
            {
                this.storedData = deletePointsByClusteringKey(point,this.storedData);
                this.duplicates = deletePointsByClusteringKey(point,this.duplicates);
            }

            if (this.parent != null)
            {
                /* check if the stored points in the children is below max
                   if true remove a level from the tree
                 */
                if (checkStoredPointsBelowMax(this.parent.getChildren()) && checkAllChildrenAreLeaves(this.parent.getChildren()))
                {
                    this.parent.storedData = (Vector<OctPoint>) moveDataFromChildren(this.parent.children)[0];
                    this.parent.duplicates = (Vector<OctPoint>) moveDataFromChildren(this.parent.children)[1];
                    this.parent.children = new OctNode[8];
                    this.parent.isLeaf = true;
                }
            }

            return;
        }
        OctNode[] children = this.getChildren();
        for (int i = 0; i < children.length; i++) {
            Object minX = children[i].getX1(), minY = children[i].getY1(), minZ = children[i].getZ1();
            Object maxX = children[i].getX2(), maxY = children[i].getY2(), maxZ = children[i].getZ2();

            if (isContainsNull(point))
            {
                children[0].delete(point, deleteByClusteringKey);
                return;
            }
            else if (withinTheRange(point, minX, maxX, minY, maxY, minZ, maxZ)) {
                children[i].delete(point, deleteByClusteringKey);
            }
        }

    }

    public TreeSet<Integer> search (OctPoint point)
    {
        if (this.isLeaf)
        {
            return getPagesId(point, this.storedData, this.duplicates);
        }
        else
        {
            OctNode[] children = this.getChildren();
            for (int i = 0; i < children.length; i++) {
                Object minX = children[i].getX1(), minY = children[i].getY1(), minZ = children[i].getZ1();
                Object maxX = children[i].getX2(), maxY = children[i].getY2(), maxZ = children[i].getZ2();

                if (isContainsNull(point))
                {
                   return children[0].search(point);

                }
                else if (withinTheRange(point, minX, maxX, minY, maxY, minZ, maxZ)) {
                    return children[i].search(point);
                }
            }
        }
        return new TreeSet<>();
    }

    public int searchByClusteringKey(OctPoint point)
    {
        if (this.isLeaf)
        {
            return getPageIdByClusteringKey(point,this.storedData,this.duplicates,point.getClusteringKey());
        }
        OctNode[] children = this.getChildren();
        for (int i = 0; i < children.length; i++) {
            Object minX = children[i].getX1(), minY = children[i].getY1(), minZ = children[i].getZ1();
            Object maxX = children[i].getX2(), maxY = children[i].getY2(), maxZ = children[i].getZ2();

            if (isContainsNull(point))
            {
                return children[0].searchByClusteringKey(point);

            }
            else if (withinTheRange(point, minX, maxX, minY, maxY, minZ, maxZ)) {
                return children[i].searchByClusteringKey(point);
            }
        }
        return -1;
    }
    public TreeSet<Integer> select(OctPoint minPoint, OctPoint maxPoint) {
    	TreeSet<Integer> result = new TreeSet<Integer>();
    	
    	if(this.isLeaf) {
    		TreeSet<Integer> res = new TreeSet<Integer>();
    	
    		 for (int i = 0; i < this.storedData.size(); i++) {
    			 	if (withinTheRange(this.storedData.get(i),minPoint.getX(),maxPoint.getX(),
    			 			minPoint.getY(),maxPoint.getY(),minPoint.getZ(),maxPoint.getZ())) {
    	            res.add(this.storedData.get(i).getPageId());    
    			 	}
    	        }
    		 
    		 for (int i = 0; i < this.duplicates.size(); i++) {
 			 	if (withinTheRange(this.duplicates.get(i),minPoint.getX(),maxPoint.getX(),
 			 			minPoint.getY(),maxPoint.getY(),minPoint.getZ(),maxPoint.getZ())) {
 	            res.add(this.duplicates.get(i).getPageId());    
 			 	}
 	        }
    	
    	result.addAll(res);  
    	}
    	
    	else {
    		children = this.getChildren();
    		
    		for(int i = 0; i < children.length; i++) {
    			TreeSet<Integer> ch = children[i].select(minPoint, maxPoint);
    			
    			result.addAll(ch);
    			
    		}
    		
    	}
    	
    	return result;
    }

    public boolean find (OctPoint point)
    {
        if (this.isLeaf)
        {
            return containsPoint(point, this.storedData) || containsPoint(point, this.duplicates);
        }
        OctNode[] children = this.getChildren();
        for (int i = 0; i < children.length; i++) {
            Object minX = children[i].getX1(), minY = children[i].getY1(), minZ = children[i].getZ1();
            Object maxX = children[i].getX2(), maxY = children[i].getY2(), maxZ = children[i].getZ2();

            if (isContainsNull(point))
            {
                return children[0].find(point);

            }
            else if (withinTheRange(point, minX, maxX, minY, maxY, minZ, maxZ)) {
                return children[i].find(point);
            }
        }
        return false;
    }

    public int getPageIdByClusteringKey (OctPoint point, Vector<OctPoint> storedData, Vector<OctPoint> duplicates,Object clusteringKey)
    {
        for (OctPoint target : storedData)
        {
            if (isEqual(point, target) && target.getClusteringKey().equals(clusteringKey))
            {
                return target.getPageId();
            }
        }
        for (OctPoint target : duplicates)
        {
            if (isEqual(point, target) && target.getClusteringKey().equals(clusteringKey))
            {
                return target.getPageId();
            }
        }
        return -1;
    }

    public TreeSet<Integer> getPagesId(OctPoint point, Vector<OctPoint> storedData, Vector<OctPoint> duplicates)
    {
        TreeSet<Integer> res = new TreeSet<>();
        for (OctPoint target : storedData)
        {
            if (isEqual(point, target))
            {
                res.add(target.getPageId());
            }
        }

        for (OctPoint target : duplicates)
        {
            if (isEqual(point, target))
            {
                res.add(target.getPageId());
            }
        }
        return res;
    }

    public boolean checkAllChildrenAreLeaves (OctNode[] children)
    {
        for (OctNode node : children)
        {
            if (!node.isLeaf) return false;
        }
        return true;
    }

    public Object[] moveDataFromChildren (OctNode[] children)
    {
        Vector<OctPoint> movedData = new Vector<>();
        Vector<OctPoint> movedDuplicates = new Vector<>();
        for (OctNode node: children)
        {
            movedData.addAll(node.storedData);
            movedDuplicates.addAll(duplicates);
        }
        Object[] res = new Object[2];
        res[0] = movedData;
        res[1] = movedDuplicates;
        return res;
    }

    public boolean checkStoredPointsBelowMax(OctNode[] children)
    {
        int storedPoints = 0;
        for (OctNode node : children)
        {
            for (OctPoint point : node.storedData)
            {
                if (isContainsNull(point)) continue;
                storedPoints++;
            }
        }

        return storedPoints < DBApp.MaximumEntriesinOctreeNode;
    }
    public Vector<OctPoint> deletePoints(OctPoint point, Vector<OctPoint> storedData)
    {
        for (int i = 0; i < storedData.size(); i++)
        {
            OctPoint target = storedData.get(i);
            if (isEqual(point, target))
            {
                storedData.remove(i);
                i--;
            }
        }
        return storedData;
    }
    public Vector<OctPoint> deletePointsByClusteringKey(OctPoint point, Vector<OctPoint> storedData)
    {
        for (int i = 0; i < storedData.size(); i++)
        {
            OctPoint target = storedData.get(i);
            if (isEqual(point, target) && compare(point.getClusteringKey(), target.getClusteringKey()) == 0)
            {
                storedData.remove(i);
                i--;
            }
        }
        return storedData;
    }

    public boolean containsPointAndPageId(OctPoint point, Vector<OctPoint> storedData)
    {
        for (OctPoint target: storedData)
        {
            if (isEqual(point,target) && point.getPageId() == target.getPageId())
            {
                return true;
            }
        }
        return false;
    }

    public boolean containsPoint(OctPoint point, Vector<OctPoint> storedData)
    {
        for (OctPoint target: storedData)
        {
            if (isEqual(point,target))
            {
                return true;
            }
        }
        return false;
    }

    public void printTree()
    {
        if (this.isLeaf)
        {
            System.out.println("Leaf/ Coordinates: minX: "+ this.getX1()+" maxX: "+this.getX2()+" minY: "+this.getY1()+" maxY: "+this.getY2()+" minZ: "+this.getZ1()+" maxZ: "+ this.getZ2());
            System.out.println("Stored Data: " + this.storedData.toString());
            System.out.println("Duplicates: " + this.duplicates.toString());
            return;
        }
        else
        {
            System.out.println("Parent "+this.toString());
            System.out.println("---------------------------------------------------");
            for (int i = 0; i < this.children.length; i++)
            {
                children[i].printTree();
            }
            System.out.println("---------------------------------------------------");
        }
    }

    public int childIndex (OctPoint Point, OctNode[] children)
    {
        for (int i = 0; i < children.length; i++)
        {
            Object minX = children[i].getX1(), minY = children[i].getY1(), minZ = children[i].getZ1();
            Object maxX = children[i].getX2(), maxY = children[i].getY2(), maxZ = children[i].getZ2();
            if (withinTheRange(Point,minX,maxX,minY,maxY,minZ,maxZ))
            {
                return i;
            }
        }
        return -1;
    }

    public static Object getMedian (Object operand1, Object operand2)
    {
        if (operand1 instanceof java.lang.Integer)
        {
            return (((Integer) operand1) + ((Integer) operand2)) / 2;
        }
        else if (operand1 instanceof java.lang.Double) {
            return (((Double) operand1) + ((Double) operand2)) / 2.0;
        }
        else if (operand1 instanceof java.util.Date)
        {
            return new Date((((Date) operand1).getTime() + ((Date) operand2).getTime()) / 2);
        }

        String str1 = (String) operand1;
        String str2 = (String) operand2;
        str1 = str1.toLowerCase();
        str2 = str2.toLowerCase();
        if (str1.length() > str2.length())
        {
            str2 += str1.substring(str2.length());
        }
        else
        {
            str1 += str2.substring(str1.length());
        }

        int[] arr = new int[str1.length()+1];
        for(int i = 0; i < str1.length(); i++)
        {
            arr[i+1] = (int)str1.charAt(i) - 97 + (int)str2.charAt(i) - 97;
        }

        for (int i = str1.length(); i >= 1; i--)
        {
            arr[i-1] += (int)arr[i] / 26;
            arr[i] %= 26;
        }

        for (int i = 0; i <= str1.length(); i++)
        {
            if ((arr[i] & 1) != 0)
            {
                if (i + 1 <= str1.length())
                {
                    arr[i+1] += 26;
                }
            }
            arr[i] = (int)arr[i] / 2;
        }
        String res = "";
        for (int i = 1; i <= str1.length(); i++)
        {
            res += (char)(arr[i] + 97);
        }
        return res;
    }

    public boolean withinTheRange (OctPoint Point,Object X1, Object X2, Object Y1, Object Y2, Object Z1, Object Z2)
    {
        return (compare(Point.getX(), X1) >= 0 && compare(Point.getX(),X2) <= 0)
                && (compare(Point.getY(), Y1) >= 0 && compare(Point.getY(),Y2) <= 0)
                && (compare(Point.getZ(), Z1) >= 0 && compare(Point.getZ(),Z2) <= 0);
    }

    public int compare (Object o1,Object o2)
    {
        if (o1 instanceof DBAppNull && o2 instanceof DBAppNull) return 0;
        else if (o1 instanceof java.lang.Integer && o2 instanceof java.lang.Integer) return ((Integer)o1).compareTo((Integer)o2);
        else if (o1 instanceof java.lang.Double && o2 instanceof java.lang.Double) return ((Double)o1).compareTo((Double)o2);
        else if (o1 instanceof java.lang.String && o2 instanceof java.lang.String) return ((String)o1).compareTo((String)o2);
        else if (o1 instanceof java.util.Date && o2 instanceof java.util.Date) ((Date)o1).compareTo((Date)o2);
        return -1;
    }

    public boolean isEqual(OctPoint p1, OctPoint p2)
    {
        return (compare(p1.getX(),p2.getX()) == 0)
                && (compare(p1.getY(),p2.getY()) == 0)
                && (compare(p1.getZ(),(p2.getZ())) == 0);
    }

    public boolean isContainsNull(OctPoint p)
    {
        return (p.getX() instanceof DBAppNull) || (p.getY() instanceof DBAppNull) || (p.getZ() instanceof DBAppNull);
    }

    @Override
    public String toString() {
        return "Parent maxX: "+this.x1+" minX: "+this.x2+" maxY: "+this.y1+" minY: "+this.y2+" maxZ: "+this.z1+" minZ: "+this.z2+" children: "+this.storedData.toString();
    }

}
