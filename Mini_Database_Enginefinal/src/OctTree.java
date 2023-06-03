import java.io.*;
import java.util.*;

public class OctTree implements Serializable {
    private OctNode root;
    private String indexedCol1;
    private String indexedCol2;
    private String indexedCol3;

    public OctTree(OctNode root, String indexedCol1, String indexedCol2, String indexedCol3)
    {
        this.root = root;
        this.indexedCol1 = indexedCol1;
        this.indexedCol2 = indexedCol2;
        this.indexedCol3 = indexedCol3;
    }

    public void insert (Object o1,Object o2,Object o3,Object clusteringKey, int pageId)
    {
        OctPoint point = new OctPoint(o1, o2, o3, clusteringKey, pageId);
        root.insert(point);
    }

    public void delete (Object o1,Object o2, Object o3,Object clusteringKey, boolean deleteByClusteringKey)
    {
        OctPoint point = new OctPoint(o1,o2,o3,clusteringKey,-1);
        root.delete(point,deleteByClusteringKey);
    }

    public TreeSet<Integer> search (Object o1, Object o2, Object o3)
    {
        OctPoint point = new OctPoint(o1, o2, o3, null,-1);
        return root.search(point);
    }
    public int searchByClusteringKey (Object o1, Object o2, Object o3, Object clusteringKey)
    {
        OctPoint point = new OctPoint(o1, o2, o3, clusteringKey,-1);
        return root.searchByClusteringKey(point);
    }
    public TreeSet<Integer> select(Object l1,Object l2, Object l3, Object h1,Object h2, Object h3)
    {	
    	OctPoint minPoint = new OctPoint(l1, l2, l3, null,-1);
    	OctPoint maxPoint = new OctPoint(h1, h2, h3, null,-1);
    	
    	
    	return root.select(minPoint, maxPoint);
    }

    public boolean find (Object o1, Object o2, Object o3)
    {
        OctPoint point = new OctPoint(o1, o2, o3, null, -1);
        return root.find(point);
    }
    public void printTree()
    {
        root.printTree();
    }

    public String getIndexedCol1() {
        return indexedCol1;
    }

    public void setIndexedCol1(String indexedCol1) {
        this.indexedCol1 = indexedCol1;
    }

    public String getIndexedCol2() {
        return indexedCol2;
    }

    public void setIndexedCol2(String indexedCol2) {
        this.indexedCol2 = indexedCol2;
    }

    public String getIndexedCol3() {
        return indexedCol3;
    }

    public void setIndexedCol3(String indexedCol3) {
        this.indexedCol3 = indexedCol3;
    }

    public OctNode getRoot() {
        return root;
    }

    public void setRoot(OctNode root) {
        this.root = root;
    }

}
