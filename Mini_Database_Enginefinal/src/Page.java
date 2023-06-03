import java.io.*;
import java.util.*;

public class Page implements Serializable {
    private String path;
    private int pageId;
    private Object maxClusteringKey;
    private Object minClusteringKey;
    private Vector<Hashtable<String,Object>> records;

    public Page(String path,int id, Object maxClusteringKey, Object minClusteringKey,
                Vector<Hashtable<String, Object>> records) {

        this.path = path;
        this.pageId = id;
        this.maxClusteringKey = maxClusteringKey;
        this.minClusteringKey = minClusteringKey;
        this.records = records;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getPageId()
    {
        return this.pageId;
    }

    public void setPageId(int pageId)
    {
        this.pageId = pageId;
    }

    public Object getMaxClusteringKey() {
        return maxClusteringKey;
    }

    public void setMaxClusteringKey(Object maxClusteringKey) {
        this.maxClusteringKey = maxClusteringKey;
    }

    public Object getMinClusteringKey() {
        return minClusteringKey;
    }

    public void setMinClusteringKey(Object minClusteringKey) {
        this.minClusteringKey = minClusteringKey;
    }

    public Vector<Hashtable<String, Object>> getRecords() {
        return records;
    }

    public void setRecords(Vector<Hashtable<String, Object>> records) {
        this.records = records;
    }




}
