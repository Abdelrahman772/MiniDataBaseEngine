import java.io.*;
import java.util.*;

public class Table implements Serializable {
    private String tableName;
    transient private Vector<Page> Pages;
    private Vector<Integer> pagesId;// keep track of pagesID in my table
    private Vector<Integer> indicesId;
    private int noPages;


    public Table(String tableName, Vector<Page> pages, Vector<Integer> pagesId,Vector<Integer> indicesId, int noPages) {
        this.tableName = tableName;
        this.Pages = pages;
        this.pagesId = pagesId;
        this.indicesId = indicesId;
        this.noPages = noPages;

    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Vector<Page> getPages() {
        return Pages;
    }

    public void setPages(Vector<Page> pages) {
        this.Pages = pages;
    }

    public Vector<Integer> getPagesId() {
        return this.pagesId;
    }

    public void setPagesId(Vector<Integer> pagesId) {
        this.pagesId = pagesId;
    }

    public int getNoPages() {
        return noPages;
    }

    public void setNoPages(int noPages) {
        this.noPages = noPages;
    }

    public Vector<Integer> getIndicesId() {
        return indicesId;
    }

    public void setIndicesId(Vector<Integer> indicesId) {
        this.indicesId = indicesId;
    }
}

