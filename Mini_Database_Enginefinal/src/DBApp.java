import javax.print.attribute.HashAttributeSet;
import java.io.*;
import java.nio.file.Path;
import java.text.*;
import java.util.*;

public class DBApp {
    static int MaximumRowsCountinTablePage;
    static int MaximumEntriesinOctreeNode;

    public void init()
    {
        //create resources directory
        File resourcesDirectory = new File("src/main/resources/data/Tables");
        if (!resourcesDirectory.exists()) resourcesDirectory.mkdirs();

        try {
            File metadata = new File("src/main/resources/metadata.csv");
            if (!metadata.exists()) metadata.createNewFile();

            FileWriter metadataWriter = new FileWriter("src/main/resources/metadata.csv", true);
            metadataWriter.append("Table Name,Column Name,Column Type,ClusteringKey,IndexName,IndexType,min,max\n");
            metadataWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax ) throws DBAppException
    {
        //validate the given inputs
        for (String obj : htblColNameType.keySet())
            if (!htblColNameMin.containsKey(obj) || !htblColNameMax.containsKey(obj))
                throw new DBAppException("type");

        for (String obj : htblColNameMax.keySet())
            if (!htblColNameType.containsKey(obj) || !htblColNameMin.containsKey(obj))
                throw new DBAppException("max");

        for (String obj : htblColNameMin.keySet())
            if (!htblColNameType.containsKey(obj) || !htblColNameMax.containsKey(obj))
                throw new DBAppException("min");

        if (!htblColNameType.containsKey(strClusteringKeyColumn)) throw new DBAppException("Clustering key Column does not exist");

        File tableDirectory = new File("src/main/resources/data/Tables/"+strTableName);
        File indexDirectory = new File("src/main/resources/data/Tables/"+strTableName+"/Indices");

        if (!tableDirectory.exists()) tableDirectory.mkdir();
        else throw new DBAppException("Table already exists");

        indexDirectory.mkdirs();

        //Create table and initializing its attributes then serializing it
        Vector<Page> pages = new Vector<>();
        Vector<Integer> pagesID = new Vector<>();
        Table table = new Table(strTableName,pages,pagesID,new Vector<>(),0);
        String tablePath = "src/main/resources/data/Tables/"+strTableName+"/"+strTableName+".ser";
        Serialize(tablePath,table);

        //insert information about the table in the metadata file
        try {
            FileWriter fw = new FileWriter("src/main/resources/metadata.csv", true);
            for (String obj : htblColNameType.keySet()) {
                StringBuilder sb = new StringBuilder();
                sb.append(strTableName + "," + obj + "," + htblColNameType.get(obj) + ",");
                if (obj.equals(strClusteringKeyColumn)) sb.append("true,");
                else sb.append("false,");
                sb.append("null,null," + htblColNameMin.get(obj) + "," + htblColNameMax.get(obj));
                fw.append(sb.toString() + "\n");
            }
            fw.close();
        } catch (IOException e) {
            throw new DBAppException();
        }
    }

    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException
    {
        Object[] dataInfo = readFromCSV(strTableName);
        String primaryKey = (String) dataInfo[0];
        String clusteringType = (String) dataInfo[1];
        Hashtable<String, String> dataTypes = (Hashtable) dataInfo[2];
        Hashtable<String, Object> minValues = (Hashtable) dataInfo[3];
        Hashtable<String, Object> maxValues = (Hashtable) dataInfo[4];

        if (strarrColName.length != 3) throw new DBAppException("Can not build index on the given columns");

        validateColumnNames(strarrColName);

        for (String col : strarrColName)
        {
            if (!dataTypes.containsKey(col)) throw new DBAppException("Column does not exist");
        }

        Table table = (Table) Deserialize("src/main/resources/data/Tables/"+strTableName+"/"+strTableName+".ser");
        Vector<Integer> indicesId = table.getIndicesId();

        if (indexIsCreated(strTableName,strarrColName, table)) throw new DBAppException("Index Already Created on the given columns");

        String indexedCol1 = strarrColName[0], indexedCol2 = strarrColName[1], indexedCol3 = strarrColName[2];
        Object minX = minValues.get(indexedCol1), minY = minValues.get(indexedCol2), minZ = minValues.get(indexedCol3);
        Object maxX = maxValues.get(indexedCol1), maxY = maxValues.get(indexedCol2), maxZ = maxValues.get(indexedCol3);
        OctNode root = new OctNode(minX, maxX, minY, maxY, minZ, maxZ, true, null);
        OctTree tree = new OctTree(root, indexedCol1, indexedCol2, indexedCol3);
        int indexId = indicesId.size();

        String indexPath = "src/main/resources/data/Tables/"+strTableName+"/Indices/index"+indexId+".ser";
        indicesId.add(indexId);
        table.setIndicesId(indicesId);
        //to handle if index is created after inserting the records
        if (table.getPagesId().size() != 0)
        {
            populateRecordsInIndex(strTableName,table.getPagesId(),tree,primaryKey);
        }

        updateMetaData(strTableName,strarrColName, indexId);
        Serialize("src/main/resources/data/Tables/"+strTableName+"/"+strTableName+".ser", table);
        Serialize(indexPath, tree);
    }

    public void validateColumnNames (String[] strarrColName) throws DBAppException {

        if (strarrColName[0].equals(strarrColName[1]) || strarrColName[0].equals(strarrColName[2]) || strarrColName[1].equals(strarrColName[2]))
        {
            throw new DBAppException("Can not build index on two same columns");
        }

    }

    public void updateMetaData(String strTableName, String[] strarrColName, int indexId) throws DBAppException {
        try {
            FileReader oldMetaDataFile = new FileReader("src/main/resources/metadata.csv");
            BufferedReader br = new BufferedReader(oldMetaDataFile);
            List<String[]> rows = new ArrayList<>();
            String curLine = "";

            while ((curLine = br.readLine()) != null) {
                String[] values = curLine.split(",");
                rows.add(values);
            }

            for (String[] row : rows)
            {
                if (!row[0].equals(strTableName)) continue;

                for (String col : strarrColName)
                {
                    if (row[1].equals(col))
                    {
                        row[4] = "Index"+indexId;
                        row[5] = "Octree";
                    }
                }
            }
            FileWriter metaDataFile = new FileWriter("src/main/resources/metadata.csv");
            for (String[] row : rows)
            {
                metaDataFile.write(String.join(",", row) + "\n");
            }
            metaDataFile.close();
        }
        catch (Exception e)
        {
            throw new DBAppException("Exception thrown in updateMetaData");
        }
    }

    public void populateRecordsInIndex(String strTableName, Vector<Integer> pagesId, OctTree tree, String primaryKey) throws DBAppException {
        readConfig();
        for (Integer id : pagesId)
        {
            String path = "src/main/resources/data/Tables/"+strTableName+"/Page"+id+".ser";
            Page target = (Page) Deserialize(path);
            Vector<Hashtable<String, Object>> records = target.getRecords();
            for (Hashtable<String,Object> record : records)
            {
                String indexedCol1 = tree.getIndexedCol1(), indexedCol2 = tree.getIndexedCol2(), indexedCol3 = tree.getIndexedCol3();
                Object o1 = record.get(indexedCol1), o2 = record.get(indexedCol2), o3 = record.get(indexedCol3);
                Object clusteringKey = record.get(primaryKey);
                tree.insert(o1,o2,o3,clusteringKey,id);
            }
        }
    }

    public boolean indexIsCreated(String strTableName, String[] strarrColName, Table table) throws DBAppException {
        Vector<Integer> indicesId = table.getIndicesId();
        if (indicesId.size() == 0) return false;
        HashSet<String> hs = new HashSet<>();
        for (String s : strarrColName)
        {
            hs.add(s);
        }
        for (Integer id : indicesId)
        {
            String path = "src/main/resources/data/Tables/"+strTableName+"/Indices/index"+id+".ser";
            OctTree tree = (OctTree) Deserialize(path);
            if (hs.contains(tree.getIndexedCol1()) || hs.contains(tree.getIndexedCol2()) || hs.contains(tree.getIndexedCol3()))
            {
                return true;
            }
        }
        return false;
    }

    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException
    {
        if (htblColNameValue.isEmpty()) throw new DBAppException("Clustering key value cannot be null");

        Object[] tableInfo = readFromCSV(strTableName);

        String primaryKey = (String) tableInfo[0];

        if (!htblColNameValue.containsKey(primaryKey)) throw new DBAppException("Clustering key value cannot be null");

        Hashtable<String, String> dataTypes = (Hashtable) tableInfo[2];
        Hashtable<String, Object> minValues = (Hashtable) tableInfo[3];
        Hashtable<String, Object> maxValues = (Hashtable) tableInfo[4];

        checkColDataTypes(dataTypes, htblColNameValue);
        checkColCompatibility(dataTypes, htblColNameValue);
        checkRange(dataTypes, minValues, maxValues, htblColNameValue);
        // memic the null values
        for (String key : dataTypes.keySet()) {
            if (!htblColNameValue.containsKey(key)) {
                htblColNameValue.put(key, new DBAppNull());
            }
        }

        String tablePath = "src/main/resources/data/Tables/"+strTableName+"/"+strTableName+".ser";
        Table table = (Table) Deserialize(tablePath);
        Vector<Integer> pagesId = table.getPagesId();

        //to know whether the page I will insert record in it is full or not
        readConfig();
        Vector<String> indiciesPath = searchIfIndexExists(strTableName,table,htblColNameValue);

        //there is no pages yet in the table so we need to create our first page
        if (table.getNoPages() == 0)
        {
            String pagePath = "src/main/resources/data/Tables/"+strTableName+"/Page0.ser";
            Vector<Hashtable<String,Object>> records = new Vector<>();
            records.add(htblColNameValue);
            Page p = new Page(pagePath,0,htblColNameValue.get(primaryKey),htblColNameValue.get(primaryKey),records);
            table.setNoPages(1);
            pagesId.add(p.getPageId());
            table.setPagesId(pagesId);
            Serialize(pagePath, p);
            Serialize(tablePath,table);
            insertIntoIndex(indiciesPath, htblColNameValue, htblColNameValue.get(primaryKey), 0);
            return;
        }

        //search for the page I want to insert in
        int indexOfPage = searchForPageToInsert(pagesId,strTableName,htblColNameValue.get(primaryKey));
        String pagePath = "src/main/resources/data/Tables/"+strTableName+"/Page"+indexOfPage+".ser";
        Page p = (Page) Deserialize(pagePath);

        //case 1: There is an empty space for the record to be inserted
        if (!isFull(p.getRecords()))
        {
            //binary search to insert the record in the correct place
            Vector<Hashtable<String,Object>> records = p.getRecords();
            int insertIndexInPage = searchForRecord(records,htblColNameValue.get(primaryKey),primaryKey);
            records.add(insertIndexInPage,htblColNameValue);
            p.setMaxClusteringKey((compare(htblColNameValue.get(primaryKey),p.getMaxClusteringKey()) > 0 ? htblColNameValue.get(primaryKey) : p.getMaxClusteringKey()));
            p.setMinClusteringKey((compare(htblColNameValue.get(primaryKey),p.getMinClusteringKey()) <= 0 ? htblColNameValue.get(primaryKey) : p.getMinClusteringKey()));
            p.setRecords(records);
            Serialize(pagePath,p);
            Serialize(tablePath,table);
            insertIntoIndex(indiciesPath, htblColNameValue, htblColNameValue.get(primaryKey), indexOfPage);
            return;
        }

        //the page is full so we have to insert the record in the correct place and shift the last record
        Vector<Hashtable<String,Object>> records = p.getRecords();
        int indexToInsert = searchForRecord(records,htblColNameValue.get(primaryKey),primaryKey);
        records.add(indexToInsert,htblColNameValue);
        Hashtable<String,Object> shiftedRecord = records.lastElement();
        records.remove(records.size() - 1);
        //to handle the min and max clustering key of the page
        Hashtable<String,Object> firstRecord = records.firstElement();
        Hashtable<String,Object> lastRecord = records.lastElement();
        p.setMinClusteringKey(firstRecord.get(primaryKey));
        p.setMaxClusteringKey(lastRecord.get(primaryKey));
        p.setRecords(records);
        Serialize(pagePath,p);
        Serialize(tablePath,table);
        deleteFromIndex(indiciesPath, shiftedRecord, shiftedRecord.get(primaryKey), true);
        insertIntoIndex(indiciesPath, htblColNameValue, htblColNameValue.get(primaryKey), indexOfPage);

        //check if this page is the last page so I will have to create a new page to enter the shifted record
        if (pagesId.lastElement() == p.getPageId())
        {
            Serialize(pagePath,p);
            int newId = pagesId.lastElement() + 1;
            String newPath = "src/main/resources/data/Tables/"+strTableName+"/Page"+newId+".ser";
            Vector<Hashtable<String,Object>> newRecords = new Vector<>();
            newRecords.add(shiftedRecord);
            Object clusteringKey = shiftedRecord.get(primaryKey);
            Page newPage = new Page(newPath,newId,clusteringKey,clusteringKey,newRecords);
            Serialize(newPath, newPage);
            pagesId.add(newId);
            table.setNoPages(table.getNoPages() + 1);
            Serialize(tablePath,table);
            deleteFromIndex(indiciesPath, shiftedRecord, shiftedRecord.get(primaryKey), true);
            insertIntoIndex(indiciesPath, shiftedRecord, shiftedRecord.get(primaryKey), newId);
            return;
        }

        insertIntoTable(strTableName,shiftedRecord);

    }

    public  void deleteFromIndex (Vector<String> indicesPath, Hashtable<String,Object> htblColNameValue, Object clusteringKey, boolean deleteByClusteringKey) throws DBAppException {
        for (String path : indicesPath)
        {
            OctTree tree = (OctTree) Deserialize(path);
            Object o1 = htblColNameValue.get(tree.getIndexedCol1());
            Object o2 = htblColNameValue.get(tree.getIndexedCol2());
            Object o3 = htblColNameValue.get(tree.getIndexedCol3());
            tree.delete(o1, o2, o3, clusteringKey,deleteByClusteringKey);
            Serialize(path, tree);
        }
    }

    public void insertIntoIndex (Vector<String> indicesPath, Hashtable<String,Object> htblColNameValue, Object clusteringKey,int pageId) throws DBAppException {

        for (String path : indicesPath)
        {
            OctTree tree = (OctTree) Deserialize(path);
            Object o1 = htblColNameValue.get(tree.getIndexedCol1());
            Object o2 = htblColNameValue.get(tree.getIndexedCol2());
            Object o3 = htblColNameValue.get(tree.getIndexedCol3());
            tree.insert(o1, o2, o3, clusteringKey, pageId);
            Serialize(path, tree);
        }
    }

    public Vector<String> searchIfIndexExists(String strTableName,Table table, Hashtable<String,Object> htblColNameValue) throws DBAppException {

        Vector<String> res = new Vector<>();
        Vector<Integer> indicesId = table.getIndicesId();
        for (Integer id : indicesId)
        {
            String path = "src/main/resources/data/Tables/"+strTableName+"/Indices/index"+id+".ser";
            OctTree tree = (OctTree) Deserialize(path);
            String indexedCol1 = tree.getIndexedCol1();
            String indexedCol2 = tree.getIndexedCol2();
            String indexedCol3 = tree.getIndexedCol3();
            if (htblColNameValue.containsKey(indexedCol1) && htblColNameValue.containsKey(indexedCol2) && htblColNameValue.containsKey(indexedCol3))
            {
                res.add(path);
            }
        }

        return res;
    }

    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue ) throws DBAppException {

        if (htblColNameValue.isEmpty()) return;

        Object[] tableInfo = readFromCSV(strTableName);

        String clusteringKey = (String) tableInfo[0];
        String clusteringType = (String) tableInfo[1];
        Hashtable<String,String> dataTypes = (Hashtable) tableInfo[2];
        Hashtable<String,Object> minValues= (Hashtable) tableInfo[3];
        Hashtable<String,Object> maxValues = (Hashtable) tableInfo[4];

        //make sure not to update the clustering key
        if (htblColNameValue.containsKey(clusteringKey))
        {
            throw new DBAppException("can not update clustering");
        }

        Object clusteringObject;
        switch (clusteringType)
        {
            case "java.lang.Integer":
                try {
                    clusteringObject = Integer.parseInt(strClusteringKeyValue);
                }catch (NumberFormatException e)
                {
                    throw new DBAppException();
                }
                break;
            case "java.lang.Double":
                try {
                    clusteringObject = Double.parseDouble(strClusteringKeyValue);
                }catch (NumberFormatException e)
                {
                    throw new DBAppException();
                }
                break;
            case "java.util.Date":
                try {
                    clusteringObject = new SimpleDateFormat("yyyy-MM-dd").parse(strClusteringKeyValue);
                } catch (ParseException e) {
                    throw new DBAppException("Parse exception in update table method ");
                }
                break;
            default:
                clusteringObject = (String)strClusteringKeyValue;
                break;
        }

        htblColNameValue.put(clusteringKey,clusteringObject);

        checkColDataTypes(dataTypes, htblColNameValue);
        checkColCompatibility(dataTypes, htblColNameValue);
        checkRange(dataTypes, minValues, maxValues, htblColNameValue);

        String tablePath = "src/main/resources/data/Tables/"+strTableName+"/"+strTableName+".ser";
        Table table = (Table) Deserialize(tablePath);
        Vector<Integer> pagesId = table.getPagesId();

        if (pagesId.size() > 0) {
            //search for the page to update in
            int indexPage = binarySearchOnPages(strTableName, pagesId, clusteringObject);

            if (indexPage == -1) throw new DBAppException("Clustering key value does not exits");

            String updatePath = "src/main/resources/data/Tables/" + strTableName + "/Page" + indexPage + ".ser";
            Page updatePage = (Page) Deserialize(updatePath);
            Vector<Hashtable<String, Object>> records = updatePage.getRecords();
            int recordIndex = searchForRecordToUpdate(records, clusteringKey, clusteringObject);

            if (recordIndex == -1) throw new DBAppException("Clustering key value does not exits");

            Hashtable<String, Object> oldRecord = records.get(recordIndex);
            Vector<String> indicesPath = searchIfIndexExists(strTableName, table, htblColNameValue);
            deleteFromIndex(indicesPath, oldRecord,oldRecord.get(clusteringKey), true);

            for (String key : htblColNameValue.keySet()) {
                records.get(recordIndex).replace(key, htblColNameValue.get(key));
            }

            Hashtable<String,Object> updatedRecord = records.get(recordIndex);
            updatePage.setRecords(records);
            Serialize(updatePath, updatePage);

            // delete old record from indices and update the new one
            insertIntoIndex(indicesPath, updatedRecord, updatedRecord.get(clusteringKey), indexPage);

        }
        Serialize(tablePath,table);
    }

    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {

        Object[] tableInfo = readFromCSV(strTableName);

        String clusteringKey = (String) tableInfo[0];
        Hashtable<String,String> dataTypes = (Hashtable) tableInfo[2];
        Hashtable<String,Object> minValues= (Hashtable) tableInfo[3];
        Hashtable<String,Object> maxValues = (Hashtable) tableInfo[4];

        checkColDataTypes(dataTypes, htblColNameValue);
        checkColCompatibility(dataTypes, htblColNameValue);
        checkRange(dataTypes, minValues, maxValues, htblColNameValue);

        String tablePath = "src/main/resources/data/Tables/"+strTableName+"/"+strTableName+".ser";
        Table table = (Table) Deserialize(tablePath);
        Vector<Integer> pagesId = table.getPagesId();
        Vector<String> availableIndices = getAvailableIndices(strTableName, table);
        Vector<String> indicesPath = searchIfIndexExists(strTableName, table, htblColNameValue);
        boolean useIndex = indicesPath.size() != 0;
        boolean clusteringKeyExists = htblColNameValue.containsKey(clusteringKey);

        if (table.getNoPages() > 0){

            if (useIndex)
            {
                TreeSet<Integer> targetId = new TreeSet<>();
                for (String path : indicesPath)
                {
                    OctTree tree = (OctTree) Deserialize(path);
                    String indexedCol1 = tree.getIndexedCol1(), indexedCol2 = tree.getIndexedCol2(), indexedCol3 = tree.getIndexedCol3();
                    Object o1 = htblColNameValue.get(indexedCol1), o2 = htblColNameValue.get(indexedCol2), o3 = htblColNameValue.get(indexedCol3);
                    targetId.addAll(tree.search(o1, o2,o3));
                    Serialize(path, tree);
                    tree = null;
                }

                for (Integer id : targetId)
                {
                    if (!pagesId.contains(id)) continue;

                    String pagePath = "src/main/resources/data/Tables/" + strTableName + "/Page" + id + ".ser";
                    Page targetPage = (Page) Deserialize(pagePath);
                    Vector<Hashtable<String, Object>> updatedRecords = deleteRecordsFromPage(targetPage.getRecords(), htblColNameValue, availableIndices);

                    if (updatedRecords.size() == 0) {

                        File f = new File(pagePath);
                        f.delete();
                        table.setNoPages(table.getNoPages() - 1);
                        Vector<Integer> pagesID = table.getPagesId();
                        pagesID.remove(new Integer(targetPage.getPageId()));
                        table.setPagesId(pagesID);

                    } else {
                        int size = updatedRecords.size();
                        targetPage.setRecords(updatedRecords);
                        targetPage.setMinClusteringKey(updatedRecords.get(0).get(clusteringKey));
                        targetPage.setMaxClusteringKey(updatedRecords.get(size - 1).get(clusteringKey));
                        Serialize(pagePath, targetPage);
                    }
                }

            }

            else {
                //linear search on the pages of the table
                for (int i = 0; i < pagesId.size(); i++) {

                    int id = table.getPagesId().get(i);
                    String path = "src/main/resources/data/Tables/" + strTableName + "/Page" + id + ".ser";
                    Page targetPage = (Page) Deserialize(path);
                    Vector<Hashtable<String, Object>> currRecords = targetPage.getRecords();

                    Vector<Hashtable<String, Object>> updatedRecords = new Vector<>();
                    if (clusteringKeyExists)
                    {
                      
                        updatedRecords = deleteRecordsByBinarySearch(currRecords, htblColNameValue, clusteringKey,htblColNameValue.get(clusteringKey), availableIndices);
                    }
                    else
                    {
                        updatedRecords =  deleteRecordsFromPage(targetPage.getRecords(), htblColNameValue, availableIndices);
                    }

                    //check if no records exist in the page after deletion
                    if (updatedRecords.size() == 0) {

                        File f = new File(path);
                        f.delete();
                        table.setNoPages(table.getNoPages() - 1);
                        Vector<Integer> pagesID = table.getPagesId();
                        pagesID.remove(new Integer(targetPage.getPageId()));
                        table.setPagesId(pagesID);
                        i--;

                    } else {

                        int size = updatedRecords.size();
                        targetPage.setRecords(updatedRecords);
                        targetPage.setMinClusteringKey(updatedRecords.get(0).get(clusteringKey));
                        targetPage.setMaxClusteringKey(updatedRecords.get(size - 1).get(clusteringKey));
                        Serialize(path, targetPage);

                    }
                }
            }
        }

        Serialize(tablePath,table);
    }
    public Object[] readFromCSV2 (String strTableName) throws DBAppException {
        String clusteringKey = "", clusteringType = "";
        Hashtable<String,String> dataIndex = new Hashtable<>();
        Hashtable<String,Object> dataIndexType = new Hashtable<>();
        boolean flag = false;
        try {
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String line = "";
            while ((line = br.readLine()) != null) {
                String[] data = line.split(",");

                if (data[0].equals(strTableName)) {

                	dataIndex.put(data[1], data[4]);
                	dataIndexType.put(data[1], data[5]);
                    flag = true;

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (!flag) throw new DBAppException("Table does not exist");

        return new Object[]{dataIndex,dataIndexType};
    }
    
  public boolean useSelectIndex (SQLTerm[] arrSQLTerms,String[] strarrOperators, String strTableName) throws DBAppException {
    	
    	
    	
    	  Object[] tableInfo = readFromCSV2(strTableName);
    	  Hashtable<String, String> Index = (Hashtable) tableInfo[0];
          Hashtable<String, Object> IndexType = (Hashtable) tableInfo[1];
    	
    	
        String index = Index.get(arrSQLTerms[0]._strColumnName);
    	boolean flag = true;
    	boolean sameIndex = false;
    	
    	
    	for(int i = 0; i < arrSQLTerms.length; i++) {
    		
    		String columnname = arrSQLTerms[i]._strColumnName;
    		if (!Index.get(columnname).equals(index)) {
    			flag = false;
    		}
    	}
    	if (arrSQLTerms.length != 3)
    		flag = false;
    	
    	if (strarrOperators.length != 2)
    		flag = false;
    	
    	for (String op : strarrOperators) {
            if (!op.equals("AND")) {
                flag = false;
                break;
            }
        }
    	
		return flag;

    }
    
  public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
	  
	  if (arrSQLTerms.length - strarrOperators.length != 1) throw new DBAppException("Invalid arrSQLTerms and strarrOperators");

      //to check on the given operators AND,OR,XOR
      validateStrarrOperators(strarrOperators);
      validateArrSQLTerms(arrSQLTerms);
      
      String clusteringKey = (String) readFromCSV(arrSQLTerms[0]._strTableName)[0];
      Hashtable<String,String> dataTypes = (Hashtable) readFromCSV(arrSQLTerms[0]._strTableName)[2];
      Hashtable<String,Object> minValues= (Hashtable) readFromCSV(arrSQLTerms[0]._strTableName)[3];
      Hashtable<String,Object> maxValues = (Hashtable) readFromCSV(arrSQLTerms[0]._strTableName)[4];
	  
	  String tableName = arrSQLTerms[0]._strTableName;
	  
  	if (!useSelectIndex(arrSQLTerms,strarrOperators, arrSQLTerms[0]._strTableName)) {
  		
  		selectLinearly(arrSQLTerms,strarrOperators);
  	}
  	else {
  		
  		Object[] tableInfo = readFromCSV2(arrSQLTerms[0]._strTableName);
	    Hashtable<String, String> Index = (Hashtable) tableInfo[0];
        Hashtable<String, Object> IndexType = (Hashtable) tableInfo[1];
         
	    
        String index = Index.get(arrSQLTerms[0]._strColumnName).toLowerCase();
	        
          
        String indexPath = "src/main/resources/data/Tables/"+arrSQLTerms[0]._strTableName+"/Indices/"+index+".ser";
  	    OctTree tree = (OctTree) Deserialize(indexPath);
  	    Serialize(indexPath,tree);

  	    Object x1 = tree.getRoot().getX1();
  	    Object x2 = tree.getRoot().getX2();
  	    Object y1 = tree.getRoot().getY1();
  	    Object y2 = tree.getRoot().getY2();
  	    Object z1 = tree.getRoot().getZ1();
  	    Object z2 = tree.getRoot().getZ2();
  	    Object rx =0;
  	    Object ry =0;
  	    Object rz =0;
  	    boolean removex = false;
  	    boolean removey = false;
  	    boolean removez = false;
  	    String s1 = "";
  	    String s2 = "";
  	    String s3 = "";
  	    
  	    
  	 for(int k = 0 ; k < arrSQLTerms.length; k++) {
  	    if(k == 0) {
  	    	switch (dataTypes.get(arrSQLTerms[k]._strColumnName).toLowerCase()) {
  	    	case "java.lang.integer":
  	    		switch (arrSQLTerms[k]._strOperator) {
  	    		
  	    		case ">": 
                  	x1 = (Integer) (arrSQLTerms[k]._objValue) + 1;
                  	x2 = (Integer) tree.getRoot().getX2();
                  	break;
                  case "<":
                  	x1 = (Integer) tree.getRoot().getX1();
                  	x2 = (Integer) (arrSQLTerms[k]._objValue) - 1;
                  	break;
                  case "<=":
                  	x1 = (Integer) tree.getRoot().getX1();
                  	x2 = (arrSQLTerms[k]._objValue);
                  	break;
                  case ">=":
                  	x1 = (Integer) (arrSQLTerms[k]._objValue);
                  	x2 = (Integer) tree.getRoot().getX2();
                  	break;
                  case "=":
                  	x1 = (Integer) (arrSQLTerms[k]._objValue);
                  	x2 = (Integer) (arrSQLTerms[k]._objValue);
                  	break;	
                  case "!=":
                  	removex=true;
                  	rx = (Integer) (arrSQLTerms[k]._objValue);
                  	s1 = arrSQLTerms[k]._strColumnName;
                  	break;		
  	    		}
  	   break;
  	    	case "java.lang.double":
  	    		switch (arrSQLTerms[k]._strOperator) {
  	    		
  	    		case ">": 
                  	x1 = (double) (arrSQLTerms[k]._objValue) + 0.0000001;
                  	x2 = (double) tree.getRoot().getX2();
                  	break;
                  case "<":
                  	x1 = (double) tree.getRoot().getX1();
                  	x2 = (double) (arrSQLTerms[k]._objValue) - 0.0000001;
                  	break;
                  case "<=":
                  	x1 = (double) tree.getRoot().getX1();
                  	x2 = (double)(arrSQLTerms[k]._objValue);
                  	break;
                  case ">=":
                  	x1 = (double) (arrSQLTerms[k]._objValue);
                  	x2 = (double) tree.getRoot().getX2();
                  	break;
                  case "=":
                  	x1 = (double) (arrSQLTerms[k]._objValue);
                  	x2 = (double) (arrSQLTerms[k]._objValue);
                  	break;
                  case "!=":
                  	removex=true;
                  	rx = (double) (arrSQLTerms[k]._objValue);
                  	s1 = arrSQLTerms[k]._strColumnName;
                  	break;	
  	    		}
  	   break;
  	    	case "java.lang.string":
  	    		switch (arrSQLTerms[k]._strOperator) {
  	    		
  	    		case ">": 
                  	x1 = (String) (arrSQLTerms[k]._objValue) + "a";
                  	x2 = (String) tree.getRoot().getX2();
                  	break;
                  case "<":
                  	x1 = (String) tree.getRoot().getX1();
                  	x2 = getPrequel((String) (arrSQLTerms[k]._objValue));   
                  	break;
                  case "<=":
                  	x1 = (String) tree.getRoot().getX1();
                  	x2 = (String)(arrSQLTerms[k]._objValue);
                  	break;
                  case ">=":
                  	x1 = (String) (arrSQLTerms[k]._objValue);
                  	x2 = (String) tree.getRoot().getX2();
                  	break;
                  case "=":
                  	x1 = (String) (arrSQLTerms[k]._objValue);
                  	x2 = (String) (arrSQLTerms[k]._objValue);
                  	break;
                  case "!=":
                  	removex=true;
                  	rx = (String) (arrSQLTerms[k]._objValue);
                  	s1 = arrSQLTerms[k]._strColumnName;
                  	break;	
  	    		}
  	   break;
  	    	case "java.lang.date":
  	    		switch (arrSQLTerms[k]._strOperator) {
  	    		
  	    		case ">": 
                  	x1 =  addSecondsToDate(((Date) (arrSQLTerms[k]._objValue)), 1);
                  	x2 = (Date) tree.getRoot().getX2();
                  	break;
                  case "<":
                  	x1 = (Date) tree.getRoot().getX1();
                  	x2 = (Date) addSecondsToDate(((Date) (arrSQLTerms[k]._objValue)), -1);   
                  	break;
                  case "<=":
                  	x1 = (Date) tree.getRoot().getX1();
                  	x2 = (Date)(arrSQLTerms[k]._objValue);
                  	break;
                  case ">=":
                  	x1 = (Date) (arrSQLTerms[k]._objValue);
                  	x2 = (Date) tree.getRoot().getX2();
                  	break;
                  case "=":
                  	x1 = (Date) (arrSQLTerms[k]._objValue);
                  	x2 = (Date) (arrSQLTerms[k]._objValue);
                  	break;
                  case "!=":
                  	removex=true;
                  	rx = (Date) (arrSQLTerms[k]._objValue);
                  	s1 = arrSQLTerms[k]._strColumnName;
                  	break;	
  	    		}
  	    		
  	    		}
  	    		
  	    		
  	    		};
  	    		if(k == 1) {
  	    	    	switch (dataTypes.get(arrSQLTerms[k]._strColumnName).toLowerCase()) {
  	    	    	case "java.lang.integer":
  	    	    		switch (arrSQLTerms[k]._strOperator) {
  	    	    		
  	    	    		case ">": 
  	                    	y1 = (Integer) (arrSQLTerms[k]._objValue) + 1;
  	                    	y2 = (Integer) tree.getRoot().getY2();
  	                    	break;
  	                    case "<":
  	                    	y1 = (Integer) tree.getRoot().getY1();
  	                    	y2 = (Integer) (arrSQLTerms[k]._objValue) - 1;
  	                    	break;
  	                    case "<=":
  	                    	y1 = (Integer) tree.getRoot().getY1();
  	                    	y2 = (arrSQLTerms[k]._objValue);
  	                    	break;
  	                    case ">=":
  	                    	y1 = (Integer) (arrSQLTerms[k]._objValue);
  	                    	y2 = (Integer) tree.getRoot().getY2();
  	                    	break;
  	                    case "=":
  	                    	y1 = (Integer) (arrSQLTerms[k]._objValue);
  	                    	y2 = (Integer) (arrSQLTerms[k]._objValue);
  	                    	break;
  	                    case "!=":
  	                    	removey=true;
  	                    	ry = (Integer) (arrSQLTerms[k]._objValue);
  	                    	s2 = arrSQLTerms[k]._strColumnName;
  	                    	break;	
  	                    	
  	    	    		}
  	    	   break;
  	    	    	case "java.lang.double":
  	    	    		switch (arrSQLTerms[k]._strOperator) {
  	    	    		
  	    	    		case ">": 
  	                    	y1 = (double) (arrSQLTerms[k]._objValue) + 0.0000001;
  	                    	y2 = (double) tree.getRoot().getY2();
  	                    	break;
  	                    case "<":
  	                    	y1 = (double) tree.getRoot().getY1();
  	                    	y2 = (double) (arrSQLTerms[k]._objValue) - 0.0000001;
  	                    	break;
  	                    case "<=":
  	                    	y1 = (double) tree.getRoot().getY1();
  	                    	y2 = (double)(arrSQLTerms[k]._objValue);
  	                    	break;
  	                    case ">=":
  	                    	y1 = (double) (arrSQLTerms[k]._objValue);
  	                    	y2 = (double) tree.getRoot().getY2();
  	                    	break;
  	                    case "=":
  	                    	y1 = (double) (arrSQLTerms[k]._objValue);
  	                    	y2 = (double) (arrSQLTerms[k]._objValue);
  	                    	break;
  	                    case "!=":
  	                    	removey=true;
  	                    	ry = (double) (arrSQLTerms[k]._objValue);
  	                    	s2 = arrSQLTerms[k]._strColumnName;
  	                    	break;	
  	    	    		}
  	    	   break;
  	    	    	case "java.lang.string":
  	    	    		switch (arrSQLTerms[k]._strOperator) {
  	    	    		
  	    	    		case ">": 
  	                    	y1 = (String) (arrSQLTerms[k]._objValue) + "a";
  	                    	y2 = (String) tree.getRoot().getY2();
  	                    	break;
  	                    case "<":
  	                    	y1 = (String) tree.getRoot().getY1();
  	                    	y2 = getPrequel((String) (arrSQLTerms[k]._objValue)); 
  	                    	break;
  	                    case "<=":
  	                    	y1 = (String) tree.getRoot().getY1();
  	                    	y2 = (String)(arrSQLTerms[k]._objValue);
  	                    	break;
  	                    case ">=":
  	                    	y1 = (String) (arrSQLTerms[k]._objValue);
  	                    	y2 = (String) tree.getRoot().getY2();
  	                    	break;
  	                    case "=":
  	                    	y1 = (String) (arrSQLTerms[k]._objValue);
  	                    	y2 = (String) (arrSQLTerms[k]._objValue);
  	                    	break;
  	                    case "!=":
  	                    	removey=true;
  	                    	ry = (String) (arrSQLTerms[k]._objValue);
  	                    	s2 = arrSQLTerms[k]._strColumnName;
  	                    	break;
  	    	    		}
  	    	   break;
  	    	    	case "java.lang.date":
  	    	    		switch (arrSQLTerms[k]._strOperator) {
  	    	    		
  	    	    		case ">": 
  	                    	y1 = addSecondsToDate(((Date) (arrSQLTerms[k]._objValue)), 1);;  
  	                    	y2 = (Date) tree.getRoot().getY2();
  	                    	break;
  	                    case "<":
  	                    	y1 = (Date) tree.getRoot().getY1();
  	                    	y2 = addSecondsToDate(((Date) (arrSQLTerms[k]._objValue)), -1);;    
  	                    	break;
  	                    case "<=":
  	                    	y1 = (Date) tree.getRoot().getY1();
  	                    	y2 = (Date)(arrSQLTerms[k]._objValue);
  	                    	break;
  	                    case ">=":
  	                    	y1 = (Date) (arrSQLTerms[k]._objValue);
  	                    	y2 = (Date) tree.getRoot().getY2();
  	                    	break;
  	                    case "=":
  	                    	y1 = (Date) (arrSQLTerms[k]._objValue);
  	                    	y2 = (Date) (arrSQLTerms[k]._objValue);
  	                    	break;
  	                    case "!=":
  	                    	removey=true;
  	                    	ry = (Date) (arrSQLTerms[k]._objValue);
  	                    	s2 = arrSQLTerms[k]._strColumnName;
  	                    	break;	
  	    	    		}
  	    	    		
  	    	    		}
  	    	    		
  	    	    		
  	    	    		};
  	    		
  	    		
  	    		
  	    	    		if(k == 2) {
  	    	    	    	switch (dataTypes.get(arrSQLTerms[k]._strColumnName).toLowerCase()) {
  	    	    	    	case "java.lang.integer":
  	    	    	    		switch (arrSQLTerms[k]._strOperator) {
  	    	    	    		
  	    	    	    		case ">": 
  	    	                    	z1 = (Integer) (arrSQLTerms[k]._objValue) + 1;
  	    	                    	z2 = (Integer) tree.getRoot().getZ2();
  	    	                    	break;
  	    	                    case "<":
  	    	                    	z1 = (Integer) tree.getRoot().getZ1();
  	    	                    	z2 = (Integer) (arrSQLTerms[k]._objValue) - 1;
  	    	                    	break;
  	    	                    case "<=":
  	    	                    	z1 = (Integer) tree.getRoot().getZ1();
  	    	                    	z2 = (arrSQLTerms[k]._objValue);
  	    	                    	break;
  	    	                    case ">=":
  	    	                    	z1 = (Integer) (arrSQLTerms[k]._objValue);
  	    	                    	z2 = (Integer) tree.getRoot().getZ2();
  	    	                    	break;
  	    	                    case "=":
  	    	                    	z1 = (Integer) (arrSQLTerms[k]._objValue);
  	    	                    	z2 = (Integer) (arrSQLTerms[k]._objValue);
  	    	                    	break;
  	    	                    case "!=":
  	    	                    	removez=true;
  	    	                    	rz = (Integer) (arrSQLTerms[k]._objValue);
  	    	                    	s3 = arrSQLTerms[k]._strColumnName;
  	    	                    	break;
  	    	    	    		}
  	    	    	   break;
  	    	    	    	case "java.lang.double":
  	    	    	    		switch (arrSQLTerms[k]._strOperator) {
  	    	    	    		
  	    	    	    		case ">": 
  	    	                    	z1 = (double) (arrSQLTerms[k]._objValue) + 0.0000001;
  	    	                    	z2 = (double) tree.getRoot().getZ2();
  	    	                    	break;
  	    	                    case "<":
  	    	                    	z1 = (double) tree.getRoot().getZ1();
  	    	                    	z2 = (double) (arrSQLTerms[k]._objValue) - 0.0000001;
  	    	                    	break;
  	    	                    case "<=":
  	    	                    	z1 = (double) tree.getRoot().getZ1();
  	    	                    	z2 = (double)(arrSQLTerms[k]._objValue);
  	    	                    	break;
  	    	                    case ">=":
  	    	                    	z1 = (double) (arrSQLTerms[k]._objValue);
  	    	                    	z2 = (double) tree.getRoot().getZ2();
  	    	                    	break;
  	    	                    case "=":
  	    	                    	z1 = (double) (arrSQLTerms[k]._objValue);
  	    	                    	z2 = (double) (arrSQLTerms[k]._objValue);
  	    	                    	break;
  	    	                    case "!=":
  	    	                    	removez=true;
  	    	                    	rz = (double) (arrSQLTerms[k]._objValue);
  	    	                    	s3 = arrSQLTerms[k]._strColumnName;
  	    	                    	break;
  	    	    	    		}
  	    	    	   break;
  	    	    	    	case "java.lang.string":
  	    	    	    		switch (arrSQLTerms[k]._strOperator) {
  	    	    	    		
  	    	    	    		case ">": 
  	    	                    	z1 = (String) (arrSQLTerms[k]._objValue) + "a";
  	    	                    	z2 = (String) tree.getRoot().getZ2();
  	    	                    	break;
  	    	                    case "<":
  	    	                    	z1 = (String) tree.getRoot().getZ1();
  	    	                    	z2 = getPrequel((String) (arrSQLTerms[k]._objValue));    
  	    	                    	break;
  	    	                    case "<=":
  	    	                    	z1 = (String) tree.getRoot().getZ1();
  	    	                    	z2 = (String)(arrSQLTerms[k]._objValue);
  	    	                    	break;
  	    	                    case ">=":
  	    	                    	z1 = (String) (arrSQLTerms[k]._objValue);
  	    	                    	z2 = (String) tree.getRoot().getZ2();
  	    	                    	break;
  	    	                    case "=":
  	    	                    	z1 = (String) (arrSQLTerms[k]._objValue);
  	    	                    	z2 = (String) (arrSQLTerms[k]._objValue);
  	    	                    	break;
  	    	                    case "!=":
  	    	                    	removez=true;
  	    	                    	rz = (String) (arrSQLTerms[k]._objValue);
  	    	                    	s3 = arrSQLTerms[k]._strColumnName;
  	    	                    	break;
  	    	    	    		}
  	    	    	   break;
  	    	    	    	case "java.lang.date":
  	    	    	    		switch (arrSQLTerms[k]._strOperator) {
  	    	    	    		
  	    	    	    		case ">": 
  	    	                    	z1 = addSecondsToDate(((Date) (arrSQLTerms[k]._objValue)), 1);;   
  	    	                    	z2 = (Date) tree.getRoot().getZ2();
  	    	                    	break;
  	    	                    case "<":
  	    	                    	z1 = (Date) tree.getRoot().getZ1();
  	    	                    	z2 = addSecondsToDate(((Date) (arrSQLTerms[k]._objValue)), -1);;    
  	    	                    	break;
  	    	                    case "<=":
  	    	                    	z1 = (Date) tree.getRoot().getZ1();
  	    	                    	z2 = (Date)(arrSQLTerms[k]._objValue);
  	    	                    	break;
  	    	                    case ">=":
  	    	                    	z1 = (Date) (arrSQLTerms[k]._objValue);
  	    	                    	z2 = (Date) tree.getRoot().getZ2();
  	    	                    	break;
  	    	                    case "=":
  	    	                    	z1 = (Date) (arrSQLTerms[k]._objValue);
  	    	                    	z2 = (Date) (arrSQLTerms[k]._objValue);
  	    	                    	break;
  	    	                    case "!=":
  	    	                    	removez=true;
  	    	                    	rz = (Date) (arrSQLTerms[k]._objValue);
  	    	                    	s3 = arrSQLTerms[k]._strColumnName;
  	    	                    	break;
  	    	    	    		}
  	    	    	    		
  	    	    	    		}
  	    	    	    		
  	    	    	    		};
  	    		
  	    		
  	    		
  	    		
  	    	}	
  	    		
  	    	TreeSet<Integer> pagesId = tree.select(x1, y1, z1, x2, y2, z2);
  	    	Vector<Hashtable<String, Object>> result = new Vector<>();
  	    	
  	    	
  	    	for (Integer id : pagesId)
  	    	{
  	    		
  	    		String pagePath = "src/main/resources/data/Tables/"+arrSQLTerms[0]._strTableName+"/Page"+id+".ser";
                Page targetPage = (Page) Deserialize(pagePath);
            	Serialize(pagePath, targetPage);
            	
                Vector<Hashtable<String, Object>> records = targetPage.getRecords();
                
                for (int i = 0; i < records.size(); i++)
                {
                	Hashtable <String, Object> tuple = records.get(i);
                	
					if(removex) {
						
						if (records.size() == 0) break;
						
						if(tuple.get(s1).toString().equals(rx.toString())) {
							records.remove(i);
							i--;
						}
					}
					
					if(removey) {
						
						if (records.size() == 0) break;
						
						if(tuple.get(s2).toString().equals(ry.toString())) {
							records.remove(i);
							i--;
						}
					}
					
					if(removez) {
						
						if (records.size() == 0) break;
						
						if(tuple.get(s3).toString().equals(rz.toString())) {
							records.remove(i);
							i--;
						}
					}
                }
                
                result.addAll(records);
  	    	}
  	    		
  	    	return result.iterator();
  	    	
  	    }
		return new Vector<>().iterator();
  	
  	}

    public Iterator selectLinearly(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException
    {
    	
    	String clusteringKey = (String) readFromCSV(arrSQLTerms[0]._strTableName)[0];
        Hashtable<String,String> dataTypes = (Hashtable) readFromCSV(arrSQLTerms[0]._strTableName)[2];
        Hashtable<String,Object> minValues= (Hashtable) readFromCSV(arrSQLTerms[0]._strTableName)[3];
        Hashtable<String,Object> maxValues = (Hashtable) readFromCSV(arrSQLTerms[0]._strTableName)[4];
    
        //insert logic of select
	    for(int i=0;i<arrSQLTerms.length;i++) {       	
	        if (readFromCSV(arrSQLTerms[i]._strTableName).equals(null))
	            throw new DBAppException("Table" + arrSQLTerms[i]._strTableName + "Does Not Exist");
	    }

	        	        
	        String tablePath = "src/main/resources/data/Tables/"+arrSQLTerms[0]._strTableName+"/"+arrSQLTerms[0]._strTableName+".ser";
	        Table table = (Table) Deserialize(tablePath);
	        Vector<Object> vector = new Vector<>();
            Vector temp = new Vector();
            Collections.addAll(temp, strarrOperators);
	        
	        if (table.getNoPages() > 0) {
	        	for(int j=0;j<table.getPagesId().size();j++) {
	        		
	        		String tableName = arrSQLTerms[j]._strTableName;
		            String col=arrSQLTerms[j]._strColumnName;
		            String operation=arrSQLTerms[j]._strOperator;
		            Object value=arrSQLTerms[j]._objValue;
		            Hashtable<String,Object> colval= new Hashtable<String,Object>();
	                colval.put(col, value);
	               
	                int id = table.getPagesId().get(j);
	                String path = "src/main/resources/data/Tables/"+arrSQLTerms[0]._strTableName+"/Page"+id+".ser";
	                Page targetPage = (Page) Deserialize(path);
	            	Serialize(path, targetPage);
	               
	                if(table.getNoPages()>0) {
			           
		                for(int j2=0; j2<targetPage.getRecords().size();j2++) {
		                Hashtable<String, Object> item =targetPage.getRecords().get(j2);
		    	        Vector<String> operations = (Vector<String>) temp.clone();
		    	        boolean[] flag = new boolean[arrSQLTerms.length];
		    	       
		    	        

	                for (int k = 0; k < arrSQLTerms.length; k++) {
	                    SQLTerm sqlTerm = arrSQLTerms[k];
	                    int t=k;
	                    switch (dataTypes.get(sqlTerm._strColumnName).toLowerCase()) {
	                        case "java.lang.integer":
	                            switch (sqlTerm._strOperator) {
	                                case ">":
	                                    if (Integer.parseInt(item.get((sqlTerm._strColumnName)).toString()) > Integer.parseInt(sqlTerm._objValue.toString()))
	                                        flag[k] = true;
	                                    break;
	                                case "<":
	                                    if (Integer.parseInt(item.get((sqlTerm._strColumnName)).toString()) < Integer.parseInt(sqlTerm._objValue.toString()))
	                                        flag[k] = true;
	                                    break;
	                                case ">=":
	                                    if (Integer.parseInt(item.get((sqlTerm._strColumnName)).toString()) > Integer.parseInt(sqlTerm._objValue.toString())
	                                    		|| Integer.parseInt(item.get((sqlTerm._strColumnName)).toString()) == Integer.parseInt(sqlTerm._objValue.toString()))
	                                        flag[k] = true;
	                                    break;
	                                case "<=":
	                                    if (Integer.parseInt(item.get((sqlTerm._strColumnName)).toString()) < Integer.parseInt(sqlTerm._objValue.toString())
	                                    		|| Integer.parseInt(item.get((sqlTerm._strColumnName)).toString()) == Integer.parseInt(sqlTerm._objValue.toString()))
	                                        flag[k] = true;
	                                    break;
	                                case "=":
	                                    if (Integer.parseInt(item.get((sqlTerm._strColumnName)).toString()) == Integer.parseInt(sqlTerm._objValue.toString()))
	                                        flag[k] = true;
	                                    break;
	                                case "!=":
	                                    if (Integer.parseInt(item.get((sqlTerm._strColumnName)).toString()) != Integer.parseInt(sqlTerm._objValue.toString()))
	                                        flag[k] = true;
	                                    break;
	                                
	        	}break;
	                        case "java.lang.double":
	                            switch (sqlTerm._strOperator) {
	                                case ">":
	                                    if (Double.compare(
	                                            Double.parseDouble(item.get((sqlTerm._strColumnName)).toString()),
	                                            Double.parseDouble(sqlTerm._objValue.toString())) > 0)
	                                        flag[k] = true;
	                                    break;
	                                case "<":
	                                    if (Double.compare(
	                                            Double.parseDouble(item.get((sqlTerm._strColumnName)).toString()),
	                                            Double.parseDouble(sqlTerm._objValue.toString())) < 0)
	                                        flag[k] = true;
	                                    break;
	                                case ">=":
	                                    if (Double.compare(
	                                            Double.parseDouble(item.get((sqlTerm._strColumnName)).toString()),
	                                            Double.parseDouble(sqlTerm._objValue.toString())) > 0 
	                                    		||  Double.compare(
	    	                                            Double.parseDouble(item.get((sqlTerm._strColumnName)).toString()),
	    	                                            Double.parseDouble(sqlTerm._objValue.toString())) == 0)
	                                        flag[k] = true;
	                                    break;
	                                case "<=":
	                                    if (Double.compare(
	                                            Double.parseDouble(item.get((sqlTerm._strColumnName)).toString()),
	                                            Double.parseDouble(sqlTerm._objValue.toString())) < 0 
	                                    		||  Double.compare(
	    	                                            Double.parseDouble(item.get((sqlTerm._strColumnName)).toString()),
	    	                                            Double.parseDouble(sqlTerm._objValue.toString())) == 0)
	                                        flag[k] = true;
	                                    break;
	                                case "=":
	                                    if (Double.compare(
	                                            Double.parseDouble(item.get((sqlTerm._strColumnName)).toString()),
	                                            Double.parseDouble(sqlTerm._objValue.toString())) == 0)
	                                        flag[k] = true;
	                                    break;
	                                case "!=":
	                                    if (Double.compare(
	                                            Double.parseDouble(item.get((sqlTerm._strColumnName)).toString()),
	                                            Double.parseDouble(sqlTerm._objValue.toString())) != 0)
	                                        flag[k] = true;
	                                    break;
	        	
	        	
	        }break;
	                
	                        case "java.util.date":
	                            switch (sqlTerm._strOperator) {
	                                case ">":
	                                    if (((Date)(item.get((sqlTerm._strColumnName))))
	                                            .compareTo((Date)sqlTerm._objValue) > 0)
	                                        flag[k] = true;
	                                    break;
	                                case "<":
	                                    if (((Date)(item.get((sqlTerm._strColumnName))))
	                                            .compareTo((Date)sqlTerm._objValue) < 0)
	                                        flag[k] = true;
	                                    break;
	                                case ">=":
	                                    if ((((Date)(item.get((sqlTerm._strColumnName))))
	                                            .compareTo((Date)sqlTerm._objValue) > 0
	                                            || ((Date)(item.get((sqlTerm._strColumnName))))
	                                                    .compareTo((Date)sqlTerm._objValue) == 0))
	                                        flag[k] = true;
	                                    break;
	                                case "<=":
	                                    if ((((Date)(item.get((sqlTerm._strColumnName))))
	                                            .compareTo((Date)sqlTerm._objValue) < 0
	                                            || ((Date)(item.get((sqlTerm._strColumnName))))
	                                                    .compareTo((Date)sqlTerm._objValue) == 0))
	                                        flag[k] = true;
	                                    break;
	                                case "=":
	                                    if (((Date)(item).get((sqlTerm._strColumnName)))
	                                            .compareTo((Date)sqlTerm._objValue) == 0)
	                                        flag[k] = true;
	                                    break;
	                                case "!=":
	                                    if (((Date)(item).get((sqlTerm._strColumnName)))
	                                            .compareTo((Date)sqlTerm._objValue) != 0)
	                                        flag[k] = true;
	                                    break;

	                                    
	        }break;
	                        case "java.lang.string":
	                            switch (sqlTerm._strOperator) {
	                                case ">":
	                                    if ((item.get((sqlTerm._strColumnName)).toString()
	                                            .compareTo(sqlTerm._objValue.toString())) > 0)
	                                        flag[k] = true;
	                                    break;
	                                case "<":
	                                    if ((item.get((sqlTerm._strColumnName)).toString()
	                                            .compareTo(sqlTerm._objValue.toString())) < 0)
	                                        flag[k] = true;
	                                    break;
	                                case ">=":
	                                    if (((item.get((sqlTerm._strColumnName)).toString()
	                                            .compareTo(sqlTerm._objValue.toString())) > 0
	                                            || item.get((sqlTerm._strColumnName)).toString()
	                                                    .compareTo(sqlTerm._objValue.toString()) == 0))
	                                        flag[k] = true;
	                                    break;
	                                case "<=":
	                                    if (((item).get((sqlTerm._strColumnName)).toString()
	                                            .compareTo(sqlTerm._objValue.toString())) < 0
	                                            || item.get((sqlTerm._strColumnName)).toString()
	                                                    .compareTo(sqlTerm._objValue.toString()) == 0)
	                                        flag[k] = true;
	                                    break;
	                                case "=":
	                                    if ((item.get((sqlTerm._strColumnName)).toString()
	                                            .compareTo(sqlTerm._objValue.toString())) == 0)
	                                        flag[k] = true;
	                                    break;
	                                case "!=":
	                                    if ((item.get((sqlTerm._strColumnName)).toString()
	                                            .compareTo(sqlTerm._objValue.toString())) != 0)
	                                        flag[k] = true;
	                                    break;

	        }break;
	                    }
	                
	            

	                Vector<Boolean> results = new Vector<>();
	                for (int i = 0; i < flag.length; i++) {
	                    results.add(flag[i]);
	                }
	                for(int i = 0; i < results.size()-1; i++){
	                    int tempo = 0;
	                    if(operations.size() > 1)
	                        tempo = i;
	                    if(operations.get(tempo).equals("AND")){
	                        results.set(i,results.get(i) & results.get(i+1));
	                        results.remove(i+1);
	                        if(operations.size() > 1)
	                        	operations.remove(tempo);
	                        i--;
	                    }
	                }for(int i = 0; i < results.size()-1; i++){
	                    int tempo = 0;
	                    if(operations.size() > 1)
	                    	tempo = i;
	                    if(operations.get(tempo).equals("XOR")){
	                        results.set(i,results.get(tempo) ^ results.get(i+1));
	                        results.remove(i+1);
	                        if(operations.size() > 1)
	                        	operations.remove(tempo);
	                        i--;
	                    }
	                }
	                for(int i = 0; i < results.size()-1; i++){
	                    int tempo = 0;
	                    if(operations.size() > 1)
	                    	tempo = i;
	                    if(operations.get(tempo).equals("OR")){
	                        results.set(i,results.get(i) | results.get(i+1));
	                        results.remove(i+1);
	                        if(operations.size() > 1)
	                        	operations.remove(tempo);
	                        i--;
	                    }
	                }
	                
	                if (results.contains(true) && !vector.contains(item))
	                    vector.add(item);
	                }
	                
	            }
	                }
	        	}
	        	
	                
	        }
	        return vector.iterator();
	    }

    

    public void validateArrSQLTerms(SQLTerm[] arrSQLTerms) throws DBAppException {
        String tableName = arrSQLTerms[0]._strTableName;

        for (int i = 1; i < arrSQLTerms.length; i++)
        {
            if (!(arrSQLTerms[1]._strTableName).equals(tableName))
            {
                throw new DBAppException("Different table names are passed ");
            }
        }

        Hashtable<String, Object> record = new Hashtable<>();
        for (int i = 0; i < arrSQLTerms.length; i++)
        {
            record.put(arrSQLTerms[i]._strColumnName, arrSQLTerms[i]._objValue);
        }

        Object[] tableInfo = readFromCSV(tableName);

        Hashtable<String,String> dataTypes = (Hashtable) tableInfo[2];
        Hashtable<String,Object> minValues= (Hashtable) tableInfo[3];
        Hashtable<String,Object> maxValues = (Hashtable) tableInfo[4];

        checkColDataTypes(dataTypes, record);
        checkColCompatibility(dataTypes, record);
        checkRange(dataTypes, minValues, maxValues, record);

    }

    public void validateStrarrOperators(String[] strarrOperators) throws DBAppException {
        for (String str : strarrOperators)
        {
            if (!str.equals("OR") && !str.equals("AND") && !str.equals("XOR"))
            {
                throw new DBAppException("strarrOperators contains invalid operators");
            }
        }
    }

    public Vector<Hashtable<String, Object>> deleteRecordsFromPage (Vector<Hashtable<String,Object>> records, Hashtable<String,Object> htblColNameValue, Vector<String> availableIndices) throws DBAppException {

        for (int i = 0; i < records.size(); i++)
        {
            Hashtable<String,Object> targetRecord = records.get(i);
            boolean delete = true;
            for (String key : htblColNameValue.keySet())
            {
                if ( ((htblColNameValue.get(key) instanceof DBAppNull) && !(targetRecord.get(key) instanceof DBAppNull))
                        || (!(htblColNameValue.get(key) instanceof DBAppNull) && (targetRecord.get(key) instanceof DBAppNull))
                        || compare(htblColNameValue.get(key),targetRecord.get(key)) != 0)
                {
                    delete = false;
                    break;
                }

            }
            if (delete){

                records.remove(i);
                i--;
                //to delete record from the existing indices
                deleteFromIndex(availableIndices, targetRecord, null, false);
            }

        }

        return records;
    }

    public Vector<Hashtable<String, Object>> deleteRecordsByBinarySearch(Vector<Hashtable<String,Object>> records, Hashtable<String, Object> htblColNameValue, String clusteringkey, Object clusteringObject, Vector<String> availableIndices) throws DBAppException {

        int lo = 0;
        int hi = records.size() - 1;

        while (lo <= hi) {

            int mid = (lo + hi) / 2;
            Hashtable<String, Object> targetRecord = records.get(mid);

            if (compare(clusteringObject,targetRecord.get(clusteringkey)) == 0)
            {
                boolean delete = true;
                for (String key : htblColNameValue.keySet())
                {
                    if ( ((htblColNameValue.get(key) instanceof DBAppNull) && !(targetRecord.get(key) instanceof DBAppNull))
                            || (!(htblColNameValue.get(key) instanceof DBAppNull) && (targetRecord.get(key) instanceof DBAppNull))
                            || compare(htblColNameValue.get(key),targetRecord.get(key)) != 0)
                    {
                        delete = false;
                        break;
                    }

                }

                if (delete){

                    records.remove(mid);
                    //to delete record from the existing indices
                    deleteFromIndex(availableIndices, targetRecord, clusteringObject, true);
                }

                return records;
            }
            else if (compare(clusteringObject,records.get(mid).get(clusteringkey)) < 0)
            {
                hi = mid - 1;
            }
            else
            {
                lo = mid + 1;
            }
        }

        return records;
    }

    public Vector<String> getAvailableIndices (String strTableName,Table table)
    {
        Vector<String> res = new Vector<>();
        Vector<Integer> indicesId = table.getIndicesId();

        for (Integer id : indicesId)
        {
            String path = "src/main/resources/data/Tables/"+strTableName+"/Indices/index"+id+".ser";
            res.add(path);
        }

        return res;

    }

    public int searchForPageToInsert (Vector<Integer> pagesID,String strTableName,Object clusteringKey) throws DBAppException {

        for (int i = 0; i < pagesID.size(); i++)
        {
            int index = pagesID.get(i);
            String path = "src/main/resources/data/Tables/"+strTableName+"/Page"+index+".ser";
            Page p = (Page) Deserialize(path);
            Serialize(path,p);

            if (compare(clusteringKey,p.getMinClusteringKey()) >= 0 && compare(p.getMaxClusteringKey(),clusteringKey) >= 0) return index;
            else if (compare(p.getMinClusteringKey(),clusteringKey) >= 0)
            {
                if (i == 0) return index;
                else
                {
                    String targetPath = "src/main/resources/data/Tables/"+strTableName+"/Page"+pagesID.get(i - 1)+".ser";
                    Page targetPage = (Page) Deserialize(targetPath);
                    Serialize(targetPath, targetPage);
                    if (isFull(targetPage.getRecords())) return index;
                    else return pagesID.get(i-1);
                }
            }
        }

        return pagesID.lastElement();
    }

    public int searchForRecord(Vector<Hashtable<String, Object>> records,Object clusteringKey,String primaryKey) throws DBAppException {

        int lo = 0;
        int hi = records.size() - 1;

        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            if (compare(clusteringKey,records.get(mid).get(primaryKey)) == 0) {
                throw new DBAppException("Primary key already exits");
            } else if (compare(clusteringKey,records.get(mid).get(primaryKey)) < 0) {
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return hi + 1 ;
    }

    public int searchForRecordToUpdate(Vector<Hashtable<String, Object>> records, String clusteringKey,Object clusteringObject) {

        int lo = 0;
        int hi = records.size() - 1;

        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            if (compare(clusteringObject, records.get(mid).get(clusteringKey)) == 0) {
                return mid;
            } else if (compare(clusteringObject, records.get(mid).get(clusteringKey)) < 0) {
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return -1;
    }

    public int binarySearchOnPages (String strTableName,Vector<Integer> pagesId, Object clusteringObject) throws DBAppException {

        int lo = 0;
        int hi = pagesId.size() - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) / 2;
            int pageId = pagesId.get(mid);
            String path = "src/main/resources/data/Tables/"+strTableName+"/Page"+pageId+".ser";
            Page p = (Page) Deserialize(path);
            Serialize(path, p);

            if (compare(clusteringObject, p.getMinClusteringKey()) >= 0 && compare(clusteringObject, p.getMaxClusteringKey()) <= 0)
            {
                return mid;
            }
            else if (compare(clusteringObject, p.getMinClusteringKey()) <= 0)
            {
                hi = mid - 1;
            }
            else
            {
                lo = mid + 1;
            }
        }
        return -1;
    }

    public boolean isFull(Vector<Hashtable<String, Object>> records)
    {
        return records.size() >= MaximumRowsCountinTablePage;
    }

    public int compare (Object o1,Object o2)
    {
        if (o1 instanceof java.lang.Integer && o2 instanceof java.lang.Integer) return ((Integer)o1).compareTo((Integer)o2);
        else if (o1 instanceof java.lang.Double && o2 instanceof java.lang.Double) return ((Double)o1).compareTo((Double)o2);
        else if (o1 instanceof java.lang.String && o2 instanceof java.lang.String) return ((String)o1).compareTo((String)o2);
        else if (o1 instanceof DBAppNull && o2 instanceof DBAppNull) return ((DBAppNull)o1).compareTo((DBAppNull)o2);
        else return ((Date)o1).compareTo((Date)o2);

    }

    //to know maximum no of records per page
    public void readConfig()
    {
        Properties prop = new Properties();
        String filename = "src/main/resources/DBApp.config";
        try (FileInputStream fis = new FileInputStream(filename))
        {
            prop.load(fis);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        MaximumRowsCountinTablePage = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
        MaximumEntriesinOctreeNode = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));
    }

    public Object[] readFromCSV (String strTableName) throws DBAppException {
        String clusteringKey = "", clusteringType = "";
        Hashtable<String,String> dataTypes = new Hashtable<>();
        Hashtable<String,Object> minValues = new Hashtable<>();
        Hashtable<String,Object> maxValues = new Hashtable<>();
        boolean flag = false;
        try {
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            String line = "";
            while ((line = br.readLine()) != null) {
                String[] data = line.split(",");

                if (data[0].equals(strTableName)) {

                    dataTypes.put(data[1], data[2]);
                    flag = true;

                    if (data[3].equals("true")) {
                        clusteringKey = data[1];
                        clusteringType = data[2];
                    }

                    switch (data[2]) {
                        case "java.lang.Integer":
                            minValues.put(data[1], Integer.parseInt(data[6]));
                            maxValues.put(data[1], Integer.parseInt(data[7]));
                            break;
                        case "java.lang.Double":
                            minValues.put(data[1], Double.parseDouble(data[6]));
                            maxValues.put(data[1], Double.parseDouble(data[7]));
                            break;
                        case "java.util.Date":
                            try {
                                minValues.put(data[1], new SimpleDateFormat("yyyy-MM-dd").parse(data[6]));
                                maxValues.put(data[1], new SimpleDateFormat("yyyy-MM-dd").parse(data[7]));
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            break;
                        default:
                            minValues.put(data[1], data[6]);
                            maxValues.put(data[1], data[7]);
                            break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (!flag) throw new DBAppException("Table does not exist");

        return new Object[]{clusteringKey,clusteringType,dataTypes,minValues,maxValues};
    }

    public void checkColDataTypes (Hashtable<String,String> dataTypes,Hashtable<String,Object> htblColNameValue) throws DBAppException {

        for (String key : htblColNameValue.keySet())
        {
            if (htblColNameValue.get(key) instanceof DBAppNull) continue;

            if (!dataTypes.containsKey(key)) throw new DBAppException("Column does not exist");
        }
    }

    public void checkColCompatibility(Hashtable<String,String> dataTypes,Hashtable<String,Object> htblColNameValue) throws DBAppException {

        for (String key : htblColNameValue.keySet())
        {
            if (htblColNameValue.get(key) instanceof DBAppNull) continue;
            Class col;
            try {
                col = Class.forName(dataTypes.get(key));
            } catch (ClassNotFoundException e) {
                throw new DBAppException("checkColCompatibility method");
            }

            if (!col.isInstance(htblColNameValue.get(key))) throw new DBAppException("Column data type is incompatible");
        }
    }

    public void checkRange(Hashtable<String,String> dataTypes,Hashtable<String,Object> minValues, Hashtable<String,Object> maxValues, Hashtable<String,Object> htblColNameValue) throws DBAppException {

        for (String key : htblColNameValue.keySet())
        {
            if (htblColNameValue.get(key) instanceof DBAppNull) continue;

            switch (dataTypes.get(key))
            {
                case "java.lang.Integer":
                    if (((Integer) htblColNameValue.get(key)).compareTo((Integer) minValues.get(key)) < 0)
                    {
                        throw new DBAppException("The value "+ key + " is below the minimum "+ minValues.get(key));
                    }
                    if (((Integer) htblColNameValue.get(key)).compareTo((Integer) maxValues.get(key)) > 0)
                    {
                        throw new DBAppException("The value "+ key + " is above the maximum "+ maxValues.get(key));
                    }
                    break;
                case "java.lang.Double":
                    if (((Double) htblColNameValue.get(key)).compareTo((Double) minValues.get(key)) < 0)
                    {
                        throw new DBAppException("The value "+ key + " is below the minimum "+ minValues.get(key));
                    }
                    if (((Double) htblColNameValue.get(key)).compareTo((Double) maxValues.get(key)) > 0)
                    {
                        throw new DBAppException("The value "+ key + " is above the maximum "+ maxValues.get(key));
                    }
                    break;
                case "java.util.Date":
                    if (((Date) htblColNameValue.get(key)).compareTo((Date) minValues.get(key)) < 0)
                    {
                        throw new DBAppException("The value "+ key + " is below the minimum "+ minValues.get(key));
                    }
                    if (((Date) htblColNameValue.get(key)).compareTo((Date) maxValues.get(key)) > 0)
                    {
                        throw new DBAppException("The value "+ key + " is above the maximum "+ maxValues.get(key));
                    }
                    break;
                default:
                    if (((String) htblColNameValue.get(key)).compareTo((String) minValues.get(key)) < 0)
                    {
                        throw new DBAppException("The value "+ key + " is below the minimum "+ minValues.get(key));
                    }
                    if (((String) htblColNameValue.get(key)).compareTo((String) maxValues.get(key)) > 0)
                    {
                        throw new DBAppException("The value "+ key + " is above the maximum "+ maxValues.get(key));
                    }
                    break;
            }

        }
    }
    
    public static String getPrequel(String input) {
        StringBuilder prequel = new StringBuilder();

        // Iterate over each character in the input string
        for (int i = 0; i < input.length() - 1; i++) {
            char currentChar = input.charAt(i);

            // Append the current character to the prequel string
            prequel.append(currentChar);
        }

        // Get the last character in the input string
        char lastChar = input.charAt(input.length() - 1);

        // Replace the last character with the previous character in the alphabet
        char prequelChar;
        if (lastChar == 'a') {
            prequelChar = 'z'; // Wrap around to 'z' if last character is 'a'
        } else {
            prequelChar = (char) (lastChar - 1); // Get the previous character
        }

        // Append the modified character to the prequel string
        prequel.append(prequelChar);

        return prequel.toString();
    }
    
    public static Date addSecondsToDate(Date date, int secondsToAdd) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.SECOND, secondsToAdd);
        return calendar.getTime();
    }

    public boolean containsNull (Hashtable<String,Object> htblColNameValue)
    {
        for (Object obj : htblColNameValue.values())
        {
            if (obj instanceof DBAppNull) return true;
        }

        return false;
    }

    public void Serialize(String path , Object obj) throws DBAppException {
        try {
            FileOutputStream fileOut =
                    new FileOutputStream(path);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(obj);
            out.close();
            fileOut.close();
        } catch (IOException e) {
            throw new DBAppException("Serialize method exception");
        }
    }
    

    public Object Deserialize(String path) throws DBAppException {
        Object o;
        try {
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            o = objectIn.readObject();
            objectIn.close();
            fileIn.close();
        } catch (Exception e) {
            throw new DBAppException("Deserialize method exception");
        }
        return o;
    }

    public void printPages(String tableName) throws DBAppException {
        String path = "src/main/resources/data/Tables/"+tableName+"/"+tableName+".ser";
        Table t = (Table) Deserialize(path);
        Serialize(path,t);
        System.out.println("Page Content ");
        for (Integer index : t.getPagesId())
        {
            String pagePath = "src/main/resources/data/Tables/"+tableName+"/Page"+index+".ser";
            Page p = (Page) Deserialize(pagePath);
            Serialize(pagePath,p);
            System.out.println(p.getRecords().toString());
            System.out.println(p.getMaxClusteringKey());
            System.out.println(p.getMinClusteringKey());
        }
        System.out.println("Table contents");
        System.out.println(t.getNoPages()+" "+t.getPagesId().toString() + " Indices: " + t.getIndicesId().toString());
        System.out.println("Index Content");
        for (Integer index : t.getIndicesId())
        {
            System.out.println("------------------------------------Index"+index+"------------------------------------");
            String indexPath = "src/main/resources/data/Tables/"+tableName+"/Indices/index"+index+".ser";
            OctTree tree = (OctTree) Deserialize(indexPath);
            Serialize(indexPath,tree);
            tree.printTree();
        }
    }
    
    public static void main(String[] args) throws DBAppException {
	}
  

}