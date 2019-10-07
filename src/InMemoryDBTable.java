import javafx.util.Pair;

import java.util.*;

public class InMemoryDBTable {
    String tableName;
    private List<List<Object>> DBTable;
    private Map<String, String> colTypeMap;
    private Map<String, Integer> colIdxMap;
    private Map<String, PriorityQueue<Pair<Object, Integer>>> colOrderMap;
    private Set<Integer> deletedSet;

    /**
     * Create a table
     * SQL: createTable(String tableName, Array of (columnName, columnDataType))
     * @param tableName Table name
     * @param colNameType Array of (columnName, columnDataType)
     */
    public InMemoryDBTable(String tableName, List<Pair<String, String>> colNameType) {
        this.tableName = tableName;
        DBTable = new ArrayList<>();
        colOrderMap = new HashMap<>();
        colIdxMap = new HashMap<>();
        colTypeMap = new HashMap<>();
        deletedSet = new HashSet<>();
        for (int i = 0; i < colNameType.size(); i++) {
            Pair<String, String> currCol = colNameType.get(i);
            this.colIdxMap.put(currCol.getKey(), i);
            this.colTypeMap.put(currCol.getKey(), currCol.getValue());
            this.colOrderMap.put(currCol.getKey(), new PriorityQueue<Pair<Object, Integer>>(new Comparator<Pair<Object, Integer>>(){
                public int compare(Pair<Object, Integer> p1, Pair<Object, Integer> p2) {
                    Object o1 = p1.getKey();
                    Object o2 = p2.getKey();
                    if (o1 instanceof String && o2 instanceof String) {
                        return ((String) o1).compareTo((String)o2);
                    }
                    else if (o1 instanceof Integer && o2 instanceof Integer) {
                        return ((Integer) o1).compareTo((Integer) o2);
                    }
                    return -1;
                }
            }));
        }
    }

    /**
     * Insert row
     * SQL: InsertRow(Array_of(ColumnName, Value))
     * @param row Array of (columnName, value) to be inserted
     * @return Insert index in the table, -1 if fail
     */
    public int insertRow(List<Pair<String, Object>> row){
        List<Object> currRow = new ArrayList<>(colTypeMap.size());
        for (int i = 0; i < colTypeMap.size(); i++) {
            currRow.add(null);
        }
        for (Pair<String, Object> currValuePair: row) {
            String colName = currValuePair.getKey();
            Object colValue = currValuePair.getValue();
            // invalid col Name
            if (!colIdxMap.containsKey(colName)){
                return -1;
            }
            // invalid type
            String type = colTypeMap.get(colName);
            Class c = null;
            try {
                c = Class.forName("java.lang." + type);
            } catch (ClassNotFoundException e) {
                return -1;
            }
            if ( !c.isInstance(colValue) ){
                return -1;
            }
            currRow.set(colIdxMap.get(colName), colValue);
            // update colOrderMap
            PriorityQueue<Pair<Object, Integer>> pq= colOrderMap.get(colName);
            pq.add(new Pair<Object, Integer>(colValue, DBTable.size()));
            colOrderMap.put(colName, pq);
        }
        DBTable.add(currRow);
        return DBTable.size() - 1;
    }

    /**
     * Delete row
     * SQL: DeleteRow(RowKey) (RowKey: Same as primary_key as in relational DB)
     * @param rowKey The row index to be deleted
     * @return true if successfully deleted
     */
    public boolean deleteRow(int rowKey) {
        if ( rowKey >= DBTable.size() || deletedSet.contains(rowKey)) {
            return false;
        }
        deletedSet.add(rowKey);
        return true;
    }

    /**
     * Update row
     * SQL: UpdateRow(RowKey, Object(ColumnName, Value)) (RowKey: Same as primary_key as in relational DB)
     * @param rowKey The row index to be updated
     * @param updateValue (ColumnName, newValue)
     * @return true if successfully updated
     */
    public boolean updateRow(int rowKey, Pair<String, Object> updateValue){
        // invalid row
        if ( rowKey >= DBTable.size() || deletedSet.contains(rowKey)) {
            return false;
        }
        int colIdx = colIdxMap.get(updateValue.getKey());
        String colType = colTypeMap.get(updateValue.getKey());
        Object colValue = updateValue.getValue();
        // invalid type
        Class c = null;
        try {
            c = Class.forName("java.lang." + colType);
        } catch (ClassNotFoundException e) {
            return false;
        }
        if ( !c.isInstance(colValue) ){
            return false;
        }
        // update value Object on DBTable
        DBTable.get(rowKey).set(colIdx, colValue);
        // update value Object on colOrderMap
        PriorityQueue<Pair<Object, Integer>> pq= colOrderMap.get(updateValue.getKey());
        Object[] pqArray = pq.toArray();
        Pair<Object, Integer>[] newPQArray = new Pair[pq.size()];
        for (int i = 0; i < pqArray.length; i++ ){
            Pair<Object, Integer> pair = (Pair)pqArray[i];
            if (pair.getValue() == rowKey){
                newPQArray[i] = new Pair<Object, Integer>(colValue, rowKey);
            } else{
                newPQArray[i] = (Pair<Object, Integer>)pair;
            }
        }
        PriorityQueue<Pair<Object, Integer>> newPQ = new PriorityQueue<>(pq.comparator());
        newPQ.addAll(Arrays.asList(newPQArray));
        colOrderMap.put(updateValue.getKey(), newPQ);
        return true;
    }

    /**
     * Get all rows
     * @return all rows in the table
     */
    public List<List<Object>> getAllRows(){
        List<List<Object>> result = new ArrayList<>();
        for (int i = 0; i < DBTable.size(); i++){
            if (!deletedSet.contains(i)){
                result.add(DBTable.get(i));
            }
        }
        return result;
    }

    /**
     * Get specific count of rows from a table
     * SQL: Select * from tableName limit count;
     * @param limitCount Count of rows to be returned
     * @return limitCount of rows
     */
    public List<List<Object>> getNRows(int limitCount){
        if (limitCount < 0){
            return null;
        }
        List<List<Object>> result = new ArrayList<>();
        for (int i = 0; i < DBTable.size(); i++){
            if (limitCount <= 0) {
                break;
            }
            if (!deletedSet.contains(i)){
                result.add(DBTable.get(i));
                limitCount --;
            }
        }
        return result;
    }

    /**
     * SORT by a column, and GET specific count of rows from table
     * @param limitCount Count of rows to be returned
     * @param colName colName to be sorted on
     * @return limitCount of rows sorted on colName
     */
    public List<List<Object>> getNRowSorted(int limitCount, String colName){
        if (limitCount < 0){
            return null;
        }
        List<List<Object>> result = new ArrayList<>();
        PriorityQueue<Pair<Object, Integer>> pq= colOrderMap.get(colName);
        Object[] pqArray = pq.toArray();
        for (int i = 0; i < pqArray.length; i++) {
            if (limitCount <= 0){
                break;
            }
            Pair<Object, Integer> pair = (Pair)pqArray[i];
            if (!deletedSet.contains(pair.getValue())){
                result.add(DBTable.get(pair.getValue()));
                limitCount--;
            }
        }
        return result;
    }

    /**
     * GroupBy a column, and GET specific count of rows from table. This is the aggregate function.
     * SQL: select aggregate_key, count(*) from table group by aggregate_key;
     * @param colName to groupBy
     * @return count of rows
     */
    public List<Pair<Object, Integer>> countOfRowsGroupByKey(String colName){
        List<Pair<Object, Integer>> result = new ArrayList<>();
        int index = colIdxMap.get(colName);
        int size = DBTable.size() - deletedSet.size();
        for (int i = 0; i < DBTable.size(); i++){
            if (!deletedSet.contains(i)) {
                result.add(new Pair(DBTable.get(i).get(index), size));
            }
        }
        return result;
    }
}
