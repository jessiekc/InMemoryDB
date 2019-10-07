# In memory Database

## InMemoryDB.java
##### Database that contains DBTable
- Map<tableName, table>
##### Insert
- public InMemoryDBTable insertTable(InMemoryDBTable table)
- return the original Table with the same name in the map

## InMemoryDBTable.java
######  m = # of col, n = # of row
##### Database Table
- String tableName
- private List<List\<Object>> DBTable
    - each row is a entry
    - each object is a cell value
- private Map<String, String> colTypeMap;
    - <colName, colType>
- private Map<String, Integer> colIdxMap;
    - <colName, Index>
- private Map<String, PriorityQueue<Pair<Object, Integer>>> colOrderMap;
    - <colName, PQ<(value, Index)>>
- private Set<Integer> deletedSet;
    - Set\<deleted index>

##### Constructor
- Time: O(m) -- m col
- Space: O(m) -- m col 

##### Insert Row
- public int insertRow(List<Pair<String, Object>> row)
- Time: O(mlogn) -- m entry O(logn) insert to priority queue
- Space: O(m) -- for the new row created

##### Delete Row
- public boolean deleteRow(int rowKey)
- Time: O(1) -- add deleted index to deletedSet
- Space: O(1) -- add deleted index to deletedSet

##### Update Row
- public boolean updateRow(int rowKey, Pair<String, Object> updateValue);
- Time: O(2n)
    - n to convert pq to array
    - 1 to change value in array
    - n to convert array to pq
- Space: O(n) -- array to hold content of pq

##### Get all rows
- public List<List\<Object>> getAllRows();
- Time: O(n) -- iterate through each row and see if the row has been deleted
- Space: O(1) no extra space allocated

##### Get specific count of rows from a table
- public List<List\<Object>> getNRows(int limitCount);
- Time: O(n) -- iterate through each row and see if the row has been deleted until k reached
- Space: O(1) no extra space allocated

##### SORT by a column, and GET specific count of rows from table
- public List<List\<Object>> getNRowSorted(int limitCount, String colName);
- Time: O(n) -- iterate through the col th pq and see if the row has been deleted until k reached
- Space: O(1) no extra space allocated

##### GroupBy a column, and GET specific count of rows from table. This is the aggregate function.
- public List<Pair<Object, Integer>> countOfRowsGroupByKey(String colName);
- Time: O(n) -- iterate through the col in the table and see if the row has been deleted
- Space: O(n) n pair of <value, count(*)>

## InMemorDBTableTest
##### JUnit Test Coverage
- 95% of InMemoryDBTable 
- 100% of InmemoryDB



