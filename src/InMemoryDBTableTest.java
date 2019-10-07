import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryDBTableTest {
    private InMemoryDB db;
    InMemoryDBTable table;
    @org.junit.jupiter.api.Test
    void constructDB() {
        db= new InMemoryDB();
        List<Pair<String, String>> colNameType = new ArrayList<>();
        Pair<String, String> p1 = new Pair<>("A", "String");
        Pair<String, String> p2 = new Pair<>("B", "Integer");
        colNameType.add(p1);
        colNameType.add(p2);
        table = new InMemoryDBTable("tableA", colNameType);
        db.insertTable(table);
    }

    @org.junit.jupiter.api.Test
    void insertRow() {
        constructDB();
        // test simple insert
        List<Pair<String, Object>> row = new ArrayList<>();
        row.add(new Pair<>("A", "Zero"));
        row.add(new Pair<>("B", 0));
        assertEquals(0, table.insertRow(row));
        row = new ArrayList<>();
        row.add(new Pair<>("A", "One"));
        row.add(new Pair<>("B", 1));
        assertEquals(1, table.insertRow(row));
        // test invalid type insert
        row = new ArrayList<>();
        row.add(new Pair<>("A", 2));
        row.add(new Pair<>("B", "Two"));
        assertEquals(-1, table.insertRow(row));

        row = new ArrayList<>();
        row.add(new Pair<>("C", 2));
        row.add(new Pair<>("D", "Two"));
        assertEquals(-1, table.insertRow(row));

        List<List<Object>> rows = table.getAllRows();
        // examine content
        assertEquals(2, rows.size());
        assertEquals("Zero", rows.get(0).get(0));
        assertEquals(1, rows.get(1).get(1));
    }

    @org.junit.jupiter.api.Test
    void deleteRow() {
        // initialize
        constructDB();
        List<Pair<String, Object>> row = new ArrayList<>();
        row.add(new Pair<>("A", "Zero"));
        row.add(new Pair<>("B", 0));
        assertEquals(0, table.insertRow(row));
        row = new ArrayList<>();
        row.add(new Pair<>("A", "One"));
        row.add(new Pair<>("B", 1));
        assertEquals(1, table.insertRow(row));
        row.add(new Pair<>("A", "Two"));
        row.add(new Pair<>("B", 2));
        assertEquals(2, table.insertRow(row));
        row = new ArrayList<>();
        row.add(new Pair<>("A", "Three"));
        row.add(new Pair<>("B", 3));
        assertEquals(3, table.insertRow(row));
        // test delete
        assertEquals(false, table.deleteRow(4));
        assertEquals(true, table.deleteRow(2));
        assertEquals(false, table.deleteRow(2));
        List<List<Object>> rows = table.getAllRows();
        // examine content
        assertEquals(3, rows.size());
        assertEquals("Three", rows.get(2).get(0));
        assertEquals(3, rows.get(2).get(1));
    }

    @org.junit.jupiter.api.Test
    void updateRow() {
        // initialize
        constructDB();
        List<Pair<String, Object>> row = new ArrayList<>();
        row.add(new Pair<>("A", "Zero"));
        row.add(new Pair<>("B", 0));
        assertEquals(0, table.insertRow(row));
        row = new ArrayList<>();
        row.add(new Pair<>("A", "One"));
        row.add(new Pair<>("B", 1));
        assertEquals(1, table.insertRow(row));
        // update
        assertEquals(true, table.updateRow(0, new Pair<>("A", "ZeroChanged")));
        assertEquals(true, table.updateRow(1, new Pair<>("B", 1000)));
        assertEquals(false, table.updateRow(2, new Pair<>("A", "ZeroChanged")));
        assertEquals(false, table.updateRow(1, new Pair<>("A", 1)));

        // examine content
        List<List<Object>> rows = table.getAllRows();
        assertEquals(2, rows.size());
        assertEquals("ZeroChanged", rows.get(0).get(0));
        assertEquals(1000, rows.get(1).get(1));

        // update deleted role
        assertEquals(true, table.deleteRow(1));
        assertEquals(false, table.updateRow(1, new Pair<>("B", 1000)));
    }

    @org.junit.jupiter.api.Test
    void getNRows() {
        // initialize
        constructDB();
        List<Pair<String, Object>> row = new ArrayList<>();
        row.add(new Pair<>("A", "Zero"));
        row.add(new Pair<>("B", 0));
        assertEquals(0, table.insertRow(row));
        row = new ArrayList<>();
        row.add(new Pair<>("A", "One"));
        row.add(new Pair<>("B", 1));
        assertEquals(1, table.insertRow(row));
        row.add(new Pair<>("A", "Two"));
        row.add(new Pair<>("B", 2));
        assertEquals(2, table.insertRow(row));
        row = new ArrayList<>();
        row.add(new Pair<>("A", "Three"));
        row.add(new Pair<>("B", 3));
        assertEquals(3, table.insertRow(row));
        // test delete
        assertEquals(true, table.deleteRow(2));
        List<List<Object>> rows = table.getNRows(3);
        assertEquals(3, rows.size());
        assertEquals("Three", rows.get(2).get(0));
        assertEquals(3, rows.get(2).get(1));
        // test limit < n
        rows = table.getNRows(2);
        assertEquals(2, rows.size());
        assertEquals("One", rows.get(1).get(0));
        assertEquals(1, rows.get(1).get(1));
    }

    @org.junit.jupiter.api.Test
    void getNRow() {
        constructDB();
        List<Pair<String, Object>> row = new ArrayList<>();
        row.add(new Pair<>("A", "A"));
        row.add(new Pair<>("B", 0));
        assertEquals(null, table.getNRows(-1));
    }

    @org.junit.jupiter.api.Test
    void getNRowSorted() {
        // initialize
        constructDB();
        List<Pair<String, Object>> row = new ArrayList<>();
        row.add(new Pair<>("A", "A"));
        row.add(new Pair<>("B", 0));
        assertEquals(0, table.insertRow(row));
        row = new ArrayList<>();
        row.add(new Pair<>("A", "D"));
        row.add(new Pair<>("B", 3));
        assertEquals(1, table.insertRow(row));
        row.add(new Pair<>("A", "C"));
        row.add(new Pair<>("B", 2));
        assertEquals(2, table.insertRow(row));
        row = new ArrayList<>();
        row.add(new Pair<>("A", "B"));
        row.add(new Pair<>("B", 1));
        assertEquals(3, table.insertRow(row));
        assertEquals(null, table.getNRowSorted(-1, "A"));
        List<List<Object>> rows = table.getNRowSorted(3, "A");
        assertEquals(3, rows.size());
        assertEquals("B", rows.get(1).get(0));
        assertEquals("C", rows.get(2).get(0));

        rows = table.getNRowSorted(4, "B");
        assertEquals(4, rows.size());
        assertEquals(1, rows.get(1).get(1));
        assertEquals(2, rows.get(2).get(1));
    }

    @org.junit.jupiter.api.Test
    void countOfRowsGroupByKey() {
        // initialize
        constructDB();
        List<Pair<String, Object>> row = new ArrayList<>();
        row.add(new Pair<>("A", "Zero"));
        row.add(new Pair<>("B", 0));
        assertEquals(0, table.insertRow(row));
        row = new ArrayList<>();
        row.add(new Pair<>("A", "One"));
        row.add(new Pair<>("B", 1));
        assertEquals(1, table.insertRow(row));
        row.add(new Pair<>("A", "Two"));
        row.add(new Pair<>("B", 2));
        assertEquals(2, table.insertRow(row));
        row = new ArrayList<>();
        row.add(new Pair<>("A", "Three"));
        row.add(new Pair<>("B", 3));
        assertEquals(3, table.insertRow(row));
        List<Pair<Object, Integer>> rows = table.countOfRowsGroupByKey("A");
        assertEquals(4, rows.size());
        assertEquals("One", rows.get(1).getKey());
        assertEquals(4, rows.get(1).getValue());

        // delete
        table.deleteRow(2);
        rows = table.countOfRowsGroupByKey("A");
        assertEquals(3, rows.size());
        assertEquals("Three", rows.get(2).getKey());
        assertEquals(3, rows.get(2).getValue());
    }
}