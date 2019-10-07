import java.util.HashMap;
import java.util.Map;

public class InMemoryDB {
    Map<String, InMemoryDBTable> DB;

    public InMemoryDB() {
        DB = new HashMap<>();
    }

    /**
     * Add Table to the dataBase, replace the existing one if the table with same name already exist
     * @param table to be inserted
     * @return Old table with the same name if there was one
     */
    public InMemoryDBTable insertTable(InMemoryDBTable table){
        return DB.put(table.tableName, table);

    }
}
