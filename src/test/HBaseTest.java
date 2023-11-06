package test;

import java.io.IOException;

import tools.HBaseTools;

public class HBaseTest {
	
	
	public static void main(String[] args) throws IOException {
		HBaseTools ht = new HBaseTools();
		ht.initConnection();
		//ht.checkTableExist("Contacts");
		//ht.deleteTable("contacts");
		//ht.createTable("stu", new String[] { "info", "grade" });
		//ht.addColumnFamily("stu","scores");
		//ht.listTables();
		//ht.deleteColumnFamily("stu","scores");
		//ht.deleteByRowKey("stu","r1");
		/*ht.putValues("stu", new String[] { "rk003", "rk002" }, new String[] { "info",
		 * "grade" },new String[] { "age", "19" }, new String[] { "60", "80" });
		 */
		//ht.getByRowKey("student","rk001");
		//ht.filterByPrefix("stu","rk003");
		//ht.filterByRandomRow("stu",(float)0.5);
		ht.filterByColumnPagination(null, 0, 0);
		ht.closeConnection();
	}

}
