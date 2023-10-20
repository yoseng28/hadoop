package test;

import java.io.IOException;

import tools.HBaseTools;

public class HBaseTest {
	
	
	public static void main(String[] args) throws IOException {
		HBaseTools.initConnection();
		//HBaseTools.checkTableExist("Contacts");
		HBaseTools.deleteTable("contacts");
		//createTable("stu", new String[] { "info", "grade" });
		//addColumnFamily("stu","scores");
		//listTables();
		//deleteColumnFamily("stu","scores");
		//deleteByRowKey("stu","r1");
		/*putValues("stu", new String[] { "rk003", "rk002" }, new String[] { "info",
		 * "grade" },new String[] { "age", "19" }, new String[] { "60", "80" });
		 */
		//getByRowKey("student","rk001");
		//filterByPrefix("stu","rk003");
		//filterByRandomRow("stu",(float)0.5);
		HBaseTools.closeConnection();
	}

}
