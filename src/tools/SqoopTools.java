package tools;

/**
 * Apache Sqoop moved into the Attic in 2021-06
 * 
 * yoseng
 * 2021-12-01
 * beta
 * 
 */

import org.apache.hadoop.conf.Configuration;
import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.tool.SqoopTool;

public class SqoopTools {

	public static void main(String[] args) throws Exception {
		// sqoopByTool();
		//importHiveByArguments();
	}

	public static Configuration getConf() {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.184.3:8020");
		System.setProperty("HADOOP_USER_NAME", "root");
		return conf;
	}

	// 获取MySQL所有数据库
	public static void listDatabase() {
		String[] arguments = new String[] { "--connect", "jdbc:mysql://slave1:3306", "--username", "yoseng",
				"--password", "密码", };
		Sqoop sqoop = new Sqoop(SqoopTool.getTool("list-databases"), SqoopTool.loadPlugins(getConf()));
		Sqoop.runSqoop(sqoop, arguments);
	}

	// MySQL -> HDFS
	public static void importHDFS() {
		String[] arguments = new String[] { "--connect", "jdbc:mysql://slave1:3306/hive?useSSL=false", "--username",
				"yoseng", "--password", "密码", "--table", "TBLS", "--target-dir", "/202012",
				"--delete-target-dir", };
		Sqoop sqoop = new Sqoop(SqoopTool.getTool("import"), SqoopTool.loadPlugins(getConf()));
		Sqoop.runSqoop(sqoop, arguments);
	}

	// MySQL -> Hive
	public static void importHive() {
		String[] arguments = new String[] { "--connect", "jdbc:mysql://slave1:3306/hive?useSSL=false", "--username",
				"yoseng", "--password", "密码", "--table", "COLUMNS_V2", "--hive-table", "COLUMNS_V4",
				"--fields-terminated-by", ",", "--hive-import", "--delete-target-dir", };
		Sqoop sqoop = new Sqoop(SqoopTool.getTool("import"), SqoopTool.loadPlugins(getConf()));
		Sqoop.runSqoop(sqoop, arguments);
	}

}
