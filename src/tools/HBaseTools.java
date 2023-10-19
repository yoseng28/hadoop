/**
 * 本代码基于HBase 2.3.7
 * 2023-10-16
 * yoseng
 */

package tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;

public class HBaseTools {

	private static Configuration conf;
	private static Connection con;
	private static Admin admin;

	public static void main(String[] args) throws IOException {
		initConnection();
		//checkTableExist("student");
		//createTable("stu", new String[] { "info", "grade" });
		//addColumnFamily("stu","scores");
		//listTables();
		//deleteTable("stu");
		//deleteColumnFamily("stu","scores");
		//deleteByRowKey("stu","r1");
		/*
		 * putValues("stu", new String[] { "rk003", "rk002" }, new String[] { "info",
		 * "grade" },new String[] { "age", "19" }, new String[] { "60", "80" });
		 */
		//getByRowKey("student","rk001");
		//filterByPrefix("stu","rk003");
		filterByRandomRow("stu",(float)0.5);
		closeConnection();
	}

	// 初始化
	public static void initConnection() {
		ResourceBundle resource = ResourceBundle.getBundle("conf");
		conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", resource.getString("hbase.rootdir"));
		conf.set("hbase.master", resource.getString("hbase.master"));
		conf.set("hbase.zookeeper.property.clientPort", resource.getString("hbase.zookeeper.property.clientPort"));
		conf.set("hbase.zookeeper.quorum", resource.getString("hbase.zookeeper.quorum"));
		try {
			con = ConnectionFactory.createConnection(conf);
			admin = con.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 关闭连接
	public static void closeConnection() throws IOException {
		if (admin != null) {
			admin.close();
		}
		if (con != null) {
			con.close();
		}
	}

	// 判断表是否存在
	public static boolean checkTableExist(String tableName) throws IOException {
		TableName table_name = TableName.valueOf(tableName);
		if (admin.tableExists(table_name)) {
			System.out.println(tableName + "表已存在！");
			return true;
		} else {
			System.out.println(tableName + "表不存在！");
			return false;
		}
	}

	// 新建表
	public static void createTable(String tableName, String[] colFamily) throws IOException {
		if (checkTableExist(tableName)) {
			return;
		}
		TableName table_name = TableName.valueOf(tableName);
		List<ColumnFamilyDescriptor> list = new ArrayList<>();
		TableDescriptorBuilder hdb = TableDescriptorBuilder.newBuilder(table_name);
		for (String colF : colFamily) {
			ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(colF.getBytes()).build();
			list.add(cfd);
		}
		TableDescriptor td = hdb.setColumnFamilies(list).build();
		admin.createTable(td);
		System.out.println("表'" + tableName + "'创建成功!");
	}

	// 新增列簇
	public static void addColumnFamily(String tableName, String colFamily) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName table_name = TableName.valueOf(tableName);
		ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(colFamily.getBytes()).build();
		admin.addColumnFamily(table_name, cfd);
		System.out.println("列簇'" + colFamily + "'新增成功!");
	}

	// 打印所有表名
	public static void listTables() throws IOException {
		for (TableName tb : admin.listTableNames()) {
			System.out.println(tb);
		}
	}

	// 删除表
	public static void deleteTable(String tableName) throws IOException {
		TableName table_name = TableName.valueOf(tableName);
		boolean isExist = checkTableExist(tableName);
		if (!isExist) {
			System.out.println("表'" + tableName + "'删除失败!");
		} else {
			admin.disableTable(table_name);
			admin.deleteTable(table_name);
			System.out.println("表'" + tableName + "'删除成功!");
		}
	}

	// 删除列簇-未判断列簇是否存在
	public static void deleteColumnFamily(String tableName, String colFamily) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName table_name = TableName.valueOf(tableName);
		admin.deleteColumnFamily(table_name, colFamily.getBytes());
		System.out.println("列簇'" + colFamily + "'删除成功!");
	}

	// 删除 by RowKey
	public static void deleteByRowKey(String tableName, String rowKey) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName table_name = TableName.valueOf(tableName);
		try {
			Table table = con.getTable(table_name);
			Delete delete = new Delete(rowKey.getBytes());
			// 添加其他删除条件
			// delete.addColumn(family, qualifier)
			// delete.set...
			table.delete(delete);
			System.out.println("数据删除成功!");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 新增一条数据
	public static void putValue(String tableName, String rowKey, String columnFamily, String qualifier, String value)
			throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName table_Name = TableName.valueOf(tableName);
		try {
			Table table = con.getTable(table_Name);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
			System.out.println("数据新增成功！");
		} catch (IOException e) {
			System.out.println("数据新增失败！");
			e.printStackTrace();
		}
	}

	// 新增多条数据
	public static void putValues(String tableName, String[] rowKeys, String[] columnFamilies, String[] qualifiers,
			String[] values) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName table_name = TableName.valueOf(tableName);
		Table table;
		try {
			table = con.getTable(table_name);
			List<Put> putList = new ArrayList<Put>();
			for (int i = 0; i < rowKeys.length; i++) {
				Put put = new Put(Bytes.toBytes(rowKeys[i]));
				put.addColumn(Bytes.toBytes(columnFamilies[i]), Bytes.toBytes(qualifiers[i]), Bytes.toBytes(values[i]));
				putList.add(put);
			}
			table.put(putList);
			table.close();
			System.out.println("批量新增数据成功！");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 新增多条数据List
	public static void putListValues(String tableName, List<Put> putsList) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName table_name = TableName.valueOf(tableName);
		try {
			Table table = con.getTable(table_name);
			table.put(putsList);
			System.out.println("数据新增成功！");
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("数据新增失败！");
		}
	}

	// 按照RowKey查询
	public static void getByRowKey(String tableName, String rowKey) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName table_name = TableName.valueOf(tableName);
		try {
			Table table = con.getTable(table_name);
			Get get = new Get(rowKey.getBytes());
			// 添加列簇、限定符
			// get.addColumn(family, qualifier);
			// 通过get.set...方法添加其他查询条件
			Result result = table.get(get);
			Cell[] cells = result.rawCells();
			printRecoder(cells);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 打印记录
	public static void printRecoder(Cell[] cells) {
		for (Cell cell : cells) {
			System.out.println("***********************************");
			System.out.println("行键：" + new String(CellUtil.cloneRow(cell)));
			System.out.println("列簇：" + new String(CellUtil.cloneFamily(cell)));
			System.out.println("列：" + new String(CellUtil.cloneQualifier(cell)));
			System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
			System.out.println("时间戳:" + cell.getTimestamp());
			System.out.println("***********************************");
		}
	}

	// shell: desc table
	public static void descTable(String tableName) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName table_name = TableName.valueOf(tableName);
		ColumnFamilyDescriptor[] colFamilies = admin.getDescriptor(table_name).getColumnFamilies();
		for (ColumnFamilyDescriptor cf : colFamilies) {
			System.out.println("Name:" + cf.getNameAsString());
			System.out.println("MaxVersions:" + cf.getMaxVersions());
			System.out.println("BloomFilter:" + cf.getBloomFilterType());
			System.out.println("Blocksize:" + cf.getBlocksize());
			System.out.println("Compression:" + cf.getCompressionType());
			System.out.println("ReplicationScope" + cf.getDFSReplication());
			System.out.println("TTL:" + cf.getTimeToLive());
		}
	}

	// 扫描表中所有数据
	public static void scanByTableName(String tableName) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName table_name = TableName.valueOf(tableName);
		try {
			Table table = con.getTable(table_name);
			Scan scan = new Scan();
			// 添加扫描条件
			// scan.addColumn(family, qualifier)
			// scan.addFamily(family)
			// scan.setStartRow(startRow)
			// scan.setStopRow(stopRow)
			// scan.setMaxVersions(maxVersions)
			ResultScanner rs = table.getScanner(scan);
			for (Result result : rs) {
				Cell[] cells = result.rawCells();
				printRecoder(cells);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// CompareOperator-RowFilter
	public static void filterByRowKey(String tableName, String rowKey) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName tb = TableName.valueOf(tableName);
		try {
			Table table = con.getTable(tb);
			Scan scan = new Scan();
			BinaryComparator bc = new BinaryComparator(rowKey.getBytes());
			Filter filter = new RowFilter(CompareOperator.EQUAL, bc);
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result result : rs) {
				Cell[] cells = result.rawCells();
				printRecoder(cells);
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// CompareOperator-QualifierFilter
	public static void filterByQualifier(String tableName, String qualifier) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName tb = TableName.valueOf(tableName);
		try {
			Table table = con.getTable(tb);
			Scan scan = new Scan();
			BinaryComparator bc = new BinaryComparator(qualifier.getBytes());
			Filter filter = new QualifierFilter(CompareOperator.EQUAL, bc);
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for (Result result : rs) {
				Cell[] cells = result.rawCells();
				printRecoder(cells);
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//按照RowKey的前缀过滤
	public static void filterByPrefix(String tableName, String keyWords) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName tb = TableName.valueOf(tableName);
		try {
			Table table = con.getTable(tb);
			Scan scan = new Scan();
			Filter filter = new PrefixFilter(Bytes.toBytes(keyWords));
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for(Result result:rs) {
				Cell[] cells = result.rawCells();
				printRecoder(cells);
			}
			rs.close();
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/*
	 *  随机选取行
	 * 传入参数chance可能性的取值区间在 0.0 到 1.0 之间的float。
	 */
	public static void filterByRandomRow(String tableName, float num) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName tb = TableName.valueOf(tableName);
		try {
			Table table = con.getTable(tb);
			Scan scan = new Scan();
			Filter filter = new RandomRowFilter(num);
			scan.setFilter(filter);
			ResultScanner rs = table.getScanner(scan);
			for(Result result:rs) {
				Cell[] cells = result.rawCells();
				printRecoder(cells);
			}
			table.close();
		}catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 *  每一行取回多少列
	 *  当一行的列数达到设定的最大值时，停止scan操作
	 */
	public static void filterByColumnWithCount(String tableName, int num) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName tb = TableName.valueOf(tableName);
		try {
			Table table = con.getTable(tb);
			Scan scan = new Scan();
			Filter filter = new ColumnCountGetFilter(num);
			scan.setFilter(filter);
			ResultScanner rs= table.getScanner(scan);
			for(Result result:rs) {
				Cell[] cells = result.rawCells();
				printRecoder(cells);
			}
			rs.close();
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	// 遇到某一列的值不符合条件，跳过该行
	public static void filterBySkipWithValue(String tableName, String value) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName tb = TableName.valueOf(tableName);
		try {
			Table table = con.getTable(tb);
			Scan scan = new Scan();
			
			Filter filter_ = new ValueFilter(CompareOperator.NOT_EQUAL, new SubstringComparator(value));
			Filter filter = new SkipFilter(filter_);
			scan.setFilter(filter);
			ResultScanner rs= table.getScanner(scan);
			for(Result result:rs) {
				Cell[] cells = result.rawCells();
				printRecoder(cells);
			}
			rs.close();
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/*
	 * 多重过滤
	 * 参数：MUST_PASS_ALL、MUST_PASS_ONE
	 */
	public static void filterByList(String tableName, String rowKey, String value) throws IOException {
		if (!checkTableExist(tableName)) {
			return;
		}
		TableName tb = TableName.valueOf(tableName);
		try {
			Table table = con.getTable(tb);
			Scan scan = new Scan();
			Filter filter1 = new RowFilter(CompareOperator.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(rowKey)));
			Filter filter2= new ValueFilter(CompareOperator.EQUAL, new SubstringComparator(value));
			List<Filter> list = new ArrayList<>();
			list.add(filter1);
			list.add(filter2);
			FilterList filter_list = new FilterList(FilterList.Operator.MUST_PASS_ALL,list);
			scan.setFilter(filter_list);
			ResultScanner rs= table.getScanner(scan);
			for(Result result:rs) {
				Cell[] cells = result.rawCells();
				printRecoder(cells);
			}
			rs.close();
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
