package tools;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
* HDFS相关工具类
* @author yoseng
* @version 202109
*/

public class HDFSTools {

	private static FileSystem fs = null;
	private static Configuration conf = null;
	private static URI uri = null;

	/* 关闭fs */
	public static void closeFileSystem() throws IOException {
		fs.close();
	}
	
	/* 获取配置信息 */
	public static void getFileSystem() throws URISyntaxException, IOException {
		conf = new Configuration();
		// 防止jar包被覆盖
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		uri = new URI("hdfs://192.168.184.3:8020");
		try {
			fs = FileSystem.get(uri, conf, "root");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/* 获取文件信息 */
	public static void getFileInfo(String filePath) throws URISyntaxException, IOException {
		getFileSystem();
		Path path = new Path(filePath);
		// 获取状态
		FileStatus fileStatus = fs.getFileLinkStatus(path);
		// 获取数据块大小
		long blockSize = fileStatus.getBlockSize();
		int blockSize_ = new Long(blockSize).intValue();
		blockSize_ = blockSize_ / (1024 * 1024);
		// 获取文件大小
		long fileSize = fileStatus.getLen();
		int fileSize_ = new Long(fileSize).intValue();
		fileSize_ = fileSize_ / (1024 * 1024);
		// 获取文件拥有者
		String fileOwner = fileStatus.getOwner();
		// 最近访问时间
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		long accessTime = fileStatus.getAccessTime();
		// 最后修改时间
		long modifyTime = fileStatus.getModificationTime();
		System.out.printf("blockSize:%dMB\n", blockSize_);
		System.out.println("fileSize:" + fileSize_ + "MB");
		System.out.println("fileOwner:" + fileOwner);
		System.out.println("accessTime:" + sdf.format(new Date(accessTime)));
		System.out.println("modifyTime:" + sdf.format(new Date(modifyTime)));
	}

	/* 创建目录 */
	public static void createDir(String dirName) throws URISyntaxException, IOException {
		getFileSystem();
		Path path = new Path(dirName);
		fs.mkdirs(path);
		System.out.println("目录创建成功！");
	}

	/* 写入文件 */
	public static void createFile(String filePath, String content) throws URISyntaxException, IOException {
		getFileSystem();
		Path path = new Path(filePath);
		FSDataOutputStream fso = fs.create(path, true);
		fso.writeBytes(content);
		fso.close();
		System.out.println("文件内容写入成功！");
	}

	/* 上传文件 */
	public static void uploadFile(String srcPath, String dstPath) throws URISyntaxException, IOException {
		getFileSystem();
		// 目录不存在，先创建目录
		if(!fs.exists(new Path(dstPath))){
			createDir(dstPath);
		}
		Path src = new Path(srcPath);
		Path dst = new Path(dstPath);
		FileStatus files[] = fs.listStatus(dst);
		for (FileStatus file : files) {
			System.out.println(file.getPath());
		}
		System.out.println("------------我是分隔符------------");
		fs.copyFromLocalFile(src, dst);
		for (FileStatus file : fs.listStatus(dst)) {
			System.out.println(file.getPath());
		}
		System.out.println("文件上传成功！");
	}

	/* 使用文件流上传文件 */
	public static void uploadFileByStream(String srcPath, String dstPath) throws IOException, URISyntaxException {
		getFileSystem();
		InputStream is = new FileInputStream(srcPath);
		OutputStream os = fs.create(new Path(dstPath));
		IOUtils.copyBytes(is, os, 1024);
		is.close();
		os.close();
		System.out.println("文件上传成功！");
	}

	/* 删除文件、目录 */
	public static void deleteFileAndDir(String filePath) throws URISyntaxException, IOException {
		getFileSystem();
		boolean result = fs.delete(new Path(filePath), true);
		if (result) {
			System.out.println("文件删除成功！");
		} else {
			System.out.println("文件删除失败！");
		}
	}

	/* 压缩文件 */
	public static void gzipCompressFile(String filePath, String compressPath)
			throws URISyntaxException, IOException, ClassNotFoundException {
		getFileSystem();
		// 设置压缩文件的格式
		String codecClassName = "org.apache.hadoop.io.compress.GzipCodec";
		// 通过java反射机制加载压缩格式类GzipCodec的类对象
		Class<?> codecClass = Class.forName(codecClassName);
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		OutputStream out = fs.create(new Path(compressPath), true);
		CompressionOutputStream cos = codec.createOutputStream(out);
		InputStream is = new BufferedInputStream(new FileInputStream(filePath));
		IOUtils.copyBytes(is, cos, 4096, true);
		is.close();
		cos.close();
		System.out.println("文件压缩成功！");
	}

}
