package hdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class HadoopTools {

	public static void main(String[] args) throws IOException {
		IntWritable writable = new IntWritable(163);
		byte[] bytes = serializeWritable(writable);
		System.out.println(bytes.length);

		IntWritable newIntWritable = new IntWritable();
		// 将序列化的字节数组bytes反序列化为IntWritable 对象
		deserializeWritable(newIntWritable, bytes);
		System.out.println(newIntWritable.get());
	}

	/* Hadoop序列化 */
	public static byte[] serializeWritable(Writable writable) throws IOException {
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		// 将字节数组输出流包装为数据输出流
		DataOutputStream dataOut = new DataOutputStream(byteOut);
		// 将writable对象序列化为数据字节输出流
		writable.write(dataOut);
		dataOut.close();
		return byteOut.toByteArray();
	}

	/* Hadoop反序列化 */
	public static void deserializeWritable(Writable writable, byte[] bytes) throws IOException {
		ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
		// 将字节数组输入流对象包装为数据输入流对象
		DataInputStream dataIn = new DataInputStream(byteIn);
		writable.readFields(dataIn);
		dataIn.close();
	}

}
