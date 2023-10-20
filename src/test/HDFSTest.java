package test;

import java.io.IOException;
import java.net.URISyntaxException;

import tools.HDFSTools;

public class HDFSTest {
	
	public static void main(String[] args) throws URISyntaxException, IOException {
		//HDFSTools.createDir("/test");
		//HDFSTools.getFileInfo("/yoseng/test.txt");
		//HDFSTools.uploadFileByStream("D:\\news1.txt","/datasets/news1.txt");
		//HDFSTools.uploadFile("D:\\workspace\\eclipse_workspace\\hadoop\\src\\datasets\\sort1.txt","/datasets");
		HDFSTools.deleteFileAndDir("/test");
		//HDFSTools.gzipCompressFile("D:\\test.txt","/test.gz");
		//HDFSTools.uploadFile("D:\\Contacts.txt","/datasets");
		
	}

}
