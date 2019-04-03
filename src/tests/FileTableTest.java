package tests;

import DBMS.Kernel;
import DBMS.fileManager.dataAcessManager.file.data.FileBlock;
import DBMS.fileManager.dataAcessManager.file.data.FileTable;
import DBMS.fileManager.dataAcessManager.file.data.FileTuple;

public class FileTableTest {
	
	public static void main(String[] args) {
		
		FileTable t = new FileTable("C:\\Users\\Gustavo\\git\\seal-db\\schemas\\company\\tables\\testao.b", Kernel.BLOCK_SIZE);
		
		
		FileBlock b1 =  t.read(4);
		
		System.out.println(b1);
		
		for (FileTuple f : b1.readTuplesArray()) {
			System.out.println(f);
		}
		
	}
}
