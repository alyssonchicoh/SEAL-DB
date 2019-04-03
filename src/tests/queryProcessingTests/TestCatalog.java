package tests.queryProcessingTests;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import DBMS.Kernel;
import DBMS.fileManager.dataAcessManager.file.ExceededSizeBlockException;
import DBMS.fileManager.dataAcessManager.file.data.FileBlock;
import DBMS.fileManager.dataAcessManager.file.data.FileTable;
import DBMS.fileManager.dataAcessManager.file.data.FileTuple;
import DBMS.queryProcessing.ITable;
import DBMS.queryProcessing.TableManipulate;

public class TestCatalog {

	public static void main(String[] args) throws ExceededSizeBlockException, RemoteException {
		
		
		

		
		//Kernel.BUFFER_SIZE = 1;
		Kernel.start();
		
	//	@SuppressWarnings("unused")
	//	FileTable t = Kernel.getCatalog().getShemas().get(0).getCatalog();
		
		
		//create(t);
		
		
		
		List<ITable> tableManipulate = Kernel.getCatalog().getShemas().get(0).getTables();
		
		for (ITable iTable : tableManipulate) {
			System.out.println(Arrays.toString(iTable.getColumnNames()));
			System.out.println(iTable.getPath());
			
			for (int i = 0; i < 300; i++) {
				
			//	iTable.writeTuple(new Transaction(new DBConnection("company")), i+"|Roberto Mariano Silva|5000");
				
					
			}
			
			
			
			break;
		}
	
		//Kernel.getBufferManager().flush();
		
		
		
		
	///create(t);
		
		
		
	//	tableID | name | path | columnStructure | numberOfBlocks | numberOfTuples | lastBlockWrited |
		
		
		
		
		
		
	}
	@SuppressWarnings("unused")
	private static void create(final FileTable t) throws ExceededSizeBlockException {
		//	tableID | name | path | columnStructure | numberOfBlocks | numberOfTuples | lastBlockWrited |
		FileTuple t1 = FileTuple.build(0,"1|empregado|schemas\\company\\tables\\empregado.b|id"+TableManipulate.SUB_SEPARATOR+"nome"+TableManipulate.SUB_SEPARATOR+"salario|0|0|0");
		FileTuple t2 = FileTuple.build(1,"2|departamento|schemas\\company\\tables\\departamento.b|id"+TableManipulate.SUB_SEPARATOR+"nome|0|0|0");
	
		
		
		FileBlock b = new FileBlock(Kernel.BLOCK_SIZE);
	
		b.writeTuple(t1);
		
		b.writeTuple(t2);

		
		b.setId(0);
		
		t.write(b);
		show(t.read(0));
	}
	private static void show(FileBlock block) {
		ArrayList<FileTuple> a = block.readTuplesArray();
		for (FileTuple tuple : a) {
			System.out.println(tuple);
		}
	}

}
