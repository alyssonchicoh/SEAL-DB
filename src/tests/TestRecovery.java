package tests;

import DBMS.fileManager.dataAcessManager.file.log.FileLog;
import DBMS.fileManager.dataAcessManager.file.log.FileRecord;
import DBMS.fileManager.dataAcessManager.file.log.FileLog.Pointer;

public class TestRecovery {
	
	public static void main(String[] args) {
	
		FileLog log = new FileLog("log02.b");
		
		
		log.append(createBlock(1, FileRecord.COMMIT_RECORD_TYPE));
		log.append(createBlock(2, FileRecord.ABORT_RECORD_TYPE));
		log.append(createBlock(3, FileRecord.UPDATE_LOG_RECORD_TYPE));
		log.append(createBlock(4, FileRecord.CHECKPOINT_RECORD_TYPE));
		log.append(createBlock(5, FileRecord.UPDATE_LOG_RECORD_TYPE));
		log.append(createBlock(6, FileRecord.ABORT_RECORD_TYPE));
		log.append(createBlock(7, FileRecord.UPDATE_LOG_RECORD_TYPE));
		log.append(createBlock(8, FileRecord.UPDATE_LOG_RECORD_TYPE));
		
	
		Pointer pointer = log.getPointer();
		FileRecord f = log.readPrev(pointer);
		while(f!=null){
			showBlock(f);
			f = log.readPrev(pointer);
		}
		
		System.out.println("End");
	}
	
	
	public static FileRecord createBlock(int id, int type){
		FileRecord r1 = new FileRecord(type);
		r1.setLSN(id);
		r1.setTransactionId(4+id);
		r1.setRecordType(type);

		if(r1.getRecordType()==FileRecord.UPDATE_LOG_RECORD_TYPE){
			r1.setAfterImage(("AFTER IMAGEM "+id).getBytes());
			r1.setBeforeImage(("BEFORE IMAGEM "+id).getBytes());		
			r1.setSchemaID(1+id);
			r1.setTableID(2+id);
			r1.setBlockID(3+id);
			
		}
		
		return r1;
	}
	
	public static void showBlock(FileRecord r){
		System.out.println("-----------------------------------");
		System.out.println("LSN " + r.getLSN());
		System.out.println("Transaction " + r.getTransactionId());
		System.out.println("Type " + r.getRecordType());
		
		if(r.getRecordType()==FileRecord.UPDATE_LOG_RECORD_TYPE){

			System.out.println("Schema " + r.getSchemaID());
			System.out.println("Table " + r.getTableID());
			System.out.println("Block " + r.getBlockID());
			System.out.println("AfterImage " + new String(r.getAfterImage()));
			System.out.println("BeforeImage " + new String(r.getBeforeImage()));		
		}
	}

	
	
}	

