package simpledb;
import java.util.*;

public class HeapFileIterator implements DbFileIterator {
	
	private HeapFile hf;
	private TransactionId tranId;
	private int tableId;
	
	
	private boolean isOpened;
	private int curPageNo;
	private Iterator<Tuple> curPageTupleIter;
	
	public HeapFileIterator(HeapFile hf, TransactionId tranId, int tableId){
		this.hf=hf;
		this.tranId=tranId;
		this.tableId=tableId;
		
		isOpened=false;
		curPageNo=0;
		curPageTupleIter=null;
	}
	
	public void open() throws DbException, TransactionAbortedException{
		isOpened=true;
		curPageNo=0;
		curPageTupleIter=null;
		if(curPageNo<hf.numPages()){
			HeapPage curHp=(HeapPage)Database.getBufferPool().getPage(tranId, new HeapPageId(tableId,curPageNo), Permissions.READ_ONLY);
			if(curHp!=null)
				curPageTupleIter=curHp.iterator();
		}
	}
	
	public boolean hasNext() throws DbException, TransactionAbortedException{
		if(curPageTupleIter == null)
			return false;
		if(curPageTupleIter.hasNext())
			return true;
		else{
			curPageNo=curPageNo+1;
			while(curPageNo<hf.numPages()){
				HeapPage curHp=(HeapPage)Database.getBufferPool().getPage(tranId, new HeapPageId(tableId,curPageNo), Permissions.READ_ONLY);
				curPageTupleIter=curHp.iterator();
				if(curPageTupleIter.hasNext())
					break;
				curPageNo++;	
			}
			if(curPageTupleIter.hasNext())
				return true;
			else
				return false;
		}
	}
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException{
		 if(!isOpened)
			 throw new NoSuchElementException("This page's iteractor isn't opened!");
		 Tuple nextOne= hasNext()? curPageTupleIter.next():null;
		 return nextOne;
	}
	public void rewind() throws DbException, TransactionAbortedException{
		if(!isOpened)
			throw new DbException("Iterator has not been opened.");
		close();
		open();
			
	}
	
	
	public void close(){
		isOpened =false;
		curPageNo=0;
		curPageTupleIter=null;
	}
	

}
