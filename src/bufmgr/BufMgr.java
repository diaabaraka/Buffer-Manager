package bufmgr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

import diskmgr.DB;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;

public class BufMgr {
	private String FIFO = "FIFO";
	private String Clock = "Clock";
	private String LRU = "LRU";
	private String MRU = "MRU";
	private String LOVEHATE = "LOVE/HATE";

	private byte[][] bufPool;// The actual buffer pool.........
	private Descriptor[] bufDescr;// To store the descriptor of each frame in
									// the pool....

	private Page[] pages;// to store references to pages of each frame
	private Hashtable<Integer, Integer> bufTable;// To map between the
													// framesNums and the
													// pagesIds....

	private Queue<Integer> fifoQueue;// Clock or FIFO Policy....
	private Queue<Integer> lruQueue;// LRU Policy Or Hated List
	private Stack<Integer> mruStack;// MRU Policy Or Loved List

	private String replaceArgument;

	/**
	 * Create the BufMgr object Allocate pages (frames) for the buffer pool in
	 * main memory and make the buffer manager aware that the replacement policy
	 * is specified by replaceArg (i.e. FIFO, LRU, MRU, love/hate)
	 * 
	 * @param numbufs
	 *            number of buffers in the buffer pool
	 * @param replaceArg
	 *            name of the buffer replacement policy
	 */
	public BufMgr(int numBufs, String replaceArg) {
		bufPool = new byte[numBufs][global.GlobalConst.MINIBASE_PAGESIZE];
		bufDescr = new Descriptor[numBufs];

		pages = new Page[numBufs];
		bufTable = new Hashtable<>();

		fifoQueue = new LinkedList<>();
		lruQueue = new LinkedList<>();
		mruStack = new Stack<>();

		replaceArgument = replaceArg;

		for (int i = 0; i < numBufs; i++) {
			fifoQueue.add(i);
			lruQueue.add(i);
			mruStack.push(i);

			bufDescr[i] = new Descriptor();
			pages[i] = new Page(bufPool[i]);

		}
	}

	/**
	 * Pin a page First check if this page is already in the buffer pool.
	 * 
	 * 
	 * If it is, increment the pin_count and return pointer to this page. If the
	 * pin_count was 0 before the call, the page was a replacement candidate,
	 * but is no longer a candidate.
	 * 
	 * 
	 * 
	 * If the page is not in the pool, choose a frame (from the set of
	 * replacement candidates) to hold this page, read the page (using the
	 * appropriate method from diskmgr package) and pin it.
	 * 
	 * Also, must write out the old page in chosen frame if it is dirty before
	 * reading new page. (You can assume that emptyPage == false for this
	 * assignment.)
	 * 
	 * @param pgid
	 *            page number in the minibase.
	 * @param page
	 *            the pointer point to the page.
	 * @param emptyPage
	 *            true (empty page), false (nonempty page).
	 * @throws BufferPoolExceededException
	 */
	public void pinPage(PageId pgid, Page page, boolean emptyPage, boolean loved)
			throws BufferPoolExceededException {
		int frameNum;

		if (bufTable.containsKey(pgid.pid)) {

			frameNum = bufTable.get(pgid.pid);// getting the frame number of
												// this page ID

			updatePolicy(frameNum);

			int pCount = bufDescr[frameNum].getPinCount();

			bufDescr[frameNum].setPinCount(pCount + 1);
			page.setpage(bufPool[frameNum]);
//			page = new Page(bufPool[frameNum]);

		} else {

			if (fifoQueue.isEmpty()) {

				throw new BufferPoolExceededException(null, "");

			} else {

				// get tuned with policy

				frameNum = replaceByPolicy(replaceArgument);

				updatePolicy(frameNum);

				if (bufDescr[frameNum].isDirtyBit()) {
					flushPage(bufDescr[frameNum].getPageNum());

				}Page temp = new Page();

				try {

					// read the page to return a reference to it.......
					
					SystemDefs.JavabaseDB.read_page(pgid, temp);
					


				} catch (InvalidPageNumberException | FileIOException
						| IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (bufDescr[frameNum].getPageNum() != null) {
					bufTable.remove(bufDescr[frameNum].getPageNum().pid);

				}

				// Editing the information of the descriptor
				bufDescr[frameNum].setPinCount(1);
				bufDescr[frameNum].setPageNum(new PageId(pgid.pid));
				bufDescr[frameNum].setDirtyBit(false);

				bufTable.put(pgid.pid, frameNum);

				bufPool[frameNum] = temp.getpage();
				page.setpage(bufPool[frameNum]);
//				page = new Page(bufPool[frameNum]);

			}

		}

	}

	public void updatePolicy(int frameNum) {
		if (replaceArgument.equals(FIFO) || replaceArgument.equals(Clock)) {
			if (fifoQueue.contains(new Integer(frameNum))) {
				fifoQueue.remove(new Integer(frameNum));
			}
		} else if (replaceArgument.equals(LRU)) {
			lruQueue.remove(new Integer(frameNum));
			lruQueue.add(frameNum);
		} else if (replaceArgument.equals(MRU)) {
			mruStack.remove(new Integer(frameNum));
			mruStack.push(frameNum);
		} else if (replaceArgument.equals(LOVEHATE)) {

			if (mruStack.contains(new Integer(frameNum))) {

				if (lruQueue.contains(new Integer(frameNum))) {
					lruQueue.remove(new Integer(frameNum));
					mruStack.remove(new Integer(frameNum));
					lruQueue.add(frameNum);
					mruStack.push(frameNum);
				} else {
					mruStack.remove(new Integer(frameNum));
					mruStack.push(frameNum);
				}

			}

		}

	}

	

	public int replaceByPolicy(String repArgu) {
		// To check that there a candidate cause the fifoQueue contains all
		// frames with pinCount equal ZERO

		if (!fifoQueue.isEmpty()) {

			if (repArgu.equals(FIFO) || repArgu.equals(Clock)) {
				return fifoQueue.poll();

			} else if (repArgu.equals(LRU)) {

				for (int fn : lruQueue) {

					if (bufDescr[fn].getPinCount() == 0) {
						fifoQueue.remove(fn);
						return fn;
					}
				}
				return -1;
			} else if (repArgu.equals(MRU)) {
				for (int fn : mruStack) {

					if (bufDescr[fn].getPinCount() == 0) {

						fifoQueue.remove(fn);
						return fn;
					}
				}
				return -1;
			} else if (repArgu.equals(LOVEHATE)) {

				for (int fn : lruQueue) {

					if (bufDescr[fn].getPinCount() == 0) {

						fifoQueue.remove(fn);
						return fn;
					}
				}
				for (int fn : mruStack) {

					if (bufDescr[fn].getPinCount() == 0) {

						fifoQueue.remove(fn);
						return fn;
					}
				}
				return -1;

			}
			return -1;

		}
		return -1;// The buffer pool is full

	}

	/**
	 * Unpin a page specified by a pageId. This method should be called with
	 * dirty == true if the client has modified the page. If so, this call
	 * should set the dirty bit for this frame. Further, if pin_count > 0, this
	 * method should decrement it. If pin_count = 0 before this call, throw an
	 * excpetion to report error. (for testing purposes, we ask you to throw an
	 * exception named PageUnpinnedExcpetion in case of error.)
	 * 
	 * @param pgid
	 *            page number in the minibase
	 * @param dirty
	 *            the dirty bit of the frame.
	 * @throws HashEntryNotFoundException
	 * @throws PageUnpinnedExcpetion
	 */
	public void unpinPage(PageId pgid, boolean dirty, boolean loved)
			throws HashEntryNotFoundException, PageUnpinnedExcpetion {

		// remain the policy of loved and hated
		if (bufTable.containsKey(pgid.pid)) {
			int frameNum = bufTable.get(pgid.pid);
			if (replaceArgument.equals(LOVEHATE)) {
				if (loved) {
					if (lruQueue.contains(new Integer(frameNum))) {
						lruQueue.remove(new Integer(frameNum));
					}
				}
			}

			int pinCount = bufDescr[frameNum].getPinCount();

			if (pinCount == 0) {

				throw new PageUnpinnedExcpetion(null, "");

			} else {

				bufDescr[frameNum].setPinCount(pinCount - 1);
				if (dirty) {
					flushPage(pgid);
				}
				if (pinCount == 1) {
					fifoQueue.remove(frameNum);
					fifoQueue.add(frameNum);
				}
			}

		} else {
			throw new HashEntryNotFoundException(null, "");
		}
	}

	/**
	 * Allocate new page(s). Call DB Object to allocate a run of new pages and
	 * find a frame in the buffer pool for the first page and pin it. (This call
	 * allows a client of the Buffer Manager to allocate pages on disk.) If
	 * buffer is full, i.e., you can\t find a frame for the first page, ask DB
	 * to deallocate all these pages, and return null.
	 * 
	 * @param firstPage
	 *            the address of the first page.
	 * @param howmany
	 *            total number of allocated new pages.
	 * 
	 * @return the first page id of the new pages. null, if error.
	 * @throws BufferPoolExceededException
	 */
	public PageId newPage(Page firstPage, int howmany)
			throws BufferPoolExceededException {
		if (fifoQueue.isEmpty()) {
			return null;
		}
		// page id Empty..........

		PageId pageId = new PageId();

		try {
			SystemDefs.JavabaseDB.allocate_page(pageId, howmany);

		} catch (OutOfSpaceException | InvalidRunSizeException
				| InvalidPageNumberException | FileIOException
				| DiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		pinPage(pageId, firstPage, false, false);
		return pageId;
	}

	/**
	 * This method should be called to delete a page that is on disk. This
	 * routine must call the method in diskmgr package to deallocate the page.
	 * 
	 * @param pgid
	 *            the page number in the database.
	 * @throws PageUnpinnedExcpetion
	 * @throws HashEntryNotFoundException
	 * @throws PagePinnedException
	 */
	public void freePage(PageId pgid) throws HashEntryNotFoundException,
			PagePinnedException, PageUnpinnedExcpetion {

		if (pgid == null) {
			return;
		}
		if (bufTable.containsKey(pgid.pid)) {
			int frameNum = bufTable.get(pgid.pid);

			if (bufDescr[frameNum].getPinCount() > 1) {
				

				throw new PagePinnedException(null, "");
			} else if (bufDescr[frameNum].getPinCount() == 1) {
				unpinPage(pgid, false, false);
			}
		}
		try {
			SystemDefs.JavabaseDB.deallocate_page(pgid);
		} catch (InvalidRunSizeException | InvalidPageNumberException
				| FileIOException | DiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Used to flush a particular page of the buffer pool to disk. This method
	 * calls the write_page method of the diskmgr package.
	 * 
	 * @param pgid
	 *            the page number in the database.
	 */
	public void flushPage(PageId pgid) {
		if (pgid == null || !bufTable.containsKey(pgid.pid)) {
			return;
		}
		int frameNum = bufTable.get(pgid.pid);

		Page page = new Page(bufPool[frameNum]);
		page.setpage(bufPool[frameNum]);
		try {

			SystemDefs.JavabaseDB.write_page(pgid, page);
		} catch (InvalidPageNumberException | FileIOException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}public void flushAllPages() {
		for (int i = 0; i < bufDescr.length; i++) {
			flushPage(bufDescr[i].getPageNum());
		}

	}

	public int getNumUnpinnedBuffers() {
		return fifoQueue.size();

	}

}