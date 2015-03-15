package bufmgr;

import global.PageId;

public class Descriptor {
	private PageId pageNum;
	private int pinCount;
	private boolean dirtyBit;
	public Descriptor() {
		// TODO Auto-generated constructor stub
	}

	public Descriptor(PageId pNum, int pCount, boolean dirty) {
		pageNum = pNum;
		pinCount = pCount;
		dirty = dirty;
	}

	public PageId getPageNum() {
		return pageNum;
	}

	public int getPinCount() {
		return pinCount;
	}

	public boolean isDirtyBit() {
		return dirtyBit;
	}

	public void setPageNum(PageId pageNum) {
		this.pageNum = pageNum;
	}

	public void setPinCount(int pinCount) {
		this.pinCount = pinCount;
	}

	public void setDirtyBit(boolean dirtyBit) {
		this.dirtyBit = dirtyBit;
	}

}
