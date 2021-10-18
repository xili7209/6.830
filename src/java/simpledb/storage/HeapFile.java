package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     * the file that stores the on-disk backing store for this heap
     * file.
     */
    private File file;
    private TupleDesc tupleDesc;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pgNo = pid.getPageNumber();
        int pgSize = BufferPool.getPageSize();
        int tableId = pid.getTableId();
        int offset = pgNo * pgSize;
        try (RandomAccessFile f = new RandomAccessFile(file, "r")) {
            byte[] bytes = HeapPage.createEmptyPageData();
            f.seek(offset);
            int read = f.read(bytes, 0, pgSize);
            HeapPageId id = new HeapPageId(tableId, pgNo);
            return new HeapPage(id, bytes);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pgNo = page.getId().getPageNumber();
        int pgSize = BufferPool.getPageSize();
        int offset = pgNo * pgSize;
        try(RandomAccessFile f = new RandomAccessFile(file, "rw")){
            f.seek(offset);
            f.write(page.getPageData(),0,pgSize);
        }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        List<Page> res = new ArrayList<>();
        int i =0;
        for (; i < numPages(); i++) {
            PageId pageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                res.add(page);

                break;
            }
        }
        if (i==numPages()) {
            HeapPage page = new HeapPage(new HeapPageId(getId(), i), HeapPage.createEmptyPageData());
            page.insertTuple(t);
            res.add(page);
            writePage(page);
        }

        return res;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> res = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            PageId pageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            if (page.findTupleNumber(t) != -1) {
                page.deleteTuple(t);
                res.add(page);
                break;
            }

        }
        return res;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {

            private Iterator<Tuple> iterator;
            private int currPage;


            private Iterator<Tuple> getIterator(int pageNumber) throws TransactionAbortedException, DbException {
                if (pageNumber >= 0 && pageNumber < numPages()) {
                    HeapPageId pageId = new HeapPageId(getId(), pageNumber);
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                    return page.iterator();
                } else {
                    throw new DbException(String.format("problems opening/accessing the database pageNo %d ", pageNumber));
                }
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                currPage = 0;
                iterator = getIterator(currPage);
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (iterator == null) {
                    return false;
                } else if (iterator.hasNext()) {
                    return true;
                } else {
                    // get next iterator
                    currPage++;
                    if (currPage >= numPages()) {
                        return false;
                    } else {
                        iterator = getIterator(currPage);
                        return hasNext();
                    }
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (iterator == null || !iterator.hasNext()) {
                    throw new NoSuchElementException();
                }
                return iterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                iterator = null;
            }
        };
    }

}

