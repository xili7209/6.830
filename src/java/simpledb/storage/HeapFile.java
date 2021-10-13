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
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
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
        return  tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pgNo = pid.getPageNumber();
        int pgSize = BufferPool.getPageSize();
        int tableId = pid.getTableId();
        int offset = pgNo*pgSize;
        try(RandomAccessFile f = new RandomAccessFile(file,"r")){
            byte[] bytes = HeapPage.createEmptyPageData();
            f.seek(offset);
            int read = f.read(bytes,0,pgSize);
            HeapPageId id = new HeapPageId(tableId,pgNo);
            return new HeapPage(id,bytes);
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
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            BufferPool bufferPool = Database.getBufferPool();
            Iterator<Tuple> child = null;
            int pid =-1;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                  pid =0;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(child!=null&&child.hasNext()) return true;
                else if(pid<0||pid>=numPages()) return false;
                else {
                    child = ((HeapPage)bufferPool.getPage(tid,new HeapPageId(getId(),pid++),Permissions.READ_ONLY)).iterator();
                    return child.hasNext();
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(hasNext()){
                    return child.next();
                }
                throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pid = 0;
                child = null;
            }

            @Override
            public void close() {
                pid = -1;
                child = null;
            }
        };
    }

}

