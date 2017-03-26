package qp.operators;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

/** 
 * Block nested loop join algorithm.
 */
public class SortMergeJoin extends Join {

    public final int LEFT_CHILD = 0;
    public final int RIGHT_CHILD = 1;
    
    int batchSize;  // Number of tuples per batch

    /**
     * The following fields are useful during execution of
     * SortMergeJoin algorithm.
     */
    int leftIndex;  // Index of the join attribute in the left table
    int rightIndex;

    static int filenum = 0;  // To get unique filenum for this operation

    Batch outBatch;  // Output buffer
    Batch leftBatch;  // Buffer for the left input stream
    Batch rightBatch;  // Buffer for the right input stream

    int lcurs;  // Cursor for left side buffer
    int rcurs;  // Cursor for right side buffer
    int tempRCurs;
    boolean eosl;  // Indicates whether end of left table stream is reached
    boolean eosr;  // End of right table stream is reached

    ExternalMergeSort sortedLeft;
    ExternalMergeSort sortedRight;

    ArrayList<String> createdFiles;  // Keeps track of created files


    /* Variables used for merging state */
    boolean inMergingState;
    String leftFileName;
    String rightFileName;
    ObjectInputStream leftFile;
    ObjectInputStream rightFile;
    Batch leftMergeBatch;
    Batch rightMergeBatch;
    int leftMergeTupleCursor;
    int rightMergeTupleCursor;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    /**
     * During open, finds the index of the join attributes,
     * materializes the right hand side into a file and opens
     * the connection.
     */
    public boolean open() {

        // Calculate number of tuples per batch
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        // Obtain index of join attribute in left and right table
        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftIndex = left.getSchema().indexOf(leftattr);
        rightIndex = right.getSchema().indexOf(rightattr);

        // Sort both left and right child
        sortedLeft = new ExternalMergeSort(left, leftIndex, OpType.SORT);
        sortedLeft.setNumBuff(getNumBuff());
        sortedLeft.setSchema(left.getSchema());

        sortedRight = new ExternalMergeSort(right, rightIndex, OpType.SORT);
        sortedRight.setNumBuff(getNumBuff());
        sortedRight.setSchema(right.getSchema());

        if (!sortedLeft.open() || !sortedRight.open()) {
            return false;
        }

        // Initialize the cursors of the input buffers
        inMergingState = false;
        lcurs = 0;  rcurs = 0;
        eosl = false;
        eosl = false;
        // Initialize left and right batch
        leftBatch = (Batch) sortedLeft.next();
        rightBatch = (Batch) sortedRight.next();

        // Array for temp files
        createdFiles = new ArrayList<String>();

        return true;
    }

    public Batch next() {
        // End of stream
        if (eosl || eosr) {
            close();
            return null;
        }

        // 1 batch buffer for output
        outBatch = new Batch(batchSize);

        while (!outBatch.isFull()) {

            try {
                // Merging state should be prioritized to process first
                if (inMergingState) {
                    Tuple tuple;
                    while ((tuple = getMergedTuple()) != null) {
                        outBatch.add(tuple);
                        if (outBatch.isFull()) {
                            return outBatch;
                        }
                    }
                    inMergingState = false;
                }

                if (leftBatch == null || rightBatch == null || eosl || eosr) {
                    eosl = true;
                    eosr = true;
                    return outBatch;
                }

                // Advance left cursor util r-tuple >= s-tuple
                while (compareLeftRight() < 0) {
                    incLeftBuffer();
                    if (leftBatch == null) {
                        eosl = true;
                        return outBatch;
                    }
                }

                // Advance right cursor util s-tuple >= r-tuple
                while (compareLeftRight() > 0) {
                    incRightBuffer();
                    if (rightBatch == null) {
                        eosl = true;
                        return outBatch;
                    }
                }

                if (compareLeftRight() == 0) {
                    inMergingState = true;

                    leftFileName = createPartitionFileOfChildWithValueEqualsTo(getLeftTuple(), LEFT_CHILD);
                    rightFileName = createPartitionFileOfChildWithValueEqualsTo(getRightTuple(), RIGHT_CHILD);

                    leftFile = new ObjectInputStream(new FileInputStream(leftFileName));
                    rightFile = new ObjectInputStream(new FileInputStream(rightFileName));

                    leftMergeBatch = (Batch) leftFile.readObject();
                    rightMergeBatch = (Batch) rightFile.readObject();

                    leftMergeTupleCursor = 0;
                    rightMergeTupleCursor = 0;
                    
                    Tuple tuple;
                    while ((tuple = getMergedTuple()) != null) {
                        outBatch.add(tuple);
                        if (outBatch.isFull()) {
                            return outBatch;
                        }
                    }
                    inMergingState = false;
                }

            } catch (ClassNotFoundException c) {
                System.out.println("SortMergeJoin:Some error in deserialization ");
                System.exit(1);

            } catch (IOException io) {
                System.out.println("SortMergeJoin:temporary file reading error");
                System.exit(1);
            }
        }  // outbatch while not full

        return outBatch;
    }

    /** Close the operator */
    public boolean close(){
        for (String fileName : createdFiles) {
            File file = new File(fileName);
            file.delete();
        }
        return sortedLeft.close() && sortedRight.close();
    }

    public void incRightBuffer() {
        if (rcurs == rightBatch.size() - 1) {
            rightBatch = (Batch) sortedRight.next();
            rcurs = 0;
        } else {
            rcurs++;
        }
    }

    public void incLeftBuffer() {
        if (lcurs == leftBatch.size() - 1) {
            leftBatch = (Batch) sortedLeft.next();
            lcurs = 0;
        } else {
            lcurs++;
        }
    }

    public Tuple getLeftTuple() {
        return (leftBatch == null ? null : leftBatch.elementAt(lcurs));
    }

    public Tuple getRightTuple() {
        return (rightBatch == null ? null : rightBatch.elementAt(rcurs));
    }

    public int compareLeftRight() {
        return Tuple.compareTuples(leftBatch.elementAt(lcurs), rightBatch.elementAt(rcurs), leftIndex, rightIndex);
    }

    /**
     * Returns 1 merged tuple during merging state.
     *
     * Guarantee: rightFile and leftFile are both not empty
     */
    public Tuple getMergedTuple() throws IOException, ClassNotFoundException {
        while (leftMergeBatch != null) {
            Tuple leftTuple = leftMergeBatch.elementAt(leftMergeTupleCursor);
            Tuple rightTuple = rightMergeBatch.elementAt(rightMergeTupleCursor);
            Tuple outTuple = leftTuple.joinWith(rightTuple);

            // Update cursor before return answer
            if (rightMergeTupleCursor == rightMergeBatch.size() - 1) {
                rightMergeTupleCursor = 0;
                // If we already read the last tuple of this batch,
                // need to load the next batch
                try {
                    rightMergeBatch = (Batch) rightFile.readObject();
                } catch (EOFException e) {
                    // Read the last batch of right file, we need to reload rightfile
                    rightFile.close();
                    rightFile = new ObjectInputStream(new FileInputStream(rightFileName));
                    rightMergeBatch = (Batch) rightFile.readObject();
                    
                    // Reach end of right file, increase cursor of left file by 1
                    // If we also reach the end of left file, return null
                    if (leftMergeTupleCursor == leftMergeBatch.size() - 1) {
                        leftMergeTupleCursor = 0;
                        try {
                            leftMergeBatch = (Batch) leftFile.readObject();
                        } catch (EOFException eof) {
                            leftMergeBatch = null;
                        }
                    } else {
                        leftMergeTupleCursor++;
                    }
                }
            } else {
                rightMergeTupleCursor++;
            }
            return outTuple;
        }
        return null;
    }

    public String createPartitionFileOfChildWithValueEqualsTo(Tuple model, int childType) throws IOException {

        filenum++;
        String fileName = "SMJtemp-" + filenum;
        ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(fileName));

        createdFiles.add(fileName);

        Tuple tuple = (childType == LEFT_CHILD ? getLeftTuple() : getRightTuple());
        Batch batch = new Batch(batchSize);
        // Write all tuple with attr value equals to model to a temp file
        while (tuple != null && Tuple.compareTuples(tuple, model, (childType == LEFT_CHILD ? leftIndex : rightIndex)) == 0) {
            batch.add(tuple);
            if (batch.isFull()) {
                out.writeObject(batch);
                batch = new Batch(batchSize);
            }
            if (childType == LEFT_CHILD) {
                incLeftBuffer();
                tuple = getLeftTuple();
            } else {
                incRightBuffer();
                tuple = getRightTuple();
            }
        }
        if (!batch.isEmpty()) {
            out.writeObject(batch);
        }
        out.close();

        return fileName;
    }

}
