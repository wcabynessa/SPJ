package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

/** 
 * Block nested loop join algorithm.
 */
public class SortMergeJoin extends Join {
    
    int batchSize;  // Number of tuples per batch

    /**
     * The following fields are useful during execution of
     * SortMergeJoin algorithm.
     */
    int leftIndex;  // Index of the join attribute in the left table
    int rightIndex;
    String rfname;  // The file name where the right table is materialized

    static int filenum = 0;  // To get unique filenum for this operation

    Batch outBatch;  // Output buffer
    Batch leftBatch;  // Buffer for the left input stream
    Batch rightBatch;  // Buffer for the right input stream
    Batch tempRightBatch;
    ObjectInputStream in;  // File pointer to the right hand materialized file

    int lcurs;  // Cursor for left side buffer
    int rcurs;  // Cursor for right side buffer
    int tempRCurs;
    boolean eosl;  // Indicates whether end of left table stream is reached
    boolean eosr;  // End of right table stream is reached

    ExternalMergeSort sortedLeft;
    ExternalMergeSort sortedRight;


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

        // Initialize the cursors of the input buffers
        lcurs = 0;  rcurs = 0;
        eosl = false;
        eosl = false;

        sortedLeft = new ExternalMergeSort(left, leftIndex, OpType.SORT);
        sortedLeft.setNumBuff(getNumBuff());
        sortedRight = new ExternalMergeSort(right, rightIndex, OpType.SORT);
        sortedRight.setNumBuff(getNumBuff());

        // Right-hand side table is to be materialized for the
        // Block Nested Join to perform.
        if (!sortedRight.open()) {
            return false;
        } else {
            filenum++;
            rfname = "SMJtemp-" + String.valueOf(filenum);

            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                Batch rightBatch;
                while ((rightBatch = sortedRight.next()) != null) {
                    out.writeObject(rightBatch);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("SortMergeJoin:writing the temporary file error");
                return false;
            }

            if (!sortedRight.close()) {
                return false;
            }
        }

        if (!sortedLeft.open()) {
            return false;
        }

        leftBatch = (Batch) sortedLeft.next();
        rightBatch = (Batch) sortedRight.next();
        return true;
    }

    public void incRightBuffer() {
        if (rcurs == rightBatch.size() - 1) {
            rightBatch = sortedRight.next();
            rcurs = 0;
        }
        rcurs++;
    }

    public void incLeftBuffer() {
        if (lcurs == leftBatch.size() - 1) {
            leftBatch = sortedLeft.next();
            lcurs = 0;
        }
        lcurs++;
    }

    public void saveRightState() {
        in.mark(Integer.MAX_VALUE);
        tempRCurs = rcurs;
        tempRightBatch = rightBatch;
    }

    public void restoreRightState() throws IOException {
        in.reset();
        rcurs = tempRCurs;
        rightBatch = tempRightBatch;
    }

    public int compareLeftRight() {
        return Tuple.compareTuples(leftBatch.elementAt(lcurs), rightBatch.elementAt(rcurs), leftIndex, rightIndex);
    }

    public Batch next() {
        // End of left stream
        if (eosl || eosr) {
            close();
            return null;
        }

        // 1 batch buffer for output
        outBatch = new Batch(batchSize);
        try {
            in = new ObjectInputStream(new FileInputStream(rfname));
        } catch (IOException io) {
            System.out.println("SortMergeJoin:writing the temporary file error");
            System.exit(1);
        }

        while (!outBatch.isFull()) {

            try {
                if (leftBatch == null || rightBatch == null) {
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
                        eosr = true;
                        return outBatch;
                    }
                }

                if (compareLeftRight() == 0) {
                    // Save state to go back later
                    saveRightState();

                    while (compareLeftRight() == 0) {
                        Tuple leftTuple = leftBatch.elementAt(lcurs);
                        Tuple rightTuple = rightBatch.elementAt(rcurs);
                        Tuple outTuple = leftTuple.joinWith(rightTuple);
                        incRightBuffer();

                        outBatch.add(outTuple);
                        if (outBatch.isFull()) {
                            if (rightBatch == null || compareLeftRight() != 0) {
                                restoreRightState();
                                incLeftBuffer();
                            }
                            return outBatch;
                        }
                    }
                    restoreRightState();
                    incLeftBuffer();
                }

            }  catch (EOFException e) {
                try {
                    in.close();
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin:Error in temporary file reading");
                }
                eosr=true;

            } catch (IOException io) {
                System.out.println("BlockNestedJoin:temporary file reading error");
                System.exit(1);
            }
        }  // outbatch while not full

        return outBatch;
    }

    /** Close the operator */
    public boolean close(){
        File f = new File(rfname);
        f.delete();
        return true;
    }

}
