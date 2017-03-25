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
 *
 * In block nested loop join algorithm, we use 1 batch (page)
 * for output, 1 batch for right side table and the rest b-2
 * batches for left side table.
 */
public class BlockNestedJoin extends Join {

    int batchSize;  // Number of tuples per batch

    /**
     * The following fields are useful during execution
     * of BlockNestedJoin algorithm.
     */

    int leftIndex;  // Index of the join attribute in the left table
    int rightIndex;

    String rfname;  // The file name where the right table is materialized

    static int filenum = 0;  // To get unique filenum for this operation

    Batch outBatch;  // Output buffer
    Batch rightBatch;  // Buffer for the right input stream
    Batch[] leftBlock;  // Buffer for the left input stream
    int leftBlockSize;  // Number of batches in the buffer for the left input stream
    ObjectInputStream in;  // File pointer to the right hand materialized file

    int batchIndex;  // Index of current batch in left side block buffer
    int lcurs;  // Index of cursor in current batch
    int rcurs;  // Cursor for right side buffer
    boolean eosl;  // Indicates whether end of left table stream is reached
    boolean eosr;  // End of right table stream is reached


    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    /** During open finds the index of the join attributes,
     *  materializes the right hand side into a file and
     *  opens the connection.
     **/
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
        batchIndex = 0;  lcurs = 0;  rcurs = 0;
        eosl = false;
        // Because the right stream is to be repetitively scanned
        // If it reaches end, we have to start new scan
        eosr = true;

        // Right-hand side table is to be materialized for the
        // Block Nested Join to perform.
        if (!right.open()) {
            return false;
        } else {
            filenum++;
            rfname = "NJtemp-" + String.valueOf(filenum);

            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                Batch rightBatch;
                while ((rightBatch = right.next()) != null) {
                    out.writeObject(rightBatch);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin:writing the temporay file error");
                return false;
            }

            if (!right.close()) {
                return false;
            }
        }

        return left.open();
    }


    /**
     * From input buffers selects the tuples satisfying join condition
     * and returns a page of output tuples
     */
    public Batch next() {
        // End of left stream
        if (eosl) {
            close();
            return null;
        }

        // 1 batch buffer for output
        outBatch = new Batch(batchSize);

        while (!outBatch.isFull()) {

            // Check if we need to read new block of left table
            if (lcurs == 0 && batchIndex == 0 && eosr == true) {
                // Read b-2 batches of new left pages into buffer
                leftBlock = new Batch[getNumBuff() - 2];
                // It's possible that in the last loop, we don't
                // have enough numBuff-2 batches to read.
                leftBlockSize = 0;

                for (int i = 0;  i < getNumBuff() - 2;  i++) {
                    leftBlock[i] = (Batch) left.next();
                    leftBlockSize++;

                    if (leftBlock[i] == null) {
                        if (i == 0) {
                            eosl = true;
                            return outBatch;
                        }
                        break;
                    }
                }
            }

            // Whenever a new left page comes, we have to start the scanning of the 
            // right table
            try {
                in = new ObjectInputStream(new FileInputStream(rfname));
                eosr=false;
            } catch (IOException io) {
                System.err.println("BlockNestedJoin:error in reading the file");
                System.exit(1);
            }

            while (eosr == false) {
                try {
                    if (rcurs == 0 && lcurs == 0 && batchIndex == 0) {
                        rightBatch = (Batch) in.readObject();
                    }

                    for (int i = batchIndex;  i < leftBlockSize;  i++) {
                        for (int j = lcurs;  j < leftBlock[i].size();  j++) {
                            for (int t = rcurs;  t < rightBatch.size();  t++) {
                                Tuple leftTuple = leftBlock[i].elementAt(j);
                                Tuple rightTuple = rightBatch.elementAt(t);

                                if (leftTuple.checkJoin(rightTuple, leftIndex, rightIndex)) {
                                    Tuple outTuple = leftTuple.joinWith(rightTuple);

                                    outBatch.add(outTuple);
                                    if (outBatch.isFull()) {
                                        // If t is the last tuple in this right batch
                                        if (t == rightBatch.size() - 1) {
                                            // If j is the last tuple in this left batch
                                            if (j == leftBlock[i].size() - 1) {
                                                // If i is the last batch in this left block
                                                batchIndex = (i == leftBlockSize - 1 ? 0 : i + 1);
                                                lcurs = 0;
                                            } else {
                                                batchIndex = i;
                                                lcurs = j++;
                                            }
                                            rcurs = 0;
                                        } else {
                                            batchIndex = i;
                                            lcurs = j;
                                            rcurs = t + 1;
                                        }
                                    }
                                }
                            }  // t loop
                            rcurs = 0;
                        }  // j loop
                        lcurs = 0;
                    }  // i loop
                    batchIndex = 0;
                }  catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin:Error in temporary file reading");
                    }
                    eosr=true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin:Some error in deserialization ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin:temporary file reading error");
                    System.exit(1);
                }
            }  // eosr loop
        }  // outBatch not full loop

        return outBatch;
    }


    /** Close the operator */
    public boolean close(){
        File f = new File(rfname);
        f.delete();
        return true;

    }
}
