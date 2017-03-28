package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;


/**
 * External Sort-Merge algorithm.
 */
public class ExternalMergeSort extends Operator {

    Operator base;
    int batchSize;

    /** The following fields are useful during execution of
     ** the External Sort Merge algorithm.
     **/
    int keyIndex;  // Index of the key attribute in the table

    static int filenum = 0;  // To have a unique file name for this operation

    int numBuff;  // Number of buffer can be used

    Batch outBatch;  // Output buffer
    Batch[] inBatches;  // Input buffer
    Tuple[] tuples;  // Used when producing first pass runs

    ArrayList<Run> runs;  // Runs produced by the algorithm
    Run finalRun;  // Final run after sorting and merging
    int[] cursors;  // Cursor of batches in sorting

    boolean eos;  // Indicate whether we reach the end

    public ExternalMergeSort(Operator base, int keyIndex, int type) {
        super(type);
        this.base = base;
        this.keyIndex= keyIndex;
        this.schema = base.getSchema();
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return this.base;
    }

    public void setNumBuff(int numBuff) {
        this.numBuff = numBuff;
    }

    public int getNumBuff() {
        return numBuff;
    }

    /**
     * Sort the list of tuples with given index.
     */
    public void sortTuples(Tuple[] tuples, int tupleCount, int index) {
        Arrays.sort(tuples, 0, tupleCount, new Comparator<Tuple>() {
            public int compare(Tuple t1, Tuple t2) {
               return Tuple.compareTuples(t1, t2, index);
            }
        });
    }

    /**
     * Produce run with given list of tuples.
     */
    public void produceRunWith(Tuple[] tuples, int tupleCount) throws IOException {
        // Create new file to store run
        filenum++;
        String fileName = "EMStemp-" + filenum;
        ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(fileName));

        Batch batch = new Batch(batchSize);
        for (int i = 0;  i < tupleCount;  i++) {
            batch.add(tuples[i]);
            if (batch.isFull()) {
                out.writeObject(batch);
                batch = new Batch(batchSize);
            }
        }
        if (!batch.isEmpty()) {
            out.writeObject(batch);
        }
        out.close();

        // Wrap up in a new run
        runs.add(new Run(fileName));
    }

    /**
     * Use all buffers to produce sorted first pass merge runs.
     */
    public void createFirstPassMergeRuns() {
        Batch batch;
        // Use all available buffers for first pass
        int availNumTuples = batchSize * getNumBuff();
        tuples = new Tuple[availNumTuples];
        int tupleCount = 0;

        try {
            while ((batch = base.next()) != null) {
                for (int i = 0;  i < batch.size();  i++) {
                    tuples[tupleCount] = batch.elementAt(i);
                    tupleCount++;
                    if (tupleCount >= availNumTuples) {
                        sortTuples(tuples, tupleCount, keyIndex);
                        produceRunWith(tuples, tupleCount);
                        tupleCount = 0;
                    }
                }
            }
            if (tupleCount > 0) {
                sortTuples(tuples, tupleCount, keyIndex);
                produceRunWith(tuples, tupleCount);
            }
        } catch (IOException io) {
            System.out.println("EMStemp: Cannot write to file");
            System.exit(1);
        }

        // Free memory of tuples after used
        tuples = null;
    }

    /**
     * Return tuples with smallest key in all batches.
     */
    public Tuple getTupleWithSmallestKey(int starting, int ending, int keyIndex) {
        Tuple smallestTuple = null;
        int runIndex = -1;  // Run contains smallest tuple

        for (int i = starting;  i < ending;  i++) {
            int index = i - starting;  // Index of batch of this run stored in memory
            if (inBatches[index] == null || cursors[index] >= inBatches[index].size()) {
                if (runs.get(i).eos) {
                    inBatches[index] = null;
                } else {
                    inBatches[index] = runs.get(i).next();
                    cursors[index] = 0;
                }
            }

            if (inBatches[index] != null) {
                Tuple tuple = inBatches[index].elementAt(cursors[index]);
                if (smallestTuple == null || Tuple.compareTuples(tuple, smallestTuple, keyIndex) < 0) {
                    smallestTuple = tuple;
                    runIndex = index;
                }
            }
        }
        if (smallestTuple != null) {
            // Update cursor of run containing smallest tuple
            int index = runIndex;
            cursors[index]++;
        }
        return smallestTuple;
    }

    public void performMergeSort() {
        ArrayList<Run> tempRuns = new ArrayList<Run>();

        outBatch = new Batch(batchSize);
        inBatches = new Batch[getNumBuff() - 1];
        // Merge numBuff-1 runs at a time
        for (int starting = 0;  starting < runs.size();  starting += getNumBuff() - 1) {
            try {
                filenum++;
                String fileName = "EMStemp-" + filenum;
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(fileName));

                int ending = Math.min(starting + getNumBuff() - 1, runs.size());
                cursors = new int[ending - starting];  // Cursor of batches in buffer
                for (int i = starting;  i < ending;  i++) {
                    runs.get(i).open();
                    // We will read new batch if cursor >= batch size,
                    // so initialize cursor with MAXVALUE
                    cursors[i - starting] = Integer.MAX_VALUE;
                }

                // Repeat finding smallest tuple and output it
                Tuple tuple;
                while ((tuple = getTupleWithSmallestKey(starting, ending, keyIndex)) != null) {
                    outBatch.add(tuple);
                    if (outBatch.isFull()) {
                        out.writeObject(outBatch);
                        outBatch = new Batch(batchSize);
                    }
                }
                // Print the rest of output buffer
                if (!outBatch.isEmpty()) {
                    out.writeObject(outBatch);
                }
                out.close();

                tempRuns.add(new Run(fileName));
           } catch (IOException io) {
               System.out.println("ExternalMergeSort: Error writing to temporary file");
               System.exit(1);
           }
        }

        // Delete all current runs and replace it with new runs
        for (int i = 0;  i < runs.size();  i++) {
            runs.get(i).close();
        }
        runs = tempRuns;
    }

    /**
     * Everything is done in open, including:
     * - Creates first pass merge runs
     * - Merge sort
     */
    public boolean open() {

        // Calculate number of tuples per batch
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;
        
        if (!base.open()) {
            return false;
        }

        runs = new ArrayList<Run>();
        createFirstPassMergeRuns();
        while (runs.size() > 1) {
            performMergeSort();
        }

        eos = false;
        if (!runs.isEmpty()) {
            finalRun = runs.get(0);
            finalRun.open();
        }
        return true;
    }

    public Batch next() {
        if (eos || finalRun == null) {
            close();
            return null;
        }
        Batch batch = finalRun.next();
        if (batch == null) {
            close();
            eos = true;
            return null;
        }
        return batch;
    }

    public boolean close() {
        if (finalRun != null) {
            finalRun.close();
        }
        return true;
    }

    /**
     * Represents a run produced by this algorithm.
     *
     * Each run will be read only once, so the run file will be deleted
     * in close.
     */
    public static class Run {

        public String fileName;
        public boolean eos;  // Indicates whether cursor reaches the end of run file
        public ObjectInputStream in; // InputStream for reading run file

        public Run(String fileName) {
            this.fileName = fileName;
        }

        public boolean open() {
            eos = false;
            try {
                in = new ObjectInputStream(new FileInputStream(fileName));
            } catch (IOException io) {
                System.out.println("ExternalMergeSort: Failed to load run");
                System.exit(1);
            }
            return true;
        }

        public Batch next() {
            if (eos) {
                close();
                return null;
            }

            try {
                Batch batch = (Batch) in.readObject();
                return batch;

            }  catch (EOFException e) {
                try {
                    in.close();
                } catch (IOException io) {
                    System.out.println("ExternalMergeSort:Error in temporary file reading");
                }
                close();
                eos = true;

            } catch (ClassNotFoundException c) {
                System.out.println("ExternalMergeSort:Some error in deserialization ");
                System.exit(1);

            } catch (IOException io) {
                System.out.println("ExternalMergeSort:temporary file reading error");
                System.exit(1);
            }
            return null;
        }

        public boolean close() {
            File f = new File(fileName);
            f.delete();
            return true;
        }
    }

    public Operator clone() {
        Operator newbase = (Operator) base.clone();
        ExternalMergeSort newsort = new ExternalMergeSort(newbase,keyIndex,optype);
        newsort.setSchema(newbase.getSchema());
        return newsort;
    }
}
