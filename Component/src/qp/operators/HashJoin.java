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

public class HashJoin extends Join {

	int batchSize;  // Number of tuples per batch

	/**
	 * The following fields are useful during execution
	 * of HashJoin algorithm.
	 */

	int leftIndex;  // Index of the join attribute in the left table
	int rightIndex;

	static int filenum = 0;  // To get unique filenum for this operation

	Batch outBatch;  // Output buffer
	Batch inBatch;  // Buffer for the right input stream
	Batch[] buffers;  // Buffer for the left input stream

	ObjectInputStream in;  // File pointer to the right hand materialized file
	ObjectOutputStream out; //

	int batchIndex;  // Index of current batch in left side block buffer

	int lcurs; // Index of cursor in current batch
	int rcurs; // Cursor for right side buffer

	boolean eosl;  // Indicates whether end of left table stream is reached
	boolean eosr;  // End of right table stream is reached

	int leftBatchNo[];
	int rightBatchNo[];
	int numBucket;

	String lfname;
	String rfname;

	public HashJoin(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}

	public boolean open () {

		numBucket = numBuff - 1;

		filenum ++;

		Attribute leftattr = con.getLhs();
		leftIndex = left.getSchema().indexOf(leftattr);
		lfname = "HJLeftTemp-" + String.valueOf(filenum);
		int lefTupleSize = left.getSchema().getTupleSize();
		int leftBatchSize = Batch.getPageSize()/lefTupleSize;

		Attribute rightAttr = (Attribute) con.getRhs();
		rightIndex = right.getSchema().indexOf(rightAttr);
		rfname = "HJRightTemp-" + String.valueOf(filenum);
		int rightTupleSize = right.getSchema().getTupleSize();
		int rightBatchSize = Batch.getPageSize()/rightTupleSize;

		//partitioning of the left operator
		if (!left.open()) {
			return false;
		} else {

			buffers = new Batch[numBucket];
			leftBatchNo = new int[numBucket];

			for(int i=0; i<numBucket; i++) {
				buffers[i] = new Batch(leftBatchSize);
				leftBatchNo[i] = 0;
			}

			inBatch = left.next();

			while (inBatch != null) {
				for (int i = 0; i < inBatch.size(); i++) {
					Tuple tuple = inBatch.elementAt(i);
					int index = tuple.dataAt(leftIndex).hashCode() % numBucket;
					buffers[index].add(tuple);

					if (buffers[index].isFull()) {

						try {
							String fileName = lfname + "-" + String.valueOf(index) + "-" + String.valueOf(leftBatchNo[index]);
							out = new ObjectOutputStream(new FileOutputStream(fileName));
							out.writeObject(buffers[index]);
							leftBatchNo[index]++;
							out.close();

						} catch (IOException io) {
							System.out.println("HashJoin: writing the temporary file error");
							return false;
						}

						buffers[index] = new Batch(leftBatchSize);
					}
				}

				inBatch = left.next();
			}

			for (int i = 0; i < numBucket; i++) {
				if (!buffers[i].isEmpty()) {
					try {
						String fileName = lfname + "-" + String.valueOf(i) + "-" + String.valueOf(leftBatchNo[i]);
						out = new ObjectOutputStream(new FileOutputStream(fileName));
						out.writeObject(buffers[i]);
						leftBatchNo[i]++;
						out.close();

					} catch (Exception e) {
						System.out.println("HashJoin: writing the temporary file error");
						return false;
					}
				}

			}

			if (!left.close()) {
				return false;
			}
		}

		if (!right.open()) {
			return false;
		} else {
			buffers = new Batch[numBucket];
			rightBatchNo = new int[numBucket];

			for(int i=0; i<numBucket; i++) {
				buffers[i] = new Batch(rightBatchSize);
				rightBatchNo[i] = 0;
			}

			inBatch = right.next();

			while (inBatch != null) {
				for (int i = 0; i < inBatch.size(); i++) {
					Tuple tuple = inBatch.elementAt(i);
					int index = tuple.dataAt(rightIndex).hashCode() % numBucket;
					buffers[index].add(tuple);

					if (buffers[index].isFull()) {

						try {
							String fileName = rfname + "-" + String.valueOf(index) + "-" + String.valueOf(rightBatchNo[index]);
							out = new ObjectOutputStream(new FileOutputStream(fileName));
							out.writeObject(buffers[index]);
							rightBatchNo[index]++;
							out.close();

						} catch (IOException io) {
							System.out.println("HashJoin: writing the temporary file error");
							return false;
						}

						buffers[index] = new Batch(rightBatchSize);
					}
				}

				inBatch = right.next();
			}

			for (int i = 0; i < numBucket; i++) {
				if (!buffers[i].isEmpty()) {
					try {
						String fileName = rfname + "-" + String.valueOf(i) + "-" + String.valueOf(rightBatchNo[i]);
						out = new ObjectOutputStream(new FileOutputStream(fileName));
						out.writeObject(buffers[i]);
						rightBatchNo[i]++;
						out.close();

					} catch (Exception e) {
						System.out.println("HashJoin: writing the temporary file error");
						return false;
					}
				}
			}

			if (!right.close()) {
				return false;
			}
		}

		return true;
	}

	public Batch next() {
		close();
		return null;
	}

	public boolean close() {
		for(int i=0; i<numBucket; i++) {
			for(int j=0; j<leftBatchNo[i]; j++) {
				String fileName = lfname + "-" + String.valueOf(i) + "-" + String.valueOf(j);
				File f = new File(fileName);
				f.delete();
			}
		}


		for(int i=0; i<numBucket; i++) {
			for(int j=0; j<rightBatchNo[i]; j++) {
				String fileName = rfname + "-" + String.valueOf(i) + "-" + String.valueOf(j);
				File f = new File(fileName);
				f.delete();
			}
		}

		return true;
	}
}
