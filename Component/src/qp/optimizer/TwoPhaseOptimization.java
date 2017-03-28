package qp.optimizer;

import java.lang.Math;
import java.util.Vector;
import java.util.Random;

import qp.utils.*;
import qp.operators.*;

/** 
 * performs randomized optimization, two phase optimization algorithm
 */
public class TwoPhaseOptimization {

    /** enumeration of different ways to find the neighbor plan **/

    public static final int METHODCHOICE = 0;  //selecting neighbor by changing a method for an operator
    public static final int COMMUTATIVE = 1;   // by rearranging the operators by commutative rule
    public static final int ASSOCIATIVE = 2;  // rearranging the operators by associative rule

    /** Number of altenative methods available for a node as specified above */

    public static final int NUMCHOICES = 3;


    SQLQuery sqlquery;     // Vector of Vectors of Select + From + Where + GroupBy
    int numJoin;          // Number of joins in this query plan


    /** constructor **/

    public TwoPhaseOptimization(SQLQuery sqlquery){
        this.sqlquery = sqlquery;
    }


    /** Randomly selects a neighbour **/


    protected Operator getNeighbor(Operator root){
        //Randomly select a node to be altered to get the neighbour
        int nodeNum = RandNumb.randInt(0,numJoin-1);

        //Randomly select type of alteration: Change Method/Associative/Commutative
        int changeType = RandNumb.randInt(0,NUMCHOICES-1);
        Operator neighbor=null;

        switch(changeType){
            case  METHODCHOICE:   // Select a neighbour by changing the method type
                neighbor = neighborMeth(root,nodeNum);
                break;
            case COMMUTATIVE:
                neighbor=neighborCommut(root,nodeNum);
                break;
            case ASSOCIATIVE:
                neighbor=neighborAssoc(root,nodeNum);
                break;
        }
        return neighbor;
    }


    /**
     * Performs iterative improvement
     */
    public Operator getOptimalPlanByIterativeImprovment(Operator initPlan) {

        boolean isLocalMinimumReached = false;
        int initCost = getPlanCost(initPlan);

        if (numJoin != 0) {

            int minNeighborCost;
            Operator minNeighbor;

            while (!isLocalMinimumReached) {   // flag = false when local minimum is reached
                Operator initPlanCopy = (Operator) initPlan.clone();

                // Initialization
                minNeighbor = initPlanCopy;
                minNeighborCost = initCost;

                /** 
                 * In this loop we consider from the
                 * possible neighbors (randomly selected)
                 * and take the minimum among for next step
                 */
                for (int i = 1;  i < 2 * numJoin;  i++) {
                    Operator neighbor = getNeighbor((Operator) initPlan.clone());
                    int neighborCost = getPlanCost(neighbor);

                    if (neighborCost < minNeighborCost) {
                        minNeighbor = neighbor;
                        minNeighborCost = neighborCost;
                    }
                }

                if (minNeighborCost < initCost) {
                    initPlan = minNeighbor;
                    initCost = minNeighborCost;
                } else {
                    // Neighbor with lower cost not found
                    isLocalMinimumReached = true;
                }
            }  // While local minimum is not reached
        }  // if numjoin != 0

        return initPlan;
    }

    
    /**
     * Performs Simulated Annealing.
     * 
     * Parameters:
     * - Temperature: 2 * initCost
     * - Frozen(Stopping condition): Temperature smaller than 1
     *                                  and initCost unchanged for 4 steps
     * - Number of moves for each stage: 4 * numJoin
     * - uphill probability: e^-C/T
     * - Reduce temperature: 0.95
     */
    public Operator getOptimalPlanBySimulatedAnnealing(Operator initPlan) {

        int initCost = getPlanCost(initPlan);
        int numMoves = 4 * numJoin;
        double temperature = 2.0 * initCost;
        Random random = new Random();

        while (temperature >= 1) {

            for (int i = 0;  i < numMoves;  i++) {
                Operator neighbor = getNeighbor((Operator) initPlan.clone());
                int neighborCost = getPlanCost(neighbor);

                if (neighborCost < initCost) {
                    initCost = neighborCost;
                    initPlan = neighbor;
                } else {
                    int delta = neighborCost - initCost;
                    double probability = Math.exp(((double)-delta) / temperature);

                    if (random.nextDouble() <= probability) {
                        initCost = neighborCost;
                        initPlan = neighbor;
                    }
                }
            }

            temperature = temperature * 0.95;
        }
        return initPlan;
    }


    /** 
     * Implementation of Two Phase Algorithm
     * for Randomized optimization of Query Plan
     **/
    public Operator getOptimizedPlan() {

        // Get an initial plan for the given sql query
        RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
        numJoin = rip.getNumJoins();

        int MINCOST = Integer.MAX_VALUE;
        Operator finalPlan = null;


        // NUMTER is number of times random restart
        int NUMITER = (numJoin == 0 ? 1 : numJoin * 2);

        System.out.println("---------------------------Initial Plan----------------");

        /* Randomly restart the gradient descent until
         * the maximum specified number of random restarts (NUMITER)
         * has satisfied
         */
        for (int j=0; j<NUMITER; j++) {

            Operator initPlan = rip.prepareInitialPlan();

            modifySchema(initPlan);

            // Phase one
            initPlan = getOptimalPlanByIterativeImprovment(initPlan);

            // Phase two
            initPlan = getOptimalPlanBySimulatedAnnealing(initPlan);

            int initCost = getPlanCost(initPlan);
            Debug.PPrint(initPlan);
            System.out.println("  "+initCost);

            if (initCost < MINCOST){
                MINCOST = initCost;
                finalPlan = initPlan;
            }
        }

        System.out.println("\n\n\n");
        System.out.println("---------------------------Final Plan----------------");
        Debug.PPrint(finalPlan);
        System.out.println("  "+MINCOST);
        return finalPlan;
    }


    /** Selects a random method choice for join wiht number joinNum
     **  e.g., Nested loop join, Sort-Merge Join, Hash Join etc..,
     ** returns the modified plan
     **/

    protected Operator neighborMeth(Operator root, int joinNum){
        int numJMeth = JoinType.numJoinTypes();
        if (numJMeth > 1) {
            /** find the node that is to be altered **/
            Join node = (Join) findNodeAt(root,joinNum);
            int prevJoinMeth = node.getJoinType();
            int joinMeth = RandNumb.randInt(0,numJMeth-1);
            while(joinMeth == prevJoinMeth){
                joinMeth = RandNumb.randInt(0,numJMeth-1);
            }
            node.setJoinType(joinMeth);
        }
        return root;
    }



    /** Applies join Commutativity for the join numbered with joinNum
     **  e.g.,  A X B  is changed as B X A
     ** returns the modifies plan
     **/

    protected Operator neighborCommut(Operator root, int joinNum){
        /** find the node to be altered**/
        Join node = (Join) findNodeAt(root,joinNum);
        Operator left = node.getLeft();
        Operator right = node.getRight();
        node.setLeft(right);
        node.setRight(left);
        /*** also flip the condition i.e.,  A X a1b1 B   = B X b1a1 A  **/
        node.getCondition().flip();
        //Schema newschem = left.getSchema().joinWith(right.getSchema());
        // node.setSchema(newschem);

        /** modify the schema before returning the root **/
        modifySchema(root);
        return root;
    }

    /** Applies join Associativity for the join numbered with joinNum
     **  e.g., (A X B) X C is changed to A X (B X C)
     **  returns the modifies plan
     **/

    protected Operator neighborAssoc(Operator root,int joinNum){
        /** find the node to be altered**/
        Join op =(Join) findNodeAt(root,joinNum);
        //Join op = (Join) joinOpList.elementAt(joinNum);
        Operator left = op.getLeft();
        Operator right = op.getRight();

        if(left.getOpType() == OpType.JOIN && right.getOpType() != OpType.JOIN){
            transformLefttoRight(op,(Join) left);

        }else if(left.getOpType() != OpType.JOIN && right.getOpType()==OpType.JOIN){
            transformRighttoLeft(op,(Join) right);
        }else if(left.getOpType()==OpType.JOIN && right.getOpType()==OpType.JOIN){
            if(RandNumb.flipCoin())
                transformLefttoRight(op,(Join) left);
            else
                transformRighttoLeft(op,(Join) right);
        }else{
            // The join is just A X B,  therefore Association rule is not applicable
        }

        /** modify the schema before returning the root **/
        modifySchema(root);
        return root;
    }



    /** This is given plan (A X B) X C **/
    protected void transformLefttoRight(Join op, Join left) {
        Operator right = op.getRight();
        Operator leftleft = left.getLeft();
        Operator leftright = left.getRight();
        Attribute leftAttr = op.getCondition().getLhs();
        Join temp;

        /** CASE 1 :  ( A X a1b1 B) X b4c4  C     =  A X a1b1 (B X b4c4 C)
         ** a1b1,  b4c4 are the join conditions at that join operator
         **/

        if(leftright.getSchema().contains(leftAttr)) {

            temp = new Join(leftright,right,op.getCondition(),OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(leftleft);
            op.setJoinType(left.getJoinType());
            op.setNodeIndex(left.getNodeIndex());
            op.setRight(temp);
            op.setCondition(left.getCondition());

        } else {
            /**CASE 2:   ( A X a1b1 B) X a4c4  C     =  B X b1a1 (A X a4c4 C)
             ** a1b1,  a4c4 are the join conditions at that join operator
             **/
            temp = new Join(leftleft,right,op.getCondition(),OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(leftright);
            op.setRight(temp);
            op.setJoinType(left.getJoinType());
            op.setNodeIndex(left.getNodeIndex());
            Condition newcond = left.getCondition();
            newcond.flip();
            op.setCondition(newcond);
        }
    }


    /** This is given plan A X (B X C) **/
    protected void transformRighttoLeft(Join op, Join right) {

        Operator left = op.getLeft();
        Operator rightleft = right.getLeft();
        Operator rightright = right.getRight();
        Attribute rightAttr = (Attribute) op.getCondition().getRhs();
        Join temp;
        /** CASE 3 :  A X a1b1 (B X b4c4  C)     =  (A X a1b1 B ) X b4c4 C
         ** a1b1,  b4c4 are the join conditions at that join operator
         **/
        if (rightleft.getSchema().contains(rightAttr)) {
            temp = new Join(left,rightleft,op.getCondition(),OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(temp);
            op.setRight(rightright);
            op.setJoinType(right.getJoinType());
            op.setNodeIndex(right.getNodeIndex());
            op.setCondition(right.getCondition());

        } else {
            /** CASE 4 :  A X a1c1 (B X b4c4  C)     =  (A X a1c1 C ) X c4b4 B
             ** a1b1,  b4c4 are the join conditions at that join operator
             **/
            temp = new Join(left,rightright,op.getCondition(),OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());

            op.setLeft(temp);
            op.setRight(rightleft);
            op.setJoinType(right.getJoinType());
            op.setNodeIndex(right.getNodeIndex());
            Condition newcond = right.getCondition();
            newcond.flip();
            op.setCondition(newcond);
        }
    }


    /**
     * This method traverses through the query plan and
     * returns the node mentioned by joinNum
     */
    protected Operator findNodeAt(Operator node, int joinNum) {
        if (node.getOpType() == OpType.JOIN) {
            if (((Join)node).getNodeIndex() == joinNum) {
                return node;
            } else {
                Operator temp;
                temp= findNodeAt(((Join)node).getLeft(),joinNum);
                return (temp == null ? findNodeAt(((Join)node).getRight(),joinNum) : temp);
            }

        } else if (node.getOpType() == OpType.SCAN) {
            return null;
        } else if (node.getOpType() == OpType.SELECT) {
            //if sort/project/select operator
            return findNodeAt(((Select) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.PROJECT) {
            return findNodeAt(((Project) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.SORT) {
            return findNodeAt(((ExternalMergeSort) node).getBase(), joinNum);
        } else {
            return null;
        }
    }


    /**
     * Returns the cost of a plan.
     */
    public int getPlanCost(Operator plan) {
        PlanCost pc = new PlanCost();
        return pc.getCost(plan);
    }


    /**
     * Modifies the schema of operators which are modified due to selecing an alternative neighbor plan
     */
    private void modifySchema(Operator node){

        if (node.getOpType() == OpType.JOIN) {
            Operator left = ((Join) node).getLeft();
            Operator right = ((Join) node).getRight();
            modifySchema(left);
            modifySchema(right);
            node.setSchema(left.getSchema().joinWith(right.getSchema()));

        } else if (node.getOpType() == OpType.SELECT) {
            Operator base= ((Select)node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());

        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = ((Project)node).getBase();
            modifySchema(base);
            Vector attrlist = ((Project)node).getProjAttr();
            node.setSchema(base.getSchema().subSchema(attrlist));
        } else if (node.getOpType() == OpType.SORT) {
            Operator base = ((ExternalMergeSort)node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        }
    }



    /**
     * AFter finding a choice of method for each operator prepare an execution plan 
     * by replacing the methods with corresponding join operator implementation
     **/
    public static Operator makeExecPlan(Operator node) {

        if (node.getOpType() == OpType.JOIN) {
            Operator left = makeExecPlan(((Join) node).getLeft());
            Operator right = makeExecPlan(((Join) node).getRight());
            int joinType = ((Join) node).getJoinType();
            int numbuff = BufferManager.getBuffersPerJoin();
            switch (joinType) {

                case JoinType.NESTEDJOIN:

                    NestedJoin nj = new NestedJoin((Join) node);
                    nj.setLeft(left);
                    nj.setRight(right);
                    nj.setNumBuff(numbuff);
                    return nj;

                case JoinType.BLOCKNESTED:

                    BlockNestedJoin bj = new BlockNestedJoin((Join) node);
                    bj.setLeft(left);
                    bj.setRight(right);
                    bj.setNumBuff(numbuff);
                    return bj;

                case JoinType.SORTMERGE:

                    SortMergeJoin sm = new SortMergeJoin((Join) node);
                    sm.setLeft(left);
                    sm.setRight(right);
                    sm.setNumBuff(numbuff);
                    return sm;

                case JoinType.HASHJOIN:

                    NestedJoin hj = new NestedJoin((Join) node);
                    /* + other code */
                    return hj;

                default:
                    return node;
            }
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = makeExecPlan(((Select) node).getBase());
            ((Select) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = makeExecPlan(((Project) node).getBase());
            ((Project) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.SORT) {
            Operator base = makeExecPlan(((ExternalMergeSort) node).getBase());
            ((ExternalMergeSort) node).setBase(base);
            return node;
        } else {
            return node;
        }
    }
}













