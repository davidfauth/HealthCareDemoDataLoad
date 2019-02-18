package com.dfauth.imports;


import com.dfauth.schema.Labels;
import com.dfauth.schema.RelationshipTypes;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;

public class ImportStateCountiesRunnable implements Runnable {

    private static final int TRANSACTION_LIMIT = 1000;
    private String file;
    private GraphDatabaseService db;
    private Log log;

    public ImportStateCountiesRunnable(String file, GraphDatabaseService db, Log log) {
        this.file = file;
        this.db = db;
        this.log = log;
    }

    @Override
    public void run() {
        Reader in;
        Iterable<CSVRecord> records = null;
        try {
            in = new FileReader("/" + file);
            records = CSVFormat.EXCEL.withHeader().parse(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            log.error("ImportStateCountiesRunnable Import - File not found: " + file);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("ImportStateCountiesRunnable Import - IO Exception: " + file);
        }

        Transaction tx = db.beginTx();
        try {
            int count = 0;

            assert records != null;
            for (CSVRecord record : records) {
                count++;
                if (count>1){
                    Node stateNode = null;
                    if(!record.get(0).isEmpty()){
                        stateNode = db.findNode(Labels.State, "StateCode", record.get(0));
                        if (stateNode ==null){
                            stateNode = db.createNode(Labels.State);
                            stateNode.setProperty("StateCode",record.get(0));
                        }
                    }
                    Node countyNode = null;
                    String leftFips = StringUtils.leftPad(record.get(1), 2, "0");
                    String rightFips = StringUtils.leftPad(record.get(2), 3, "0");
                    String fipsCode = leftFips + rightFips;
                    countyNode = db.findNode(Labels.County, "fipsCode", fipsCode);
                    
                    if (stateNode !=null && countyNode !=null){
                        countyNode.setProperty("CountyName",record.get(3));
                        int found=0;
                        for (Relationship r : countyNode.getRelationships(RelationshipTypes.IS_IN_STATE, Direction.OUTGOING)) {
                            String strStateCode = r.getEndNode().getProperty("StateCode").toString();
                            if (strStateCode.equalsIgnoreCase(record.get(0))){
                                found=1;
                            }
                        }
                        if (found==0){
                            countyNode.createRelationshipTo(stateNode, RelationshipTypes.IS_IN_STATE);
                        }
                    }       

                }

                

                if (count % TRANSACTION_LIMIT == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            tx.success();
        } finally {
            tx.close();
        }

    }

    

    
}
