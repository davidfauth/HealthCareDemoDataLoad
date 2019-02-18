package com.dfauth.imports;


import com.dfauth.schema.Labels;
import com.dfauth.schema.RelationshipTypes;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

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

public class ImportZipDistancesRunnable implements Runnable {

    private static final int TRANSACTION_LIMIT = 1000;
    private String file;
    private GraphDatabaseService db;
    private Log log;

    public ImportZipDistancesRunnable(String file, GraphDatabaseService db, Log log) {
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
            log.error("ImportCountiesRunnable Import - File not found: " + file);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("ImportCountiesRunnable Import - IO Exception: " + file);
        }


        HashSet<String> relationships = new HashSet<>();

        Transaction tx = db.beginTx();
        try {
            int count = 0;

            assert records != null;
            for (CSVRecord record : records) {
                count++;
                if (count>1){
                    Node zipFrom = null;
                    Node zipTo = null;
                    if(!record.get(0).isEmpty() && !record.get(1).isEmpty()){
                        int found=0;
                        zipFrom = db.findNode(Labels.PostalCode, "PostalCode", record.get(0));
                        zipTo = db.findNode(Labels.PostalCode, "PostalCode", record.get(1));
                        if (zipFrom != null && zipTo != null){
                            if (!relationships.contains(record.get(0)+record.get(1))){
                                float zipDist = Float.parseFloat(record.get(2));
                                Relationship rz = zipFrom.createRelationshipTo(zipTo, RelationshipTypes.HAS_DISTANCE);
                                rz.setProperty("Distance",zipDist);
                            }
                            relationships.add(record.get(0)+record.get(1));
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
