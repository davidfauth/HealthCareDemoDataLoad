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

public class ImportDocGraphRunnable implements Runnable {

    private static final int TRANSACTION_LIMIT = 20000;
    private String file;
    private GraphDatabaseService db;
    private Log log;

    public ImportDocGraphRunnable(String file, GraphDatabaseService db, Log log) {
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
            records = CSVFormat.EXCEL.parse(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            log.error("ImportDocGraphRunnable Import - File not found: " + file);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("ImportDocGraphRunnable Import - IO Exception: " + file);
        }

    
        HashSet<String> relationships = new HashSet<>();

        Transaction tx = db.beginTx();
        try {
            int count = 0;

            assert records != null;
            for (CSVRecord record : records) {
                count++;
                Node fromProvider = null;
                Node toProvider = null;
                if(!record.get(0).isEmpty()){
                    fromProvider = db.findNode(Labels.Provider, "NPI", record.get(0));
                    toProvider = db.findNode(Labels.Provider, "NPI", record.get(1));
                    if (fromProvider ==null){
                        fromProvider = db.createNode(Labels.Provider);
                        fromProvider.setProperty("NPI",record.get(0));
                        fromProvider.setProperty("SOURCE","docgraph");
                    }
                    if (toProvider ==null){
                        toProvider = db.createNode(Labels.Provider);
                        toProvider.setProperty("NPI",record.get(1));
                        toProvider.setProperty("SOURCE","docgraph");
                    }
                }
                Integer patient_count = Integer.parseInt(record.get(2));
                Integer transaction_count = Integer.parseInt(record.get(3));
                
                double averageDayWait = Double.parseDouble(record.get(4));
                double stdDayWait = Double.parseDouble(record.get(5));
                        
                Relationship r = fromProvider.createRelationshipTo(toProvider, RelationshipTypes.MADE_REFERRAL);
                r.setProperty("patientCount",patient_count);
                r.setProperty("transactionCount", transaction_count);
                r.setProperty("averageDayWait",averageDayWait);
                r.setProperty("stdDayWait",stdDayWait);
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
