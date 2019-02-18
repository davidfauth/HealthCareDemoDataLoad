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

public class ImportProvidersOtherName implements Runnable {

    private static final int TRANSACTION_LIMIT = 1000;
    private String file;
    private GraphDatabaseService db;
    private Log log;

    public ImportProvidersOtherName(String file, GraphDatabaseService db, Log log) {
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
            log.error("ImportProvidersRunnable Import - File not found: " + file);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("ImportProvidersRunnable Import - IO Exception: " + file);
        }

        Transaction tx = db.beginTx();
        try {
            int count = 0;

            assert records != null;
            for (CSVRecord record : records) {
                count++;
                if (count>1){
                    Node provider = null;
                    if(!record.get(0).isEmpty()){
                        provider = db.findNode(Labels.Provider, "NPI", record.get(0));
                        if (provider ==null){
                            provider = db.createNode(Labels.Provider);
                            provider.setProperty("NPI",record.get(0));
                            provider.setProperty("otherName",record.get(1));
                            provider.setProperty("otherNameType",record.get(2));
                        } else {
                            provider.setProperty("otherName",record.get(1));
                            provider.setProperty("otherNameType",record.get(2));
                        }
                    }
                

                    if (count % TRANSACTION_LIMIT == 0) {
                        tx.success();
                        tx.close();
                        tx = db.beginTx();
                    }
                }
            }
            tx.success();
        } finally {
            tx.close();
        }
    }
}
