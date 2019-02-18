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

public class ImportZipCountiesRunnable implements Runnable {

    private static final int TRANSACTION_LIMIT = 1000;
    private String file;
    private GraphDatabaseService db;
    private Log log;

    public ImportZipCountiesRunnable(String file, GraphDatabaseService db, Log log) {
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
            log.error("ImportZipCountiesRunnable Import - File not found: " + file);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("ImportZipCountiesRunnable Import - IO Exception: " + file);
        }

        HashMap<String, Node> continents = new HashMap<>();
        HashMap<String, Node> countries = new HashMap<>();
        HashMap<String, Node> metros = new HashMap<>();
        HashMap<String, Node> states = new HashMap<>();
        HashMap<String, Node> timezones = new HashMap<>();

        HashSet<String> relationships = new HashSet<>();

        Transaction tx = db.beginTx();
        try {
            int count = 0;

            assert records != null;
            for (CSVRecord record : records) {
                count++;
                if (count>1){
                    Node postalCode = null;
                    if(!record.get(0).isEmpty()){
                        postalCode = db.findNode(Labels.PostalCode, "PostalCode", record.get(0));
                        if (postalCode ==null){
                            postalCode = db.createNode(Labels.PostalCode);
                            postalCode.setProperty("PostalCode",record.get(0));
                        }
                    }
                    Node county = null;
                    county = db.findNode(Labels.County, "fipsCode", record.get(1));
                        
                    if (postalCode !=null && county !=null){
                        int found=0;
                        for (Relationship r : postalCode.getRelationships(RelationshipTypes.IS_IN_COUNTY, Direction.OUTGOING)) {
                            String strFIPS = r.getEndNode().getProperty("fipsCode").toString();
                            if (strFIPS.equalsIgnoreCase(record.get(0))){
                                found=1;
                            }
                        }
                        if (found==0){
                            postalCode.createRelationshipTo(county, RelationshipTypes.IS_IN_COUNTY);
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
