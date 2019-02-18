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

import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D;
import static org.neo4j.values.storable.Values.pointValue;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;

public class ImportGeoCodedPostalCodesRunnable implements Runnable {

    private static final int TRANSACTION_LIMIT = 1000;
    private String file;
    private GraphDatabaseService db;
    private Log log;

    public ImportGeoCodedPostalCodesRunnable(String file, GraphDatabaseService db, Log log) {
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
            records = CSVFormat.TDF.withHeader().parse(in);
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
                    Node postalCode = null;
                    if(!record.get(0).isEmpty()){
                        double latValue = Double.parseDouble(record.get(5).trim());
                        double longValue = Double.parseDouble(record.get(6).trim());
                        System.out.println(record.get(0));
                        postalCode = db.findNode(Labels.PostalCode, "PostalCode", record.get(0));
                        if (postalCode != null){
                            postalCode.setProperty("latitude",latValue);
                            postalCode.setProperty("longitude",longValue);
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

    private Node getMetro(GraphDatabaseService db, HashMap<String, Node> metros, CSVRecord record) {
        Node metro = metros.get(record.get("metro_code"));
        if (metro == null) {
            metro = db.createNode(Labels.Metro);
            metro.setProperty("code", record.get("metro_code"));
        }
        return metro;
    }

    private Node getTimezone(GraphDatabaseService db, HashMap<String, Node> timezones, CSVRecord record) {
        Node timezone = timezones.get(record.get("time_zone"));
        if (timezone == null) {
            timezone = db.createNode(Labels.Timezone);
            timezone.setProperty("name", record.get("time_zone"));
            timezones.put(record.get("time_zone"), timezone);
        }
        return timezone;
    }

    private Node getState(GraphDatabaseService db, HashMap<String, Node> states, CSVRecord record) {
        Node state = states.get(record.get("country_iso_code") + "-" + record.get("subdivision_1_iso_code"));
        if (state == null) {
            state = db.createNode(Labels.State);
            state.setProperty("code", record.get("subdivision_1_iso_code"));
            state.setProperty("name", record.get("subdivision_1_name"));
            states.put(record.get("country_iso_code") + "-" + record.get("subdivision_1_iso_code"), state);
        }
        return state;
    }

    
}
