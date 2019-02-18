package com.dfauth.imports;


import com.dfauth.schema.Labels;
import com.dfauth.schema.RelationshipTypes;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;

public class ImportLocationsRunnable implements Runnable {

    private static final int TRANSACTION_LIMIT = 1000;
    private String file;
    private GraphDatabaseService db;
    private Log log;

    public ImportLocationsRunnable(String file, GraphDatabaseService db, Log log) {
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
            log.error("ImportLocationsRunnable Import - File not found: " + file);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("ImportLocationsRunnable Import - IO Exception: " + file);
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
                Node city = null;
                if (!record.get("city_name").isEmpty()) {
                    city = db.createNode(Labels.City);
                    city.setProperty("geoname_id", record.get("geoname_id"));
                    city.setProperty("name", record.get("city_name"));
                }
                // Connect Country to Continent if necessary
                if (!relationships.contains("c2c" + record.get("country_iso_code") + "-" + record.get("continent_code"))) {
                    Node continent = getContinent(db, continents, record);
                    Node country = getCountry(db, countries, record);
                    country.createRelationshipTo(continent, RelationshipTypes.IN_LOCATION);
                    relationships.add("c2c" + record.get("country_iso_code") + "-" + record.get("continent_code"));
                }

                // Connect State to Country if necessary
                if (!record.get("subdivision_1_iso_code").isEmpty()) {
                    Node state = getState(db, states, record);

                    if (!relationships.contains("s2c" + record.get("subdivision_1_iso_code") + "-" + record.get("country_iso_code"))) {
                        Node country = getCountry(db, countries, record);
                        state.createRelationshipTo(country, RelationshipTypes.IN_LOCATION);
                        relationships.add("s2c" + record.get("subdivision_1_iso_code") + "-" + record.get("country_iso_code"));
                    }

                    // Connect State to Timezone if necessary
                    if (!relationships.contains("s2t" + record.get("subdivision_1_iso_code") + "-" + record.get("time_zone"))) {
                        Node timezone = getTimezone(db, timezones, record);
                        state.createRelationshipTo(timezone, RelationshipTypes.IN_TIMEZONE);
                        relationships.add("s2t" + record.get("subdivision_1_iso_code") + "-" + record.get("time_zone"));
                    }

                    if (!record.get("metro_code").isEmpty() && city != null) {
                        Node metro = getMetro(db, metros, record);
                        city.createRelationshipTo(metro, RelationshipTypes.IN_LOCATION);

                        // Connect Metro to State if necessary
                        if (!relationships.contains("m2s" + record.get("metro_code") + "-" + record.get("subdivision_1_iso_code"))) {
                            metro.createRelationshipTo(state, RelationshipTypes.IN_LOCATION);
                            relationships.add("m2s" + record.get("metro_code") + "-" + record.get("subdivision_1_iso_code"));
                        }
                    }
                    if (city != null) {
                        city.createRelationshipTo(state, RelationshipTypes.IN_LOCATION);
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

    private Node getCountry(GraphDatabaseService db, HashMap<String, Node> countries, CSVRecord record) {
        Node country = countries.get(record.get("country_iso_code"));
        if (country == null) {
            country = db.createNode(Labels.Country);
            country.setProperty("code", record.get("country_iso_code"));
            country.setProperty("name", record.get("country_name"));
            countries.put(record.get("country_iso_code"), country);
        }
        return country;
    }

    private Node getContinent(GraphDatabaseService db, HashMap<String, Node> continents, CSVRecord record) {
        Node continent = continents.get(record.get("continent_code"));
        if (continent == null) {
            continent = db.createNode(Labels.Continent);
            continent.setProperty("code", record.get("continent_code"));
            continent.setProperty("name", record.get("continent_name"));
            continents.put(record.get("continent_code"), continent);
        }
        return continent;
    }
}
