# Import Healthcare Sample Data via Stored Procedures
Import the Healthcare Sample Data into Neo4j using a Stored Procedure

This project requires Neo4j 3.4.x

Instructions
------------ 

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file, `target/importer-1.0-SNAPSHOT.jar`,
that can be copied to the `plugin` directory of your Neo4j instance.

    cp target/importer-1.0-SNAPSHOT.jar neo4j-enterprise-3.5.1/plugins/.


Edit your Neo4j/conf/neo4j.conf file by adding this line:

    dbms.security.procedures.unrestricted=com.dfauth.*    
    
(Re)start Neo4j

Create the schema:

    CALL com.dfauth.schema.generate;

Data Location(s):
	Data files are located at:
	
	TBD


Import the data: 
	call com.dfauth.schema.generate();

	call com.dfauth.import.providers("npidata_pfile_20050523-20180311.csv")

	call com.dfauth.import.geocodedAddresses("path to geocoded billing address","Billing");
	

	call com.dfauth.import.counties("ZIP_COUNTY_FIPS.csv");

	call com.dfauth.import.zipcounties("ZIP_COUNTY_032017.csv");

	call com.dfauth.import.zipdistances("gaz2015zcta5distance50miles.csv");

	using periodic commit 1000
	load csv with headers from "file:/Users/davidfauth/neo4j-Demos/data/gaz2015zcta5distance50miles.csv" as row
	with row 
	match (p1:PostalCode {PostalCode:row.zip1})
	match (p2:PostalCode {PostalCode:row.zip2})
	merge (p1)-[:HAS_DISTANCE {Distance:toFloat(row.mi_to_zcta5)}]->(p2);


	call com.dfauth.import.ziplatlong("/Users/davidfauth/neo4j-Demos/data/2017_Gaz_zcta_national.txt");
	
	call com.com.dfauth.import.docgraph("/Users/davidfauth/neo4j-Demos/data/docgraph_hop_teaming_2015.csv");


        
If using Windows you must escape the slashes like so:

    CALL com.dfauth.import.locations('C:\\Users\\dfauth\\Projects\\import_maxmind_sproc\\src\\main\\resources\\data\\GeoLite2-City-Locations-en.csv');    
    
    
