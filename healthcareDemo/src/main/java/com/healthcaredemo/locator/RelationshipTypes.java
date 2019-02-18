package com.healthcaredemo.locator;

import org.neo4j.graphdb.RelationshipType;

public enum  RelationshipTypes implements RelationshipType {
	EXPR_COUPON,
	COUPON_ITEM,
	STORE_PRICE,
	ING_ITEM,
	RECIPE_ING
}