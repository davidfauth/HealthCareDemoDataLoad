package com.healthcaredemo.locator;


import org.neo4j.graphdb.Label;

public enum Labels implements Label {
    Ingredient,
    Recipe,
    Store,
    SEG,
    CouponExpireDate,
	Coupon,
	Item
}
