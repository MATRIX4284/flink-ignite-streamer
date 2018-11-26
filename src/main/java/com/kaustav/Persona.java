package com.kaustav;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

public class Persona implements Serializable {


    /** Indexed field. Will be visible for SQL engine. */
    @QuerySqlField(index = true)
    private long id;

    /** Queryable field. Will be visible for SQL engine. */
    @QuerySqlField
    private String name;

    /** Queryable field. Will be visible for SQL engine. */
    @QuerySqlField
    private int age;



    public Persona(long id,String name,int age) {
        this.id=id;
        this.name=name;
        this.age=age;


    }



}