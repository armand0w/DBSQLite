package com.armandow.db;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class DBSQLiteTest {
    private static DBSQLite dbSqlite;

    @BeforeAll
    static void beforeAll() {
        dbSqlite = new DBSQLite("src/test/resources/test.db");
        assertNotNull(dbSqlite);
    }

    @Test
    void testExecuteQuery() throws Exception {
        var query = "SELECT * FROM category";
        var res = dbSqlite.executeQuery(query);
        assertNotNull( res );
        log.trace(res.toString(2));

        var parameters = new JSONArray()
                .put(new JSONObject().put("type", "long").put("value", 0L))
                .put(new JSONObject().put("type", "decimal").put("value", 1.0))
                .put(new JSONObject().put("type", "datetime").put("value", "2006-02-14 04:45:00"))
                .put(new JSONObject().put("type", "date").put("value", "2006-02-15"));
        log.trace(parameters.toString(2));

        res = dbSqlite.executeQuery(query + " WHERE category_id > ? AND category_id >= ? AND last_update BETWEEN ? AND ?", parameters);
        assertNotNull(res);
        log.trace(res.toString(2));
    }

    @Test
    void testCRUD() throws Exception {
        var insert = "INSERT INTO staff (staff_id, first_name, last_name, address_id, email, store_id, username, password) VALUES (?,?,?,?,?,?,?,?)";
        var parameters = new JSONArray()
                .put(new JSONObject().put("type", "int").put("value", "3"))
                .put(new JSONObject().put("type", "string").put("value", "Test"))
                .put(new JSONObject().put("type", "string").put("value", "User"))
                .put(new JSONObject().put("type", "int").put("value", "419"))
                .put(new JSONObject().put("type", "string").put("value", "test@mail.com"))
                .put(new JSONObject().put("type", "int").put("value", "1"))
                .put(new JSONObject().put("type", "string").put("value", "TestUser"))
                .put(new JSONObject().put("type", "string").put("value", "*Str0n6-P445Sw00Rd*"));
        log.trace(parameters.toString(2));

        var result = dbSqlite.executeUpdate(insert, parameters);
        assertEquals(1, result);

        parameters = new JSONArray()
                .put(new JSONObject().put("type", "string").put("value", "Updated"))
                .put(new JSONObject().put("type", "int").put("value", "2"));
        log.trace(parameters.toString(2));
        result = dbSqlite.executeUpdate("UPDATE staff SET username = ? WHERE staff_id > ?", parameters);
        assertEquals(1, result);

        log.info(dbSqlite.executeQuery("SELECT * FROM staff").toString(2));

        parameters = new JSONArray().put(new JSONObject().put("type", "int").put("value", "2"));
        log.trace(parameters.toString(2));
        result = dbSqlite.executeUpdate("DELETE FROM staff WHERE staff_id > ?", parameters);
        assertEquals(1, result);
    }

    @Test
    void testExecutePagedQuery_False() throws Exception {
        var res = dbSqlite.executePagedQuery("SELECT * FROM film",
                new JSONObject()
                        .put("paged", false)
                        .put("filters", JSONObject.NULL)
                        .put("fieldOrder", 1)
                        .put("typeOrder", "DESC")
                        .put("typeFilter", "AND")
                        .put("currentPage", 1)
                        .put("pageSize", 5)
                        .put("maxPageScrollElements", 11)
                        .put("queryCount", "SELECT COUNT(1) AS dataSize FROM film")
        );
        assertNotNull(res);
        log.trace(res.toString(2));

        var currentPage = res.query("/currentPage");
        var pageSize = res.query("/pageSize");
        var totalRows = res.query("/totalRows");
        var pageScroller = res.query("/pageScroller");

        assertNull(currentPage);
        assertNull(pageSize);
        assertNull(pageScroller);

        assertNotNull(totalRows);
        assertNotNull(totalRows.toString());
        assertEquals(Integer.class, totalRows.getClass());
        assertEquals(1000, totalRows);
    }

    @Test
    void testExecutePagedQuery() throws Exception {
        var query = "SELECT film_id, title, description, release_year, language_id, rental_duration, rental_rate, length, replacement_cost, rating, special_features, last_update FROM film";
        var params = new JSONObject()
                .put("paged", true)
                .put("filters", JSONObject.NULL)
                .put("fieldOrder", 1)
                .put("typeOrder", "DESC")
                .put("typeFilter", "AND")
                .put("currentPage", 1)
                .put("pageSize", 5)
                .put("maxPageScrollElements", 11);

        var res = dbSqlite.executePagedQuery(query, params);
        assertNotNull(res);
        log.trace(res.toString());

        var currentPage = res.query("/currentPage");
        var pageSize = res.query("/pageSize");
        var totalRows = res.query("/totalRows");
        var pageScrollerCount = res.getJSONArray("pageScroller");

        assertNotNull(currentPage);
        assertNotNull(currentPage.toString());
        assertEquals(Integer.class, currentPage.getClass());
        assertEquals(1, currentPage);

        assertNotNull(pageSize);
        assertNotNull(pageSize.toString());
        assertEquals(Integer.class, pageSize.getClass());
        assertEquals(5, pageSize);

        assertNotNull(totalRows);
        assertNotNull(totalRows.toString());
        assertEquals(Integer.class, totalRows.getClass());
        assertEquals(1000, totalRows);

        assertNotNull(pageScrollerCount);
        assertNotEquals(0, pageScrollerCount.length());
        assertEquals(11, pageScrollerCount.length());
    }

    @Test
    void testExecutePagedQuery_Filter() throws Exception {
        var query = "SELECT film_id, title, description, release_year, language_id, rental_duration, rental_rate, length, replacement_cost, rating, special_features, last_update FROM film";

        var params = new JSONObject()
                .put("paged", true)
                .put("fieldOrder", 1)
                .put("typeOrder", "DESC")
                .put("typeFilter", "OR")
                .put("currentPage", 1)
                .put("pageSize", 5)
                .put("maxPageScrollElements", 11)
                .put("queryCount", "SELECT COUNT(1) AS dataSize FROM film")
                .put("filters", new JSONArray()
                        .put(new JSONObject()
                                .put("field", "length")
                                .put("operator", "IN")
                                .put("values", new JSONArray()
                                        .put(new JSONObject().put("type", "int").put("value", "180"))
                                        .put(new JSONObject().put("type", "int").put("value", "185"))
                                )
                        )
                        .put(new JSONObject()
                                .put("field", "last_update")
                                .put("operator", "BETWEEN")
                                .put("values", new JSONArray()
                                        .put(new JSONObject().put("type", "string").put("value", "2006-02-15 05:00:00"))
                                        .put(new JSONObject().put("type", "string").put("value", "2006-02-15 06:00:00"))
                                )
                        )
                );
        log.trace(params.toString(2));

        var res = dbSqlite.executePagedQuery(query, params);
        assertNotNull(res);
        log.trace(res.toString(2));

        var currentPage = res.query("/currentPage");
        var pageSize = res.query("/pageSize");
        var totalRows = res.query("/totalRows");
        var pageScrollerCount = res.getJSONArray("pageScroller");

        assertNotNull(currentPage);
        assertNotNull(currentPage.toString());
        assertEquals(Integer.class, currentPage.getClass());
        assertEquals(1, currentPage);

        assertNotNull(pageSize);
        assertNotNull(pageSize.toString());
        assertEquals(Integer.class, pageSize.getClass());
        assertEquals(5, pageSize);

        assertNotNull(totalRows);
        assertNotNull(totalRows.toString());
        assertEquals(Integer.class, totalRows.getClass());
        assertEquals(1000, totalRows);

        assertNotNull(pageScrollerCount);
        assertNotEquals(0, pageScrollerCount.length());
        assertEquals(11, pageScrollerCount.length());
    }

    @Test
    void testCountQuery() throws Exception {
        var res = dbSqlite.executeCountQuery("SELECT COUNT(1) FROM staff");
        assertNotEquals(0, res);
        assertEquals(2, res);
    }
}
