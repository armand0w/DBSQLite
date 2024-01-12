package com.armandow.db;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class DBExceptionTest {
    private static DBSQLite dbSqlite;

    @BeforeAll
    static void beforeAll() {
        dbSqlite = new DBSQLite("src/test/resources/test.db");
        assertNotNull(dbSqlite);
    }

    @Test
    void testConnection() {
        var testDB = new DBSQLite("test_error.db");
        assertNotNull(testDB);
        var e = assertThrows(Exception.class, () -> testDB.executeQuery("SELECT * FROM testTable"));
        assertNotNull(e);
        assertTrue(e.getMessage().contains("[SQLITE_ERROR] SQL error or missing database (no such table: testTable)"));
    }

    @Test
    void testPagedQuery_EvaluateError() throws Exception {
        var params = new JSONObject()
                .put("paged", false)
                .put("filters", JSONObject.NULL)
                //.put("fieldOrder", null)
                .put("typeOrder", "DESC")
                .put("typeFilter", "OR")
                .put("currentPage", 1)
                .put("pageSize", 5)
                .put("maxPageScrollElements", 11);

        var e = assertThrows(Exception.class, () -> dbSqlite.executePagedQuery("SELECT 1", params));
        assertNotNull(e);
        assertEquals("Element fieldOrder cannot be null", e.getMessage());

        params.put("fieldOrder", "!");
        e = assertThrows(Exception.class, () -> dbSqlite.executePagedQuery("SELECT 1", params));
        assertNotNull(e);
        assertEquals("Invalid data type for element fieldOrder", e.getMessage());
    }

    @Test
    void testPagedQuery_InvalidFilter() throws Exception {
        var params = new JSONObject()
                .put("paged", true)
                .put("filters", new JSONArray()
                        .put(new JSONObject()
                                .put("field", "length")
                                .put("operator", "IN")
                                .put("values", new JSONArray()
                                        .put(new JSONObject().put("type", false).put("value", 180.0))
                                )
                        )
                )
                .put("fieldOrder", 1)
                .put("typeOrder", "DESC")
                .put("typeFilter", "OR")
                .put("currentPage", 1)
                .put("pageSize", 5)
                .put("maxPageScrollElements", 11);
        log.trace(params.toString(2));

        var e = assertThrows(Exception.class, () -> dbSqlite.executePagedQuery("SELECT 1", params));
        assertNotNull(e);
        assertEquals("Invalid value type in filter", e.getMessage());

        params.put("filters", new JSONArray()
                .put(new JSONObject()
                        .put("field", "length")
                        .put("operator", "IN")
                        .put("values", new JSONArray()
                                .put(new JSONObject().put("type", "string").put("value", 180.0))
                        )
                )
        );
        log.trace(params.toString(2));
        e = assertThrows(Exception.class, () -> dbSqlite.executePagedQuery("SELECT 1", params));
        assertNotNull(e);
        assertEquals("Invalid value in filter", e.getMessage());

        params.put("filters", new JSONArray()
                .put(new JSONObject()
                        .put("field", "length")
                        .put("operator", "IN")
                        .put("values", new JSONArray()
                                .put(new JSONObject().put("type", "strings").put("value", "valorString"))
                        )
                )
        );
        log.trace(params.toString(2));
        e = assertThrows(Exception.class, () -> dbSqlite.executePagedQuery("SELECT 1", params));
        assertNotNull(e);
        assertEquals("Invalid value type 'strings' in filter", e.getMessage());
    }
}
