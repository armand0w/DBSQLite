package com.armandow.db;

import com.armandow.db.exceptions.DBException;
import com.armandow.db.exceptions.DBValidationException;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Base64;

@Slf4j
public class DBSQLite {
    private final String fileName;

    public DBSQLite(String fileName) {
        this.fileName = fileName;
    }
    
    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + this.fileName);
    }

    private void closeConnection(Connection conn) throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    private <T> void evaluate(String label, Object value, Class<T> type) throws DBValidationException {
        if (value == null) {
            throw new DBValidationException("Element " + label + " cannot be null");
        }

        if (value.getClass() != type) {
            throw new DBValidationException("Invalid data type for element " + label);
        }
    }

    private String createDynamicFilter(Object filters, Object typeFilter) throws DBValidationException {
        var dynamicFilter = new StringBuilder();
        var isOr = typeFilter != null && typeFilter.toString().equals("OR");

        dynamicFilter.append("WHERE 1");

        if (filters != null && filters != JSONObject.NULL) {
            var aux = 0;

            for (var object : (JSONArray) filters) {
                var filter = (JSONObject) object;
                var field = filter.optQuery("/field");
                var operator = filter.optQuery("/operator");
                var total = filter.getJSONArray("values");

                if ((field != null && field.getClass() == String.class) && (operator != null && operator.getClass() == String.class) && !total.isEmpty()) {
                    var separator = "";
                    var init = "";
                    var end = "";

                    if (aux == 0) {
                        dynamicFilter.append(" AND ");
                        if (isOr) {
                            dynamicFilter.append(" ( ");
                        }
                        aux++;
                    } else if (isOr) {
                        dynamicFilter.append(" OR ");
                    } else {
                        dynamicFilter.append(" AND ");
                    }

                    dynamicFilter
                            .append(field).append(" ")
                            .append(operator).append(" ");

                    switch (operator.toString().toUpperCase()) {
                        case "BETWEEN" -> separator = " AND ";
                        case "IN", "NOT IN" -> {
                            init = "(";
                            separator = ",";
                            end = ")";
                        }
                        default -> {
                        }
                    }

                    dynamicFilter.append(init);

                    for (var i = 0; i < total.length(); i++) {
                        var valueType = filter.optQuery("/values/" + i + "/type");
                        var valueValue = filter.optQuery("/values/" + i + "/value");

                        if (valueType == null || valueType.getClass() != String.class) {
                            throw new DBValidationException("Invalid value type in filter");
                        }

                        if (valueValue == null || valueValue.getClass() != String.class) {
                            throw new DBValidationException("Invalid value in filter");
                        }

                        if (!valueType.toString().equalsIgnoreCase("string") && !valueType.toString().equalsIgnoreCase("int")) {
                            throw new DBValidationException("Invalid value type '" + valueType + "' in filter");
                        }

                        if (i > 0) {
                            dynamicFilter.append(separator);
                        }

                        if (valueType.toString().equalsIgnoreCase("string")) {
                            dynamicFilter.append("'").append(valueValue).append("'");
                        }
                        if (valueType.toString().equalsIgnoreCase("int")) {
                            dynamicFilter.append(valueValue);
                        }
                    }

                    dynamicFilter.append(end);
                }
            }

            if (isOr)
                dynamicFilter.append(" ) ");
        }

        return dynamicFilter.toString();
    }

    public JSONObject executePagedQuery(String query, JSONObject jsonObject) throws Exception {
        var result = new JSONObject();
        var paged = jsonObject.optQuery("/paged");
        var filters = jsonObject.optQuery("/filters");
        var fieldOrder = jsonObject.optQuery("/fieldOrder");
        var typeOrder = jsonObject.optQuery("/typeOrder");
        var typeFilter = jsonObject.optQuery("/typeFilter");
        var currentPage = jsonObject.optQuery("/currentPage");
        var pageSize = jsonObject.optQuery("/pageSize");
        var maxPageScrollElements = jsonObject.optQuery("/maxPageScrollElements");
        var queryCount = jsonObject.optQuery("/queryCount");
        var orderBy = "";

        evaluate("fieldOrder", fieldOrder, Integer.class);
        evaluate("typeOrder", typeOrder, String.class);

        orderBy = String.format("ORDER BY %d %s", (Integer) fieldOrder, typeOrder);

        var dynamicFilter = createDynamicFilter(filters, typeFilter);
        var countQueryBase = "SELECT count(1) AS dataSize FROM (" + query + ") AS T " + dynamicFilter;
        var pagedQuery = "";

        if (paged != null && !jsonObject.getBoolean("paged")) {
            pagedQuery = String.format("SELECT * FROM (%s) AS T %s %s", query, dynamicFilter, orderBy);
        } else {
            evaluate("currentPage", currentPage, Integer.class);
            evaluate("pageSize", pageSize, Integer.class);
            evaluate("maxPageScrollElements", maxPageScrollElements, Integer.class);

            pagedQuery = String.format("SELECT * FROM (%s) AS T %s %s LIMIT %d,%d", query, dynamicFilter, orderBy,
                    (((Integer) currentPage - 1) * (Integer) pageSize), (Integer) pageSize);

            result.put("currentPage", currentPage);
            result.put("pageSize", pageSize);
        }

        var resultCount = executeQuery(
                queryCount != null && queryCount.getClass() == String.class && dynamicFilter.equals("WHERE 1") ?
                        queryCount.toString() : countQueryBase,
                jsonObject.has("parameters") ? jsonObject.getJSONArray("parameters") : new JSONArray());
        var resultPaged = executeQuery(pagedQuery, jsonObject.has("parameters") ? jsonObject.getJSONArray("parameters") : new JSONArray());
        var dataSize = resultCount.optQuery("/data/0/dataSize");

        evaluate("dataSize", dataSize, Integer.class);

        result.put("totalRows", dataSize);
        result.put("table", resultPaged);

        if (paged != null && jsonObject.getBoolean("paged")) {
            evaluate("currentPage", currentPage, Integer.class);
            evaluate("pageSize", pageSize, Integer.class);
            evaluate("maxPageScrollElements", maxPageScrollElements, Integer.class);

            var pageScroller = PageScroller.create(
                    (Integer) maxPageScrollElements,
                    (Integer) dataSize,
                    (Integer) pageSize,
                    (Integer) currentPage);

            var pageScrollerArr = new JSONArray();
            for (Integer pageScroll : pageScroller) {
                pageScrollerArr.put(new JSONObject()
                        .put("label", pageScroll == null ? "..." : pageScroll.toString())
                        .put("page", pageScroll));
            }
            result.put("pageScroller", pageScrollerArr);
        }

        return result;
    }

    public JSONObject executeQuery(String query) throws SQLException, DBException {
        return executeQuery(query, null);
    }

    public int executeUpdate(String query, JSONArray params) throws DBException, SQLException {
        var simpleDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        var simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = openConnection();
            preparedStatement = connection.prepareStatement(query);

            if (params != null) {
                for (var i = 0; i < params.length(); i++) {
                    var param = (JSONObject) params.get(i);

                    switch (param.getString("type")) {
                        case "int" -> preparedStatement.setInt(i + 1, param.optInt("value"));
                        case "long" -> preparedStatement.setLong(i + 1, param.optLong("value"));
                        case "decimal" -> preparedStatement.setBigDecimal(i + 1, param.optBigDecimal("value", null));
                        case "date" -> {
                            var date = new Date(simpleDateFormat.parse(param.optString("value")).getTime());
                            preparedStatement.setDate(i + 1, date);
                        }
                        case "datetime" -> {
                            var date = new Timestamp(simpleDateTimeFormat.parse(param.optString("value")).getTime());
                            preparedStatement.setTimestamp(i + 1, date);
                        }
                        case "string" -> {
                            preparedStatement.setString(i + 1, param.optString("value", null));
                        }
                        default -> preparedStatement.setObject(i + 1, param.opt("value"));
                    }
                }
            }

            var result = preparedStatement.executeUpdate();
            preparedStatement.close();
            return result;
        } catch (Exception e) {
            throw new DBException(e);
        } finally {
            if (preparedStatement != null && !preparedStatement.isClosed()) {
                try {
                    preparedStatement.close();
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
            closeConnection(connection);
        }
    }
    
    public JSONObject executeQuery(String query, JSONArray params) throws DBException, SQLException {
        var simpleDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        var simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        ResultSetMetaData resultSetMetaData;

        try {
            connection = openConnection();
            preparedStatement = connection.prepareStatement(query);

            if (params != null) {
                for (var i = 0; i < params.length(); i++) {
                    var param = (JSONObject) params.get(i);

                    switch (param.getString("type")) {
                        case "int" -> preparedStatement.setInt(i + 1, param.optInt("value"));
                        case "long" -> preparedStatement.setLong(i + 1, param.optLong("value"));
                        case "decimal" -> preparedStatement.setBigDecimal(i + 1, param.optBigDecimal("value", null));
                        case "date" -> {
                            var date = new Date(simpleDateFormat.parse(param.optString("value")).getTime());
                            preparedStatement.setDate(i + 1, date);
                        }
                        case "datetime" -> {
                            var date = new Timestamp(simpleDateTimeFormat.parse(param.optString("value")).getTime());
                            preparedStatement.setTimestamp(i + 1, date);
                        }
                        case "string" -> {
                            preparedStatement.setString(i + 1, param.optString("value", null));
                        }
                        default -> preparedStatement.setObject(i + 1, param.opt("value"));
                    }
                }
            }

            resultSet = preparedStatement.executeQuery();
            resultSetMetaData = resultSet.getMetaData();

            var jsonObject = new JSONObject();
            var jsonArray = new JSONArray();

            for (var i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                var column = new JSONObject();

                column.put("position", i);
                column.put("label", resultSetMetaData.getColumnLabel(i + 1));
                column.put("name", resultSetMetaData.getColumnName(i + 1));

                switch (resultSetMetaData.getColumnType(i + 1)) {
                    case Types.CHAR -> {
                        column.put("size", resultSetMetaData.getPrecision(i + 1));
                        column.put("type", "CHAR");
                    }
                    case Types.VARCHAR -> {
                        column.put("size", resultSetMetaData.getPrecision(i + 1));
                        column.put("type", "VARCHAR");
                    }
                    case Types.LONGVARCHAR -> {
                        column.put("size", resultSetMetaData.getPrecision(i + 1));
                        column.put("type", "LONGVARCHAR");
                    }
                    case Types.LONGVARBINARY -> {
                        column.put("size", resultSetMetaData.getPrecision(i + 1));
                        column.put("type", "LONGVARBINARY");
                    }
                    case Types.VARBINARY -> {
                        column.put("size", resultSetMetaData.getPrecision(i + 1));
                        column.put("type", "VARBINARY");
                    }
                    case Types.TIMESTAMP -> column.put("type", "TIMESTAMP");
                    case Types.DATE -> column.put("type", "DATE");
                    case Types.DECIMAL -> {
                        column.put("type", "DECIMAL");
                        column.put("precision", resultSetMetaData.getPrecision(i + 1));
                        column.put("scale", resultSetMetaData.getScale(i + 1));
                    }
                    case Types.DOUBLE -> {
                        column.put("type", "DOUBLE");
                        column.put("precision", resultSetMetaData.getPrecision(i + 1));
                        column.put("scale", resultSetMetaData.getScale(i + 1));
                    }
                    case Types.FLOAT, Types.REAL -> {
                        column.put("type", "FLOAT");
                        column.put("precision", resultSetMetaData.getPrecision(i + 1));
                        column.put("scale", resultSetMetaData.getScale(i + 1));
                    }
                    case Types.INTEGER, Types.BIGINT, Types.TINYINT, Types.SMALLINT -> column.put("type", "INTEGER");
                    default -> {
                        log.warn("UNKNOWN TYPE [{}] :: {}", resultSetMetaData.getColumnType(i + 1), column);
                        column.put("type", "UNKNOWN");
                    }
                }
                jsonArray.put(column);
            }

            jsonObject.put("columns", jsonArray);
            jsonArray = new JSONArray();

            while (resultSet.next()) {
                var element = new JSONObject();

                for (var i = 0; i < resultSetMetaData.getColumnCount(); i++) {
                    switch (resultSetMetaData.getColumnType(i + 1)) {
                        case Types.CHAR, Types.VARCHAR ->
                                element.put(resultSetMetaData.getColumnLabel(i + 1), unescapeValue(resultSet.getString(i + 1)));
                        case Types.LONGVARCHAR ->
                                element.put(resultSetMetaData.getColumnLabel(i + 1), resultSet.getString(i + 1));
                        case Types.LONGVARBINARY, Types.VARBINARY -> {
                            var bytes = resultSet.getBytes(i + 1);
                            if (bytes != null) {
                                element.put(resultSetMetaData.getColumnLabel(i + 1), Base64.getEncoder().encodeToString(resultSet.getBytes(i + 1)));
                            } else {
                                element.put(resultSetMetaData.getColumnLabel(i + 1), JSONObject.NULL);
                            }
                        }
                        case Types.TIMESTAMP -> {
                            try {
                                element.put(resultSetMetaData.getColumnLabel(i + 1), simpleDateTimeFormat.format(resultSet.getTimestamp(i + 1)));
                            } catch (NullPointerException e) {
                                element.put(resultSetMetaData.getColumnLabel(i + 1), resultSet.getString(i + 1) == null ? "" : resultSet.getString(i + 1));
                            }
                        }
                        case Types.DATE -> {
                            try {
                                element.put(resultSetMetaData.getColumnLabel(i + 1), simpleDateTimeFormat.format(resultSet.getDate(i + 1)));
                            } catch (NullPointerException e) {
                                element.put(resultSetMetaData.getColumnLabel(i + 1), resultSet.getString(i + 1) == null ? "" : resultSet.getString(i + 1));
                            }
                        }
                        case Types.DECIMAL ->
                                element.put(resultSetMetaData.getColumnLabel(i + 1), resultSet.getBigDecimal(i + 1));
                        case Types.DOUBLE ->
                                element.put(resultSetMetaData.getColumnLabel(i + 1), resultSet.getDouble(i + 1));
                        case Types.FLOAT, Types.REAL ->
                                element.put(resultSetMetaData.getColumnLabel(i + 1), resultSet.getFloat(i + 1));
                        case Types.INTEGER, Types.BIGINT, Types.SMALLINT, Types.TINYINT ->
                                element.put(resultSetMetaData.getColumnLabel(i + 1), resultSet.getInt(i + 1));
                        default -> {
                        }
                    }
                }

                jsonArray.put(element);
            }

            jsonObject.put("data", jsonArray);

            resultSet.close();
            preparedStatement.close();

            return jsonObject;
        } catch (Exception e) {
            throw new DBException(e);
        } finally {
            if (resultSet != null && !resultSet.isClosed()) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }

            if (preparedStatement != null && !preparedStatement.isClosed()) {
                try {
                    preparedStatement.close();
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
            closeConnection(connection);
        }
    }

    private Object scapeValue(Object value) {
        if (value == null)
            return null;

        if (value.getClass() == String.class) {
            var strValue = value.toString();
            strValue = strValue.replace("\\\\\"", "\"");
            strValue = strValue.replace("\\\\n", "\n");
            strValue = strValue.replace("\\\\r", "\r");
            strValue = strValue.replace("\\\\t", "\t");
            strValue = strValue.replace("\\\\\\\\", "\\");
            return strValue;
        }

        return value;
    }

    private String unescapeValue(String value) {
        if (value != null) {
            value = value.replace("\\\\", "\\\\\\\\");
            value = value.replace("\"", "\\\\\\\"");
            value = value.replace("\t", "\\\\t");
            value = value.replace("\r", "\\\\r");
            value = value.replace("\n", "\\\\n");
        }

        return value;
    }
}
