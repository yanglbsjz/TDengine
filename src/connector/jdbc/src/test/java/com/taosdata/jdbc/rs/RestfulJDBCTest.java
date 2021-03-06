package com.taosdata.jdbc.rs;

import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Random;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RestfulJDBCTest {

    private Connection connection;

    @Before
    public void before() throws ClassNotFoundException, SQLException {
        Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
        connection = DriverManager.getConnection("jdbc:TAOS-RS://master:6041/restful_test?user=root&password=taosdata");
    }

    @After
    public void after() throws SQLException {
        if (connection != null)
            connection.close();
    }


    /**
     * 查询所有log.log
     **/
    @Test
    public void testCase001() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from log.log");
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String column = metaData.getColumnLabel(i);
                    String value = resultSet.getString(i);
                    System.out.print(column + ":" + value + "\t");
                }
                System.out.println();
            }
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * create database
     */
    @Test
    public void testCase002() {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("drop database if exists restful_test");
            stmt.execute("create database if not exists restful_test");
            stmt.execute("use restful_test");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * create super table
     ***/
    @Test
    public void testCase003() {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("create table weather(ts timestamp, temperature float, humidity int) tags(location nchar(64), groupId int)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCase004() {
        try (Statement stmt = connection.createStatement()) {
            for (int i = 1; i <= 100; i++) {
                stmt.execute("create table t" + i + " using weather tags('beijing', '" + i + "')");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Random random = new Random(System.currentTimeMillis());

    @Test
    public void testCase005() {
        try (Statement stmt = connection.createStatement()) {
            int rows = 0;
            for (int i = 0; i < 10; i++) {
                for (int j = 1; j <= 100; j++) {
                    long currentTimeMillis = System.currentTimeMillis();
                    int affectRows = stmt.executeUpdate("insert into t" + j + " values(" + currentTimeMillis + "," + (random.nextFloat() * 50) + "," + random.nextInt(100) + ")");
                    Assert.assertEquals(1, affectRows);
                    rows += affectRows;
                }
            }
            Assert.assertEquals(1000, rows);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
