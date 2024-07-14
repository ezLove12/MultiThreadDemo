package org.viethm.multithread;

import java.sql.*;
import java.util.concurrent.Callable;

public class DatabaseTask implements Runnable {
    //Start record
    private int start;
    //End record
    private int end;

    public DatabaseTask(int start, int end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName());
        Connection sourceConn = null;
        Connection targetConn = null;

        PreparedStatement sourceStmt = null;
        PreparedStatement targetStmt = null;

        ResultSet rs = null;
        try {
            //Connect to source database
            sourceConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/threaddemo", "root", "123456");
            targetConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/threaddemo_new", "root", "123456");
            //Create statement to select data from source
            sourceStmt = sourceConn.prepareStatement("SELECT * FROM ENGINEER LIMIT ? OFFSET ?");
            //set limit and offset records
            sourceStmt.setInt(1, end-start);
            sourceStmt.setInt(2, start);

            rs = sourceStmt.executeQuery();
            //create a statement to insert data into target
            targetStmt = targetConn.prepareStatement("INSERT INTO ENGINEER (id, first_name, last_name, gender, country_id, title, created) values (?,?,?,?,?,?,?)");
            while (rs.next()) {
                targetStmt.setObject(1, rs.getObject("id"));
                targetStmt.setObject(2, rs.getObject("first_name"));
                targetStmt.setObject(3, rs.getObject("last_name"));
                targetStmt.setObject(4, rs.getObject("gender"));
                targetStmt.setObject(5, rs.getObject("country_id"));
                targetStmt.setObject(6, rs.getObject("title"));
                targetStmt.setObject(7, rs.getObject("created"));

                targetStmt.addBatch();
            }
            targetStmt.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            System.out.println("==========================================");
            System.out.println("CLOSE CONNECTION SUCCESS");
        }
    }
}
