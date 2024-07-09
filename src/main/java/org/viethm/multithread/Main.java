package org.viethm.multithread;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    private static final int BATCH_SIZE = 10000;


    public static void main(String[] args) {
        int totalRecords = getTotalRecordCount();

        try (ExecutorService executor = Executors.newCachedThreadPool()) {
            List<Future<Void>> futures = new ArrayList<>();

            int start = 0;
            while(start < totalRecords) {
                int end = Math.min(start + BATCH_SIZE, totalRecords);
                futures.add(executor.submit(new DatabaseTask(start, end)));
                start = end;
            }

            for (Future<Void> future: futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            executor.shutdown();
        }

    }

    public static int getTotalRecordCount() {
        int count = 0;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/threaddemo", "root", "123456");
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT COUNT(*) FROM engineer");
            if(rs.next()) {
                count = rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return count;
    }
}