package service;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class H2Database {
    private final HikariDataSource dataSource;

    public H2Database() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:h2:mem:webcrawlerDb;DB_CLOSE_DELAY=-1");
        // -1 will prevent db from closing automatically after all connections are closed
        hikariConfig.setMaximumPoolSize(10);
        dataSource = new HikariDataSource(hikariConfig);
        initializeDatabase();
    }

    private void initializeDatabase() {
        try (Connection connection = getConnection(); Statement stmt = connection.createStatement()) {
            String createPageContentTable = "CREATE TABLE IF NOT EXISTS PageContent (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY, " +
                    "url VARCHAR(511), " +
                    "content TEXT)";

            String createWordFrequencyTable = "CREATE TABLE IF NOT EXISTS WordFrequency (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY, " +
                    "word VARCHAR(511), " +
                    "count INT)";

            stmt.execute(createPageContentTable);
            stmt.execute(createWordFrequencyTable);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

}
