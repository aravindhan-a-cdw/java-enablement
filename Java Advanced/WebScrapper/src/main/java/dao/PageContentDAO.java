package dao;

import model.WordFrequency;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class PageContentDAO {
    private final Connection connection;

    public PageContentDAO(Connection connection) {
        this.connection = connection;
    }

    public Integer savePageContent(String url, String content) throws SQLException {
        String sql = "INSERT INTO PageContent (url, content) VALUES (?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
            stmt.setString(1, url);
            stmt.setClob(2, new java.io.StringReader(content));
            stmt.executeUpdate();

            try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getInt(1);
                } else {
                    throw new SQLException("Creating page content failed, no ID obtained.");
                }
            }
        }
    }

    public String getPageContent(Integer id) throws SQLException {
        String sql = "SELECT content FROM PageContent where id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, id);
            try (ResultSet result = stmt.executeQuery()) {
                if(result.next()) {
                    return result.getString("content");
                }
            }
        }
        return  null;
    }

    public List<Integer> getAllPageContentId() throws SQLException {
        String sql = "SELECT id FROM PageContent";
        List<Integer> idList = new LinkedList<>();
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            try (ResultSet result = stmt.executeQuery()) {
                while (result.next()) {
                    int id = result.getInt("id");
                    idList.add(id);
                }
            }
        }
        return idList;
    }

    public void saveWordFrequency(String word, Integer count) throws SQLException {
        String sql = "INSERT INTO WordFrequency (word, count) VALUES (?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, word);
            stmt.setInt(2, count);
            stmt.executeUpdate();
        }
    }

    public List<WordFrequency> getAllWordFrequency() throws SQLException {
        String sql = "SELECT * from WordFrequency";
        List<WordFrequency> wordFrequencies = new LinkedList<>();
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            try (ResultSet result = stmt.executeQuery()) {
                while (result.next()) {
                    String word = result.getString("word");
                    Integer count = result.getInt("count");
                    wordFrequencies.add(new WordFrequency(word, count));
                }
            }
        }
        return  wordFrequencies;
    }
}
