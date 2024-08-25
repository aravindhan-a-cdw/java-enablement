package service;

import dao.PageContentDAO;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ContentProcessor {

    private final H2Database database;
    private ExecutorService threadPool;
    Map<String, Integer> frequency = new HashMap<>();

    public ContentProcessor(H2Database database) {
        this.database = database;
    }

    public void processDbContent() {
        System.out.println("Started Processing DB Content");
        threadPool = Executors.newFixedThreadPool(10);
        try {
            var connection = database.getConnection();
            PageContentDAO contentDAO = new PageContentDAO(connection);
            var contentIds = contentDAO.getAllPageContentId();
            contentIds.forEach(contentId -> {
                threadPool.submit(() -> {
                    try {
                        processPageContent(contentId);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
            });
            threadPool.shutdown();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
            storeFrequencyData(contentDAO);
        } catch (SQLException | InterruptedException e) {
            System.out.println("Exception at processDbContent" + e.getMessage());
        }
        System.out.println("Completed Processing DB Content");
    }

    private void storeFrequencyData(PageContentDAO contentDAO) {
        frequency.forEach((word, count) -> {
            try {
                contentDAO.saveWordFrequency(word, count);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void processPageContent(Integer contentId) throws SQLException {
        var connection = database.getConnection();
        PageContentDAO contentDAO = new PageContentDAO(connection);
        var pageContent = contentDAO.getPageContent(contentId);
        connection.close();
        Arrays.stream(pageContent.split(" ")).parallel().forEach(word -> {
            var currentValue = frequency.getOrDefault(word, 0);
            frequency.put(word, currentValue + 1);
        });
    }

}
