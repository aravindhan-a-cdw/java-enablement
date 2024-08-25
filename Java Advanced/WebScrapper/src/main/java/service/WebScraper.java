package service;

import dao.PageContentDAO;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WebScraper {
    private ArrayList<String> startingURLS;
    private final HashSet<String> visitedURLS = new HashSet<>();
    private HashSet<String> domains;
    private final AtomicInteger scrapeLimit = new AtomicInteger(Integer.MAX_VALUE);
    private Boolean sameDomain = false;
    private Integer maxDepth = Integer.MAX_VALUE;
    private ExecutorService threadPool;
    private H2Database database;
    public void setStartingURLS(ArrayList<String> links) {
        startingURLS = links;
    }
    public void setScrapeLimit(Integer scrapeLimit) {
        this.scrapeLimit.set(scrapeLimit);
    }
    public void setSameDomain(Boolean sameDomain) {
        this.sameDomain = sameDomain;
    }
    public void setMaxDepth(Integer maxDepth) {
        this.maxDepth = maxDepth;
    }
    public void setDatabase(H2Database database) { this.database = database; }

    public void startScraping() {
        // Assign thread pool to executor service
        threadPool = Executors.newCachedThreadPool();
        if(sameDomain) {
            // Create unique list of domains from the given links
            domains = new HashSet<>();
            startingURLS.forEach(link -> {
                try {
                    var url = URI.create(link).toURL();
                    domains.add(url.getProtocol() + "://" + url.getHost());
                } catch (MalformedURLException e) {
                    System.out.println("Invalid url entered: " + link);
                }
            });
        }
        // Start scraping in threads with the given links
        startingURLS.forEach(url -> {
            System.out.println("Starting Scraping with: " + url);
            threadPool.submit(() -> scrape(url, 0));
        });
        // Wait for 10 minutes before termination or shutdown now
        try {
            if(!threadPool.awaitTermination(10, TimeUnit.MINUTES)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Scraping completed");
        System.out.println("--------------------");
    }

    public void scrape(String url, Integer depth) {
        if(scrapeLimit.get() < 0) {
            // The limit has reached and hence don't proceed
            return;
        }
        if (scrapeLimit.getAndDecrement() == 0) {
            // Limit has reached first and hence don't accept more tasks and shutdown
            threadPool.shutdown();
            return;
        }
        visitedURLS.add(url);
        try {
            Document parsedBody = Jsoup.connect(url).get();
            // Save the parsed content in db
            var connection = database.getConnection();
            PageContentDAO pageContentDAO = new PageContentDAO(connection);
            pageContentDAO.savePageContent(url, parsedBody.text());
            // Close connection after usage so that it will be used by other threads
            connection.close();
            System.out.println("Done scraping: " + url);
            Elements links = parsedBody.select("a[href]");
            // Look for more links to scrape from the content
            scrapeLinks(links, depth);
        } catch (IOException | SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void scrapeLinks(Elements links, Integer currentDepth) {
        // When depth is more than limit or scrape limit is reached then don't proceed
        if(currentDepth >= maxDepth || scrapeLimit.get() <= 0) return;

        links.forEach(link -> {
            try {
                var url = URI.create(link.absUrl("href")).toURL();
                var domainWithScheme = url.getProtocol() + "://" + url.getHost();
                // Only proceed to scrape if link is not visited and contains same domain if configured
                if(!(sameDomain && domains.contains(domainWithScheme)) || visitedURLS.contains(url.toString())){
                    return;
                } else {
                    threadPool.submit(() -> scrape(url.toString(), currentDepth + 1));
                }
            } catch (MalformedURLException e) {
                System.out.println("Incorrect url" + link.attr("href"));
            }
        });
    }
}
