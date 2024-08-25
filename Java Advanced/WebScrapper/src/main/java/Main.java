import constant.Constants;
import dao.PageContentDAO;
import service.ContentProcessor;
import service.H2Database;
import service.WebScraper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;

public class Main {
    public static void main(String[] args) {
        // Initialize Objects
        H2Database db = new H2Database();
        WebScraper webScraper = new WebScraper();
        ContentProcessor processor = new ContentProcessor(db);

        // Get Links from user
        var links = getStartingLinks();

        // Setup config values of the scrapper
        webScraper.setStartingURLS(links);
        webScraper.setScrapeLimit(500);
        webScraper.setSameDomain(true);
        webScraper.setMaxDepth(2);
        webScraper.setDatabase(db);

        // Start the Scrapper
        webScraper.startScraping();

        // Start processor to process the db content
        processor.processDbContent();

        // Display the processed result
        try {
            PageContentDAO contentDAO = new PageContentDAO(db.getConnection());
            contentDAO.getAllWordFrequency().forEach(System.out::println);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        db.close();
    }

    /**
     * This method is used to get initial starting links for the scrapper.
     * @return List of string containing the starting links
     */
    private static ArrayList<String> getStartingLinks() {
        printSlowly(Constants.UserInterfaceConstants.WELCOME, 100);
        try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in))) {
            String userInput;
            ArrayList<String> links = new ArrayList<>();
            System.out.println("Enter link(s) to start scrapping: ");
            while(!(userInput = bufferedReader.readLine()).isEmpty()){
                userInput = userInput.trim();
                if(!isValidURL(userInput)) {
                    System.out.println("You have entered an invalid link! Try a correct one!");
                }
                links.add(userInput);
            }
            System.out.println(links.size());
            return links;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method is used to print a message with a specified delay to give slow printing effect
     * @param message The message that needs to be printed
     * @param delayInMillis The delay between each letter in milliseconds
     */
    public static void printSlowly(String message, int delayInMillis) {
        for (char ch : message.toCharArray()) {
            System.out.print(ch);
            System.out.flush();
            try {
                Thread.sleep(delayInMillis);
            } catch (InterruptedException e) {
                System.err.println("Printing interrupted");
                break;
            }
        }
        System.out.println();
    }

    /**
     * This method is used to check if a provided string is a valid url
     * @param url The url to check
     * @return true if url is valid else false
     */
    private static boolean isValidURL(String url) {
        return (url.startsWith("http://") || url.startsWith("https://"));
    }
}
