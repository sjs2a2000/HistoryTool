/**
 * Created by sjs2a on 11/12/2016.
 */

//Author: Scott Sontag

package com.history;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.*;

public class DownloadHistory {
    HashMap<String, String> fileMap = new HashMap<String, String>();
    HashMap<String, String> periodMap = new HashMap<>();
    String[] windows = new String[]{"d","w","m"};
    String[] exchanges = new String[]{ "nyse","amex","nasdaq"};

    {
        fileMap.put("d","c:/prices/daily/");
        fileMap.put("w", "c:/prices/weekly/");
        fileMap.put("m","c:/prices/monthly/");
    }

    DownloadHistory(int dPeriod, int wPeriod, int mPeriod) {
        java.util.Date date= new Date();
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int y = cal.get(Calendar.YEAR);
        int m = cal.get(Calendar.MONTH);
        int d = cal.get(Calendar.DAY_OF_MONTH);
        periodMap.put("d", String.format("a=%d&b=%d&c=%d&d=%d&e=%d&f=%d", m, d, y-dPeriod, m, d, y));
        System.out.println(String.format("a=%d&b=%d&c=%d&d=%d&e=%d&f=%d", m, d, y-dPeriod, m, d, y));
        periodMap.put("w", String.format("a=%d&b=%d&c=%d&d=%d&e=%d&f=%d", m, d, y-wPeriod, m, d, y));
        periodMap.put("m", String.format("a=%d&b=%d&c=%d&d=%d&e=%d&f=%d", m, d, y-mPeriod, m, d, y));
    }

    /*
    allows the modification of the period for the data via the commandline
     */
    public static void main(String[] args) {
        int dperiod=10, wperiod=10, mperiod=15;
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("d", "day", false, "period for day" );
        options.addOption( "m", "month", false, "period for month" );
        options.addOption( "y", "year", false, "period for year" );
        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption('d')){
                dperiod = Integer.parseInt(line.getOptionValue("d"));
            }
            if (line.hasOption('w')){
                wperiod = Integer.parseInt(line.getOptionValue("w"));
            }
            if (line.hasOption('m')){
                mperiod = Integer.parseInt(line.getOptionValue("m"));
            }
        }
        catch(ParseException exp){
            System.out.println( "Unexpected exception:" + exp.getMessage() );
        }
        DownloadHistory dl = new DownloadHistory(dperiod, wperiod, mperiod);
        try {
            dl.DownloadSymbols();
            dl.DownloadPrices();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void DownloadSymbols() throws IOException {
        //make the directory to store data if it does not exist
        System.out.println("DownloadSymbols");
;        File dir = new File("c:/prices/stocksdb"); //this throws error if it does not exist??
        System.out.println("test dir");
        if(dir.exists()) {
            System.out.println("dir exits for writing history");
        } else {
            System.out.println("creating directoy");
            dir.mkdirs();
        }
        for (String exch : exchanges) {
            System.out.println("processing " + exch);
            URL link = new URL(String.format("http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=%s&render=download",exch));
            FileUtils.copyURLToFile(link, new File(String.format("c:/prices/stocksdb/%s.txt", exch)));
        }
    }

    ArrayList<String> getSymbolList() throws FileNotFoundException {
        ArrayList<String> symbols = new ArrayList<String>();
        //load the files per exchange
        for(String exch : exchanges ) {
            File f = new File(String.format("c:/prices/stocksdb/%s.txt",exch));
            Scanner scanner = new Scanner(f);
            //scanner.useDelimiter(",");
            while (scanner.hasNextLine())
            {
                String line = scanner.nextLine();
                String[] fields = line.split(",");
                symbols.add(fields[0].replaceAll("\\s", ""));
            }
            scanner.close();
            System.out.println(exch);
        }
        System.out.println(symbols);
        return symbols;
    }

    void DownloadPrices() throws IOException
    {
        System.out.println("DownloadPrices");
        ArrayList<String> symbols = getSymbolList();
        for (String symbol : symbols) {
            for(String window : windows ){
                File dir = new File(fileMap.get(window));
                if(!dir.exists())
                    dir.mkdirs();
                String period=periodMap.get(window);
                symbol=symbol.replace("\"","");
                if(symbol.equals("Symbol"))
                    continue;
                String path = fileMap.get(window) + symbol + ".txt";
                System.out.println(path);
                String linkStr = String.format("http://chart.finance.yahoo.com/table.csv?s=%s&%s&g=%s&ignore=.csv",symbol, period, window);
                System.out.println(linkStr);
                URL link = new URL(linkStr);
                try {
                    FileUtils.copyURLToFile(link, new File(path));
                }catch(Exception err){
                    System.out.println(err.getMessage());
                }
            }
        }
    }
}
