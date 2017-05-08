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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class DownloadHistory {
    HashMap<String, String> fileMap = new HashMap<String, String>();
    HashMap<String, String> periodMap = new HashMap<>();
    String[] windows = new String[]{"d", "w", "m"};
    String[] exchanges = new String[]{"nyse", "amex", "nasdaq"};
    String[] othersources = new String[]{"nasdaqlisted", "otherlisted"};

    {
        fileMap.put("d", "c:/prices/daily/");
        fileMap.put("w", "c:/prices/weekly/");
        fileMap.put("m", "c:/prices/monthly/");
    }

    DownloadHistory(int dPeriod, int wPeriod, int mPeriod) {
        java.util.Date date = new Date();
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int y = cal.get(Calendar.YEAR);
        int m = cal.get(Calendar.MONTH);
        int d = cal.get(Calendar.DAY_OF_MONTH);
        periodMap.put("d", String.format("a=%d&b=%d&c=%d&d=%d&e=%d&f=%d", m, d, y - dPeriod, m, d, y));
        System.out.println(String.format("a=%d&b=%d&c=%d&d=%d&e=%d&f=%d", m, d, y - dPeriod, m, d, y));
        periodMap.put("w", String.format("a=%d&b=%d&c=%d&d=%d&e=%d&f=%d", m, d, y - wPeriod, m, d, y));
        periodMap.put("m", String.format("a=%d&b=%d&c=%d&d=%d&e=%d&f=%d", m, d, y - mPeriod, m, d, y));
    }

    /*
    allows the modification of the period for the data via the commandline
     */
    public static void main(String[] args) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date1 = new Date();
        System.out.println(dateFormat.format(date1)); //2016/11/16 12:08:43

        int dperiod = 10, wperiod = 10, mperiod = 15;
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("d", "day", false, "period for day");
        options.addOption("m", "month", false, "period for month");
        options.addOption("y", "year", false, "period for year");
        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption('d')) {
                dperiod = Integer.parseInt(line.getOptionValue("d"));
            }
            if (line.hasOption('w')) {
                wperiod = Integer.parseInt(line.getOptionValue("w"));
            }
            if (line.hasOption('m')) {
                mperiod = Integer.parseInt(line.getOptionValue("m"));
            }
        } catch (ParseException exp) {
            System.out.println("Unexpected exception:" + exp.getMessage());
        }
        DownloadHistory dl = new DownloadHistory(dperiod, wperiod, mperiod);
        try {
            dl.DownloadSymbols();
            dl.DownloadPrices();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Date date2 = new Date();
        System.out.println(dateFormat.format(date2)); //2016/11/16 12:08:43
        long difference = (date2.getTime() - date1.getTime())/1000;
        System.out.println("process ran in " + difference + " seconds");
    }
    void DownloadSymbolFile(String exch)
    {
        System.out.println("processing "+exch);
        String filename = String.format("c:/prices/stocksdb/%s.txt", exch);
        String tempname = String.format("c:/prices/stocksdb/%s_temp.txt", exch);
        try {
            File temp = new File(tempname);
            URL link = new URL(String.format("http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=%s&render=download", exch));
            FileUtils.copyURLToFile(link, temp, 5000, 15000);
            if(temp.length()>30000)
            {
                System.out.println("writing " + filename);
                temp.renameTo(new File(filename));
            } else {
                System.out.println("file too small: " + temp.length() + " , use previous day file for " + filename);
                System.out.println("link timed out: " + link);
            }
        }catch(Exception err)
        {
            System.out.println("url download for symbols failed: " + filename + "\nERROR:" + err.toString());
        }
    }

    void DownloadSymbols() throws IOException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
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
            executor.execute( () -> { DownloadSymbolFile(exch); } );
            //DownloadSymbolFile(exch);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        System.out.println("Finished all threads");
        try
        {
            URL link = new URL("ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt");
            FileUtils.copyURLToFile(link, new File("c:/prices/stocksdb/nasdaqlisted.txt"));
            link = new URL("ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt");
            FileUtils.copyURLToFile(link, new File("c:/prices/stocksdb/otherlisted.txt"));
        }catch(Exception err){
            System.out.println("download of ftp failed" + "\nERROR:" + err.toString());
        }
    }

    ArrayList<String> getSymbolList() throws FileNotFoundException {
        Set<String> symbols = new HashSet<String>();
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

        for(String other : othersources ) {
            File f = new File(String.format("c:/prices/stocksdb/%s.txt",other));
            Scanner scanner = new Scanner(f);
            //scanner.useDelimiter("|");
            while (scanner.hasNextLine())
            {
                String line = scanner.nextLine();
                String[] fields = line.split("|");
                symbols.add(fields[0].replaceAll("\\s", ""));
            }
            scanner.close();
        }
        //convert symbols from set to arraylist
        ArrayList<String> temp = new ArrayList<String>();
        temp.addAll(symbols);
        Collections.sort(temp);
        System.out.print("Number of Symbols:" + temp.size() + "\n" + temp.toString());
        return temp;
    }

    void DownloadSymbolPrice(String symbol)
    {   try {
            for (String window : windows) {
                File dir = new File(fileMap.get(window));
                if (!dir.exists())
                    dir.mkdirs();
                String period = periodMap.get(window);
                symbol = symbol.replace("\"", "");
                //if (symbol.equals("Symbol"))
                //    continue;
                //if(!symbol.equals("DCO"))
                //    continue;

                System.out.println("Next: " + symbol);
                String path = fileMap.get(window) + symbol + ".txt";
                String linkSym = symbol;
                if(symbol.contains("^"))
                    linkSym = symbol.replace("^","-P");

                if(symbol.contains("."))
                {
                    linkSym = linkSym.replaceFirst("\\.","-");
                    linkSym = linkSym.replaceFirst("\\.","");
                    System.out.println("linksym="+linkSym);
                }
                String linkStr = String.format("https://chart.finance.yahoo.com/table.csv?s=%s&%s&g=%s&ignore=.csv", linkSym, period, window);
                System.out.println("Symbol:"+symbol + " link: " + linkStr);
                URL link = new URL(linkStr);
                try {
                    FileUtils.copyURLToFile(link, new File(path), 5000, 10000);
                } catch (Exception err) {
                    System.out.println(err.getMessage());
                }
            }
        }catch(Exception err){
            System.out.println("download symbol:" + symbol + " price failed" + "\nERROR:" + err.toString());
        }
    }

    void DownloadPrices()
    {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        try {
            System.out.println("DownloadPrices");
            int index = 0;
            ArrayList<String> symbols = getSymbolList();
            for (String symbol : symbols) {
                //executor.execute(() -> {
                //    DownloadSymbolPrice(symbol);
                //});
                DownloadSymbolPrice(symbol);
                ++index;
                System.out.println("symbol:" + symbol + " done, symbol count:" + symbols.size() + " number complete:" + index);
            }
        }catch(Exception err) {
            System.out.println("download prices failed" + err.toString());
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        System.out.println("Finished all threads");
    }
}
