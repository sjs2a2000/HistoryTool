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
    String[] othersources = new String[]{"nasdaqlisted.txt", "otherlisted.txt", "IndexSymbols.csv"};

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
        System.out.println("DownloadSymbols");
        File dir = new File("c:/prices/stocksdb"); //this throws error if it does not exist??
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
        try {
            File indexes = new File("./data/IndexSymbols.csv");
            System.out.println("orig path = " + indexes.getAbsolutePath());
            File indexesnew = new File("c:/prices/stocksdb/IndexSymbols.csv");
            System.out.println("new path = " + indexesnew.getAbsolutePath());
            if( indexesnew.exists()) {
                System.out.println("index file already exists in stocksdb: " + indexesnew.getAbsolutePath() + " skipping copy");
            }else if(indexes.exists()) {
                System.out.println("index file exists, need to copy");
                FileUtils.copyFile(indexes, indexesnew);
            }else{
                System.out.println("no index file exists, skipping copy orig:"  + indexes.getAbsolutePath() + " , new:" + indexesnew.getAbsolutePath());
            }
        }catch(Exception err){
            System.out.println("Failed to copy file IndexSymbols.csv " + err.getMessage());
        }
    }

    ArrayList<String> getSymbolList() throws FileNotFoundException {
        Set<String> symbols = new HashSet<String>();
        for(String exch : exchanges ) {
            File f = new File(String.format("c:/prices/stocksdb/%s.txt",exch));
            Scanner scanner = new Scanner(f);
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
            File f = new File(String.format("c:/prices/stocksdb/%s",other));
            Scanner scanner = new Scanner(f);
            String[] fields;
            if(other.contains("Index") && scanner.hasNextLine())
                scanner.nextLine();
            while (scanner.hasNextLine())
            {
                String line = scanner.nextLine();
                if(other.contains("Index"))
                    fields = line.split(",");
                else
                    fields = line.split("|");
                symbols.add(fields[0].replaceAll("\\s", ""));
            }
            scanner.close();
        }
        ArrayList<String> temp = new ArrayList<String>();
        temp.addAll(symbols);
        Collections.sort(temp);
        System.out.print("Number of Symbols:" + temp.size() + "\n" + temp.toString());
        return temp;
    }

    void DownloadSymbolPrice(String symbol, HashMap<String, String> mapping)
    {
        try {
            for (String window : windows) {
                File dir = new File(fileMap.get(window));
                if (!dir.exists())
                    dir.mkdirs();
                String period = periodMap.get(window);
                symbol = symbol.replace("\"", "");
                String filesym = symbol;
                if(mapping.containsKey(symbol))
                    filesym = mapping.get(symbol);
                String path = fileMap.get(window) + filesym + ".txt";
                String linkSym = symbol;
                if(symbol.contains("^") && !mapping.containsKey(symbol))
                    linkSym = symbol.replace("^","-P");
                if(symbol.contains(".") &&  !mapping.containsKey(symbol))
                {
                    linkSym = linkSym.replaceFirst("\\.","-");
                    linkSym = linkSym.replaceFirst("\\.","");
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
        //load the index file and create mapping of symbol to file
        HashMap<String,String> mapping = new HashMap<String, String>();
        try {
            File f = new File(String.format("c:/prices/stocksdb/IndexSymbols.csv"));
            Scanner scanner = new Scanner(f);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] fields = line.split(",");
                mapping.put(fields[0], fields[2]);
                //symbols.add(fields[0].replaceAll("\\s", ""));
            }
            scanner.close();
        }catch(Exception err){
            System.out.println("failed to load indexSymbols file");
        }
        //ExecutorService executor = Executors.newFixedThreadPool(5);
        try {
            System.out.println("DownloadPrices");
            int index = 0;
            ArrayList<String> symbols = getSymbolList();
            for (String symbol : symbols) {
                //executor.execute(() -> {
                //    DownloadSymbolPrice(symbol);
                //});
                DownloadSymbolPrice(symbol, mapping);
                ++index;
                System.out.println("symbol:" + symbol + " done, symbol count:" + symbols.size() + " number complete:" + index);
            }
        }catch(Exception err) {
            System.out.println("download prices failed" + err.toString());
        }
        //executor.shutdown();
        //while (!executor.isTerminated()) {
        //}
        System.out.println("Finished all threads");
    }
}
