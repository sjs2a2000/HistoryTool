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
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.net.*;
import java.io.*;
import java.util.regex.*;

/*import org.apache.commons.httpclient.Cookie;
import org.apache.commons.httpclient.HttpState;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;*/
import org.apache.commons.io.input.ReversedLinesFileReader;



@SuppressWarnings("deprecation")
public class DownloadHistory {
    HashMap<String, String> fileMap = new HashMap<String, String>();
    HashMap<String, String> periodMap = new HashMap<>();
    String[] windows = new String[]{"d", "w", "m"};
    String[] exchanges = new String[]{"nyse", "amex", "nasdaq"};
    String[] othersources = new String[]{"nasdaqlisted.txt", "otherlisted.txt", "IndexSymbols.csv"};
    String _expectedDt="";
    ArrayList<String> downloadFailed = new ArrayList<>();
    {
        fileMap.put("d", "c:/prices/daily/");
        fileMap.put("w", "c:/prices/weekly/");
        fileMap.put("m", "c:/prices/monthly/");
    }

    DownloadHistory(int dPeriod, int wPeriod, int mPeriod) {
        //period1=1493202557&period2=1495794557&interval=1d
        java.util.Date date1 = new Date();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        long start = date1.toInstant().getEpochSecond();
        Calendar cal = Calendar.getInstance();
        cal.setTime(date1);
        cal.add(Calendar.DAY_OF_WEEK, -1);
        if(cal.getTime().getDay()==0)
            cal.add(Calendar.DAY_OF_WEEK, -2);
        else if(cal.getTime().getDay()==6)
            cal.add(Calendar.DAY_OF_WEEK, -1);
        _expectedDt= df.format(cal.getTime());
        cal.add(Calendar.YEAR, -dPeriod);
        long endDay = cal.getTime().toInstant().getEpochSecond();
        cal = Calendar.getInstance();
        cal.setTime(date1);
        cal.add(Calendar.YEAR, -wPeriod);
        long endWeek = cal.getTime().toInstant().getEpochSecond();
        cal = Calendar.getInstance();
        cal.setTime(date1);
        cal.add(Calendar.YEAR, -mPeriod);
        long endMonth = cal.getTime().toInstant().getEpochSecond();
        //https://query1.finance.yahoo.com/v7/finance/download/AAPL?period1=1493206674&period2=1495798674&interval=1d&events=history&crumb=NxFZ0nuSN/G
        periodMap.put("d", String.format("period1=%d&period2=%d&interval=1d&events=history", endDay, start));
        System.out.println(String.format(String.format("period1=%d&period2=%d&interval=1d&events=history", endDay, start)));
        periodMap.put("w", String.format("period1=%d&period2=%d&interval=1wk&events=history", endWeek, start));
        System.out.println(String.format(String.format("period1=%d&period2=%d&interval=1wk&events=history", endWeek, start)));
        periodMap.put("m", String.format("period1=%d&period2=%d&interval=1mo&events=history", endMonth, start));
        System.out.println(String.format(String.format("period1=%d&period2=%d&interval=1mo&events=history", endMonth, start)));
    }

    /*
    allows the modification of the period for the data via the commandline
     */
    Tuple<String, String> GetCookieAndCrumb() throws IOException
    {
        OutputStream out = null;
        URLConnection conn = null;
        InputStream in = null;
        try{
            URL url = new URL("https://finance.yahoo.com/quote/SPY?p=SPY");
            URLConnection con = url.openConnection();
            //con.setConnectTimeout(5000); //milliseconds
            String cook = "";
            Map<String, List<String>> headerFields = con.getHeaderFields();
            for (Map.Entry<String, List<String>> entry : headerFields.entrySet()) {
                if (entry.getKey() == null
                        || !entry.getKey().toLowerCase().equals("set-cookie"))
                    continue;
                for (String s : entry.getValue()) {
                    System.out.println(s);
                    cook=s;
                }
            }
            String crumb = null;
            InputStream inStream = con.getInputStream();
            InputStreamReader irdr = new InputStreamReader(inStream);
            BufferedReader rsv = new BufferedReader(irdr);

            Pattern crumbPattern = Pattern.compile(".*\"CrumbStore\":\\{\"crumb\":\"([^\"]+)\"\\}.*");

            String line = null;
            while (crumb == null && (line = rsv.readLine()) != null) {
                Matcher matcher = crumbPattern.matcher(line);
                if (matcher.matches())
                    crumb = matcher.group(1);
            }
            rsv.close();
            System.out.println(crumb); // Display the string.
            // quoteUrl = "https://query1.finance.yahoo.com/v7/finance/download/IBM?period1=1493425217&period2=1496017217&interval=1d&events=history&crumb=" + crumb;
            //url = new URL(quoteUrl);
            //con = url.openConnection();
            //con.setRequestProperty("Cookie", cook);
            //con.connect();
            return new Tuple<>(cook, crumb);
        }catch(Exception err){
            System.out.println(err.toString());
            throw err;
        }

    }

    public static void main(String[] args) {
        //https://query1.finance.yahoo.com/v7/finance/download/A?period1=1493202557&period2=1495794557&interval=1d&events=history&crumb=NxFZ0nuSN/G
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date1 = new Date();
        System.out.println(dateFormat.format(date1));
        System.out.println(date1.toInstant().getEpochSecond());

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
            Tuple<String, String> t = dl.GetCookieAndCrumb();
            dl.DownloadSymbols();
            dl.DownloadPrices(t.a, t.b);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Date date2 = new Date();
        System.out.println(dateFormat.format(date2)); //2016/11/16 12:08:43
        long difference = (date2.getTime() - date1.getTime())/1000;
        System.out.println("process ran in " + difference + " seconds");
        System.out.println("Date errors for the following symbols expecteddate=" + dl._expectedDt);
        dl.downloadFailed.forEach(System.out::println);
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

    //be sure to delete file after working with it. filenamePrefix ~ "test_", file extension ~ ".jpg", include the "."
    public File downloadFile(String url, String filenamePrefix, String fileExtension, String path, String cookie, String dtexpected) throws Exception{
        //request setup...
        URLConnection request = null;
        request = new URL(url).openConnection();
        request.setRequestProperty("Cookie", cookie);
        InputStream in = request.getInputStream();
        File downloadedFile = File.createTempFile(filenamePrefix, fileExtension);
        FileOutputStream out = new FileOutputStream(downloadedFile);
        byte[] buffer = new byte[1024];
        int len = in.read(buffer);
        int count=0;
        while (len != -1) {
            out.write(buffer, 0, len);
            len = in.read(buffer);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
        in.close();
        out.close();
        @SuppressWarnings("depreciation")
        ReversedLinesFileReader fr; //noinspection deprecation
        //noinspection deprecation
        fr = new ReversedLinesFileReader(downloadedFile);
        File output = new File(path);
        PrintWriter printer = new PrintWriter(output);
        printer.write("Date,Open,High,Low,Close,ADate,Open,High,Low,Close,Adj Close,Volume\n");
        String ch;
        String Conversion="";
        int index =0;
        boolean first = true;
        try {
            do {
                ch = fr.readLine();
                if(ch!=null && !ch.contains("Date")) {
                    printer.write(ch + "\n");
                    if(first) {
                        String dt = ch.split(",")[0];
                        if(!dt.equals(dtexpected)) throw new Exception("expected date: " + dtexpected + " not available, date available: " + dt );
                        first=false;
                    }
                }
                index++;
            } while (ch != null);
            printer.flush();
        } catch(Exception err){
            System.out.println(err.getMessage());
            downloadFailed.add(url);
        }
        fr.close();
        return downloadedFile;
    }
    void DownloadSymbolPrice(String symbol, HashMap<String, String> mapping, String cookie, String crumb)
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
                String linkStr = String.format("https://query1.finance.yahoo.com/v7/finance/download/%s?%s&crumb=%s", linkSym, period, crumb);
                System.out.println("Symbol:"+symbol + " link: " + linkStr);
                URL link = new URL(linkStr);
                try {
                    File tempFile = downloadFile( linkStr, "TEST" + symbol, ".csv", path, cookie, _expectedDt);
                } catch (Exception err) {
                    System.out.println(err.getMessage());
                }
            }
        }catch(Exception err){
            System.out.println("download symbol:" + symbol + " price failed" + "\nERROR:" + err.toString());
        }
    }

    void DownloadPrices(String cookie, String crumb)
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
                DownloadSymbolPrice(symbol, mapping, cookie, crumb);
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
