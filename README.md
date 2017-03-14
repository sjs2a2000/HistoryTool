# HistoryDownloadTool
creates directories on source system: <br>
c:/prices/daily, c:/prices/weekly, c:/prices/monthly <br>
downloads from nyse, amex, nasdaq  <br>
symbols downloaded to c:/prices/stocksdb using the exchange list and link http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=%s&render=download",exch creating symbol list  <br>

Uses the period of window to control how far back we go.  <br>
To run:
 ~/HistoryDownloadTool/out/artifacts/symbolsdownload_jar (master)
$ java -jar symbolsdownload.jar







