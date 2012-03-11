#include<cstdio>
#include<sqlite3.h>

int main() {
  sqlite3 * dbh;  
  char * zErrMsg = 0;  

  int rc = sqlite3_open("/mnt/ramdisk/cboe_weeklies.db",&dbh);

  sqlite3_enable_load_extension(dbh,1);

  rc = sqlite3_load_extension(dbh,"libsqlitefunctions.so",NULL,&zErrMsg);
  if( rc!=SQLITE_OK ) {
    puts(zErrMsg);
    sqlite3_free(zErrMsg);
  }

  sqlite3_exec(dbh,"DROP TABLE cboe_weeklies_ordered",NULL,NULL,NULL);
  
  sqlite3_exec(dbh,"CREATE TABLE cboe_weeklies_ordered ( eod_date TEXT, ticker_symbol TEXT, adj_close REAL )",NULL,NULL,NULL);
  
  sqlite3_exec(dbh,"INSERT INTO cboe_weeklies_ordered SELECT eod_date, ticker_symbol, adj_close FROM cboe_weeklies_eod ORDER BY ticker_symbol ASC, eod_date ASC",NULL,NULL,NULL); 

  sqlite3_exec(dbh,"CREATE INDEX cboe_weeklies_ordered_ticker_idx ON cboe_weeklies_ordered ( ticker_symbol )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"DROP TABLE cboe_weeklies_returns",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE TABLE cboe_weeklies_returns ( idx INTEGER, eod_date TEXT, ticker_symbol TEXT, adj_close REAL, day_change REAL )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"INSERT INTO cboe_weeklies_returns SELECT t1.rowid, t1.eod_date, t1.ticker_symbol, t1.adj_close, (t1.adj_close-t2.adj_close)/t2.adj_close FROM cboe_weeklies_ordered t1, cboe_weeklies_ordered t2 WHERE (t2.ticker_symbol=t1.ticker_symbol AND t2.rowid=(t1.rowid-1))",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE INDEX cboe_weeklies_returns_date_idx ON cboe_weeklies_returns ( eod_date )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE INDEX cboe_weeklies_returns_ticker_idx ON cboe_weeklies_returns ( ticker_symbol )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"DROP TABLE cboe_weeklies_sharpe",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE TABLE cboe_weeklies_sharpe ( ticker_symbol TEXT, avg_return REAL, sharpe_ratio REAL )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"INSERT INTO cboe_weeklies_sharpe SELECT ticker_symbol, AVG(day_change), AVG(day_change)/SQRT(SUM(day_change*day_change)/COUNT(*)-(SUM(day_change)/COUNT(*))*(SUM(day_change)/COUNT(*))) FROM cboe_weeklies_returns GROUP BY ticker_symbol",NULL,NULL,NULL);

  sqlite3_exec(dbh,"DROP TABLE cboe_weeklies_revs_ordered",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE TABLE cboe_weeklies_revs_ordered ( ticker_symbol TEXT, period TEXT, revenue REAL )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"INSERT INTO cboe_weeklies_revs_ordered SELECT * FROM cboe_weeklies_revenue ORDER BY ticker_symbol ASC, period ASC",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE INDEX cboe_weeklies_revso_ticker_idx ON cboe_weeklies_revs_ordered ( ticker_symbol )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"DROP TABLE cboe_revenue_growth",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE TABLE cboe_revenue_growth ( idx INTEGER, ticker_symbol TEXT, period TEXT, revenue REAL, rev_growth REAL )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"INSERT INTO cboe_revenue_growth SELECT t1.rowid, t1.ticker_symbol, t1.period, t1.revenue, (t1.revenue-t2.revenue)/t2.revenue FROM cboe_weeklies_revs_ordered t1, cboe_weeklies_revs_ordered t2 WHERE (t2.ticker_symbol=t1.ticker_symbol AND t2.rowid=(t1.rowid-1) AND t2.revenue>0)",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE INDEX cboe_revg_ticker_idx ON cboe_revenue_growth ( ticker_symbol )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"DROP TABLE cboe_sharpe_revg",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE TABLE cboe_sharpe_revg ( ticker_symbol TEXT, avg_rev_growth REAL, sharpe_rev_growth REAL )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"INSERT INTO cboe_sharpe_revg SELECT ticker_symbol, AVG(rev_growth), AVG(rev_growth)/SQRT(SUM(rev_growth*rev_growth)/COUNT(*)-(SUM(rev_growth)/COUNT(rev_growth))*(SUM(rev_growth)/COUNT(*))) FROM cboe_revenue_growth GROUP BY ticker_symbol",NULL,NULL,NULL);

  sqlite3_exec(dbh,"DROP TABLE cboe_sharpe_ratios",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE TABLE cboe_sharpe_ratios ( ticker_symbol TEXT, sharpe_ratio REAL, sharpe_rev_growth REAL )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"INSERT INTO cboe_sharpe_ratios SELECT t1.ticker_symbol, t1.sharpe_ratio, t2.sharpe_rev_growth FROM cboe_weeklies_sharpe t1, cboe_sharpe_revg t2 WHERE (t2.ticker_symbol=t1.ticker_symbol AND t1.sharpe_ratio>0.0 AND t2.sharpe_rev_growth>0.0)",NULL,NULL,NULL);

  sqlite3_exec(dbh,"DROP TABLE sharpe_ratio_ranks",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE TABLE sharpe_ratio_ranks ( ticker_symbol TEXT )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"INSERT INTO sharpe_ratio_ranks SELECT ticker_symbol FROM cboe_sharpe_ratios ORDER BY sharpe_ratio DESC",NULL,NULL,NULL);

  sqlite3_exec(dbh,"DROP TABLE sharpe_revg_ranks",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE TABLE sharpe_revg_ranks ( ticker_symbol TEXT )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"INSERT INTO sharpe_revg_ranks SELECT ticker_symbol FROM cboe_sharpe_ratios ORDER BY sharpe_rev_growth DESC",NULL,NULL,NULL);

  sqlite3_exec(dbh,"DROP TABLE cboe_fcf_ordered",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE TABLE cboe_fcf_ordered ( ticker_symbol TEXT, period TEXT, free_cash_flow REAL )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"INSERT INTO cboe_fcf_ordered ( ticker_symbol, period, free_cash_flow ) SELECT * FROM cboe_weeklies_fcf ORDER BY ticker_symbol ASC, period ASC",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE INDEX cboe_fcfo_ticker_idx ON cboe_fcf_ordered ( ticker_symbol )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"DROP TABLE cboe_fcf_growth",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE TABLE cboe_fcf_growth ( idx INTEGER, ticker_symbol TEXT, period TEXT, free_cash_flow REAL, fcf_growth REAL )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"INSERT INTO cboe_fcf_growth SELECT t1.rowid, t1.ticker_symbol, t1.period, t1.free_cash_flow, (t1.free_cash_flow-t2.free_cash_flow)/t2.free_cash_flow FROM cboe_fcf_ordered t1, cboe_fcf_ordered t2 WHERE (t2.ticker_symbol=t1.ticker_symbol AND t2.rowid=(t1.rowid-1) AND t2.free_cash_flow>0)",NULL,NULL,NULL);

  sqlite3_exec(dbh,"DROP TABLE cboe_sharpe_fcfg",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE TABLE cboe_sharpe_fcfg ( ticker_symbol TEXT, avg_fcfg REAL, sharpe_fcfg REAL )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"INSERT INTO cboe_sharpe_fcfg SELECT ticker_symbol, AVG(fcf_growth), AVG(fcf_growth)/SQRT(AVG(fcf_growth*fcf_growth)-AVG(fcf_growth)*AVG(fcf_growth)) FROM cboe_fcf_growth WHERE period!='TTM' GROUP BY ticker_symbol HAVING COUNT(*)>1",NULL,NULL,NULL);

  sqlite3_exec(dbh,"DROP TABLE sharpe_fcf_ranks",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE TABLE sharpe_fcf_ranks ( ticker_symbol TEXT )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"INSERT INTO sharpe_fcf_ranks SELECT ticker_symbol FROM cboe_sharpe_fcfg ORDER BY sharpe_fcfg DESC",NULL,NULL,NULL);

  sqlite3_exec(dbh,"DROP TABLE price_to_deps_ranks",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE TABLE price_to_deps_ranks ( ticker_symbol TEXT )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"INSERT INTO price_to_deps_ranks SELECT ticker_symbol FROM price_to_deps ORDER BY price_to_deps ASC",NULL,NULL,NULL);

  sqlite3_exec(dbh,"DROP TABLE cboe_norm_ranks",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE TABLE cboe_norm_ranks ( ticker_symbol TEXT, sharpe_ratio_rank INTEGER, sharpe_revg_rank INTEGER, sharpe_fcfg_rank INTEGER, price_to_deps_rank INTEGER, norm_rank REAL )",NULL,NULL,NULL);

  sqlite3_exec(dbh,"INSERT INTO cboe_norm_ranks SELECT t1.ticker_symbol, t1.rowid, t2.rowid, t3.rowid, t6.rowid, t1.rowid*t1.rowid + t2.rowid*t2.rowid + t3.rowid*t3.rowid + t6.rowid*t6.rowid FROM sharpe_ratio_ranks t1, sharpe_revg_ranks t2, sharpe_fcf_ranks t3, price_to_deps_ranks t6 WHERE (t2.ticker_symbol=t1.ticker_symbol AND t3.ticker_symbol=t1.ticker_symbol AND t6.ticker_symbol=t1.ticker_symbol)",NULL,NULL,NULL);

  sqlite3_exec(dbh,"CREATE INDEX cboe_nr_nr_idx ON cboe_norm_ranks ( norm_rank )",NULL,NULL,NULL);
  
  sqlite3_close(dbh);
}
