import datetime
from unittest import signals
import backtrader as bt
import backtrader.feeds as btfeeds
import backtrader.indicators as btind
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo
from numpy import size
import pandas as pd

cash                = 100000
commission          = 0.0004
plot                = True
from_time           = datetime.datetime.strptime('2022-06-15 00:00:00', '%Y-%m-%d %H:%M:%S')                                      
to_time             = datetime.datetime.strptime('2022-07-21 00:00:00', '%Y-%m-%d %H:%M:%S')

class GenericCSV_XARF(bt.feeds.GenericCSVData):
        lines=('signal',)
        params =(   ('timestamp'    ,  0),
                    ('open'         ,  1),
                    ('high'         ,  2),
                    ('low'          ,  3),
                    ('close'        ,  4),
                    ('volume'       , -1),
                    ('openinterest' , -1),
                    ('signal'     ,  5),
                    )


class PairTradingStrategy(bt.Strategy):
    params = dict(
                    printout         =   True,
                    portfolio_value  =  90000,
                    confidence_level = 0.99
    )

    def log(self, txt, dt=None):
        if self.p.printout:
            dt = dt or self.data.datetime[0]
            dt = bt.num2date(dt)
            print('%s, %s' % (dt.isoformat(), txt))

    def notify_order(self, order):
        if order.status in [bt.Order.Submitted, bt.Order.Accepted]:
            return  # Await further notifications

        if order.status == order.Completed:
            if order.isbuy():
                buytxt = 'BUY COMPLETE, %.2f' % order.executed.price
                self.log(buytxt, order.executed.dt)
            if not order.isbuy():
                selltxt = 'SELL COMPLETE, %.2f' % order.executed.price
                self.log(selltxt, order.executed.dt)

        elif order.status in [order.Expired, order.Canceled, order.Margin]:
            self.log('%s ,' % order.Status[order.status])
            pass  # Simply log

        # Allow new orders
        self.orderid = None

    def notify_trade(self, trade):
        if trade.isclosed:

            txt = 'TRADE PNL        Gross {}, Net {}'.format(
                                    round(trade.pnl,2),
                                    round(trade.pnlcomm,2))

            self.log(txt)

    def __init__(self):
        self.signal1         = self.datas[0].signal
        self.signal2         = self.datas[1].signal
        self.portfolio_value = self.p.portfolio_value
        self.cl               = self.p.confidence_level
        self.status          = 0

    def next(self):

        if not self.position:

            if (self.signal1 < (1-self.cl)) and (self.signal2 > self.cl):
                print(f'prob s1: {self.signal1} prob s2:{self.signal2}')

                # Placing the order
                self.log('BUY CREATE %s, price = %.4f' % ("BTC", self.data0.close[0]))
                self.buy(data=self.data0, size=(self.p.portfolio_value / 2) / self.data0.close[0])  # Place an order for selling x + qty1 shares
                self.log('SELL CREATE %s, price = %.4f' % ("ETH", self.data1.close[0]))
                self.sell(data=self.data1, size=(self.p.portfolio_value / 2) / self.data1.close[0])  # Place an order for buying y + qty2 shares
                self.status = 1

            elif (self.signal1 > self.cl) and (self.signal2 < (1-self.cl)):

                # Placing the order
                self.log('SELL CREATE %s, price = %.4f' % ("BTC", self.data0.close[0]))
                self.sell(data=self.data0, size=(self.p.portfolio_value / 2) / self.data0.close[0])  # Place an order for buying y + qty2 shares
                self.log('BUY CREATE %s, price = %.4f' % ("ETH", self.data1.close[0]))
                self.buy(data=self.data1, size=(self.p.portfolio_value / 2) / self.data1.close[0])  # Place an order for selling x + qty1 shares
                self.status = 2

            else:
                    pass

        

            
            
        
        elif self.status == 1:
            if (self.signal1 > 0.3) or (self.signal2 < 0.7):
                self.close(self.data0)
                self.close(self.data1)
                self.status = 0
            else:
                pass
        elif self.status == 2:
            if (self.signal1 < 0.7) or (self.signal2 > 0.3):
                self.close(self.data0)
                self.close(self.data1)
                self.status = 0
            else:
                pass


        

    def stop(self):
        # print('==================================================')
        # print('Starting Value : %.2f' % self.broker.startingcash)
        # print('Ending   Value : %.2f' % self.broker.getvalue())
        print('==================================================')
        # print(self.broker.getvalue() - self.broker.startingcash)


def runstrategy():

    # Create a cerebro
    cerebro = bt.Cerebro()

    # Get the dates from the args
    # fromdate = datetime.datetime.strptime(fromdate, '%Y-%m-%d')
    # todate   = datetime.datetime.strptime(todate, '%Y-%m-%d')

    # Create the 1st data
    data0 = GenericCSV_XARF(
                            dataname    =  '/home/soltani/pair_trading/project_pair/pair1.csv',
                            dtformat    = 2,
                            # fromdate    = from_time,
                            # todate      = to_time,
                            timeframe   = bt.TimeFrame.Minutes,
                            compression = 60,)

    # Add the 1st data to cerebro
    cerebro.adddata(data0)

    # Create the 2nd data
    data1 = GenericCSV_XARF(
                            dataname    = '/home/soltani/pair_trading/project_pair/pair2.csv',
                            dtformat    = 2,
                            # fromdate    = from_time,
                            # todate      = to_time,
                            timeframe   = bt.TimeFrame.Minutes,
                            compression = 60,)

    # Add the 2nd data to cerebro
    cerebro.adddata(data1)

    # Add the strategy
    cerebro.addstrategy(PairTradingStrategy)
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trades")

    # Add the commission - only stocks like a for each operation
    cerebro.broker.setcash(cash)

    # Add the commission - only stocks like a for each operation
    cerebro.broker.setcommission(commission)

    # And run it
    results = cerebro.run()
    print(results[0].analyzers.getbyname("trades").get_analysis()['pnl']['net']['total'])

    # Plot if requested
    if plot:
        # cerebro.plot(numfigs = 2, volume=False, zdown=False ,filename = '/home/soltani/pair_trading/chart.html')
        b = Bokeh(  style               = 'bar', 
                    plot_mode           = 'single', 
                    scheme              = Tradimo(), 
                    legend_text_color   = '#000000', 
                    filename            = '/home/soltani/pair_trading/html_data/chart.html',
                    volume              = False)
        cerebro.plot(b)




if __name__ == '__main__':

    runstrategy()

            
