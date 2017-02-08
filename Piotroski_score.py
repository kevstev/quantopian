"""
    Creating an algorithm based off the Piotroski Score index which is based off of a score (0-9)
    Each of the following marks (-) satisfied means one point. And in the end we'll long the stocks with a score of >= 8

    Profitability 
    - Positive ROA
    - Positive Operating Cash Flow
    - Higher ROA in current year versus last year
    - Cash flow from operations > ROA of current year

    Leverage
    - Current ratio of long term debt < last year's ratio of long term debt
    - Current year's current_ratio > last year's current_ratio
    - No new shares issued this year

    Operating Efficiency
    - Higher gross margin compared to previous year
    - Higher asset turnover ratio compared to previous year


    This algorithm demonstrates how to grasp historical fundamental data by storing it in a Pandas Panel similar to how the Quantopian 'data' is currently structured
"""

import pytz
from datetime import datetime, timedelta

import pandas as pd
    
"""
    Initialize and Handle Data
"""
    
def initialize(context):
    context.prime = False
    #: context.days holds the number of days that we've had this algorithm
    context.days = 99
    
    #: context.fundamental_dict holds the date:dictionary reference that we need
    context.fundamental_dict = {}
    
    #: context.fundamental_data holds the pandas Panel that's derived from the fundamental_dict
    context.fundamental_data = None

def before_trading_start(context): 
    """
        Called before the start of each trading day (handle_data) and updates our universe with the securities and values found from fetch_fundamentals
    """
    
    #: Reference fundamentals in a shorter variable
    f = fundamentals
    
    #: Query for the data that we need from fundamentals
    fundamental_df = get_fundamentals(query(
                                    f.valuation.market_cap,
                                    f.operation_ratios.roa,
                                    f.cash_flow_statement.operating_cash_flow,
                                    f.cash_flow_statement.cash_flow_from_continuing_operating_activities,
                                    f.operation_ratios.long_term_debt_equity_ratio,
                                    f.operation_ratios.current_ratio,
                                    f.valuation.shares_outstanding,
                                    f.operation_ratios.gross_margin,
                                    f.operation_ratios.assets_turnover,
            f.valuation_ratios.ev_to_ebitda,
                            )

.filter(fundamentals.valuation.market_cap > 1.5e9)
.filter(fundamentals.valuation_ratios.ev_to_ebitda > 0)
.order_by(fundamentals.valuation_ratios.ev_to_ebitda.asc())
                             .limit(200)
                             )
    
    #: Set our fundamentals into a context variable
    context.fundamental_df = fundamental_df
    
    #: Update our universe with the values
    update_universe(fundamental_df.columns.values)  
    
def handle_data(context, data):
    if context.prime == False:
        order_target_percent(symbol('SPY'),1)
        context.prime = True
        
    #: Only run every 25 trading days
    if context.days % 25 == 0:
        
        #: Insert a new dataframe into our dictionary 
        context.fundamental_dict[get_datetime()] = context.fundamental_df
        
        #: If it's greater than the first trading day
        if context.days > 0:
            context.fundamental_data = pd.Panel(context.fundamental_dict)
            scores = get_piotroski_scores(context.fundamental_data, get_datetime())
            
            #: Only rebalance when we have enough data
            if scores != None:
                rebalance(context, data, scores)
    
    #: Log our current positions
    if (context.days - 1) % 25 == 0:
        
        #: Portfolio position string
        portfolio_string = "Current positions: "
        
        #: Don't log if we have no positions
        if len(context.portfolio.positions) != 0:
            
            #: Add our current positions to a string
            for pos in context.portfolio.positions:
                portfolio_string += "Symbol: %s and Amount: %s, " % (pos.symbol, context.portfolio.positions[pos].amount)
        
            #: Log all our portfolios
            log.info(portfolio_string)
    
    
    context.days += 1
            
"""
    Defining our rebalance method
"""

def rebalance(context, data, scores):
    """
        This method takes in the scores found by get_piotroski_scores and orders our portfolio accordingly
    """
    
    #: Find which stocks we need to long and which ones we need to short
    num_long = [stock for stock in scores if scores[stock] >= 9]
    num_short = [stock for stock in scores if scores[stock] <= 2]
    
    #: Stocks to long
    for stock in num_long:
        if stock in data:
            log.info("Going long on stock %s with score %s" % (stock.symbol, scores[stock]))
            order_target_percent(stock, 1.0/len(num_long))
    
    # #: Stocks to short
    # for stock in num_short:
    #     if stock in data:
    #         log.info("Going short on stock %s with score %s" % (stock.symbol, scores[stock]))
    #         order_target_percent(stock, -1.0/len(num_short))
    
    #: Exit any positions we might have
    for stock in context.portfolio.positions:
        if stock in data and stock not in num_long and stock not in num_short:
            log.info("Exiting our positions on %s" % (stock.symbol))
            order_target_percent(stock, 0)
    
    record(number_long=len(num_long))
    # record(number_short=len(num_short))
    
"""
    Defining our methods for the piotroski score
"""

def get_piotroski_scores(fundamental_data, current_date):
    """
        This method finds the dataframe that contains the data for the time period we want
        and finds the total Piotroski score for those dates
    """
    all_scores = {}
    all_dates = fundamental_data.items
    
    utc = pytz.UTC
    last_year = utc.localize(datetime(year=current_date.year - 1, month = current_date.month, day = current_date.day))
    
    #: If one year hasn't passed just return None
    if last_year < min(all_dates):
        return None
    
    #: Figure out which date to use
    for i, date in enumerate(all_dates):
        if i == len(all_dates) - 1:
            continue
        if last_year > date and last_year < all_dates[i + 1]:
            break
        elif last_year == date:
            break
        
    #: This is pretty robust so just set last_year to whatever date currently is when you broke
    #: or ended the for loop
    last_year = date
    old_data = fundamental_data[last_year]
    current_data = fundamental_data[current_date]
    
    #: Find the score for each security
    for stock in current_data:
        profit = profit_logic(current_data, old_data, stock)
        leverage = leverage_logic(current_data, old_data, stock)
        operating = operating_logic(current_data, old_data, stock)
        total_score = profit + leverage + operating
        all_scores[stock] = total_score
        
    return all_scores

def profit_logic(current_data, old_data, sid):
    """
        Define our profitability logic here
    """
    
    #: Positive ROA
    positive_roa = current_data[sid]['roa'] > 0
    #: Positive Operating Cash Flow
    positive_ocf = current_data[sid]['operating_cash_flow'] > 0
    #: Current ROA > Last Year ROA
    current_last_roa = current_data[sid]['roa'] > old_data[sid]['roa']
    #: Cash flow from operations > ROA
    cash_flow_roa = current_data[sid]['cash_flow_from_continuing_operating_activities'] > current_data[sid]['roa']
    
    return int(positive_roa) + int(positive_ocf) + int(current_last_roa)+ int(cash_flow_roa)
    
def leverage_logic(current_data, old_data, sid):
    """
        Define our leverage logic here 
    """
    
    #: Current ratio of long-term debt < last year's ratio of long-term debt
    long_term_debt = current_data[sid]['long_term_debt_equity_ratio'] > old_data[sid]['long_term_debt_equity_ratio']
    #: Current year's current_ratio > last year's current_ratio
    current_ratio = current_data[sid]['current_ratio'] > old_data[sid]['current_ratio']
    #: No new shares
    new_shares = current_data[sid]['shares_outstanding'] <= old_data[sid]['shares_outstanding']
    
    return int(long_term_debt) + 2*int(current_ratio) + 2*int(new_shares)
    
def operating_logic(current_data, old_data, sid):
    """
        Define our operating efficiency logic here
    """
    
    #: Higher gross margin compared to previous year
    gross_margin = current_data[sid]['gross_margin'] > old_data[sid]['gross_margin']
    #: Higher asset turnover ratio compared to previous year
    asset_turnover = current_data[sid]['assets_turnover'] > old_data[sid]['assets_turnover']
    
    return int(gross_margin) + 2*int(asset_turnover)


  
    
    
