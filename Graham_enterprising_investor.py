"""
This is a template algorithm on Quantopian for you to adapt and fill in.
"""
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import CustomFactor, Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume
from quantopian.pipeline.filters.morningstar import Q500US
from quantopian.pipeline.data import morningstar
from quantopian.pipeline import filters

from quantopian.pipeline.filters.morningstar import IsPrimaryShare
def get_tradeable_stocks():
    # Filter for primary share equities. IsPrimaryShare is a built-in filter.
    primary_share = IsPrimaryShare()

    # Equities listed as common stock (as opposed to, say, preferred stock).
    # 'ST00000001' indicates common stock.
    common_stock = morningstar.share_class_reference.security_type.latest.eq('ST00000001')

    # Non-depositary receipts. Recall that the ~ operator inverts filters,
    # turning Trues into Falses and vice versa
    not_depositary = ~morningstar.share_class_reference.is_depositary_receipt.latest

    # Equities not trading over-the-counter.
    not_otc = ~morningstar.share_class_reference.exchange_id.latest.startswith('OTC')

    # Not when-issued equities.
    not_wi = ~morningstar.share_class_reference.symbol.latest.endswith('.WI')

    # Equities without LP in their name, .matches does a match using a regular
    # expression
    not_lp_name = ~morningstar.company_reference.standard_name.latest.matches('.* L[. ]?P.?$')

    # Equities with a null value in the limited_partnership Morningstar
    # fundamental field.
    not_lp_balance_sheet = morningstar.balance_sheet.limited_partnership.latest.isnull()

    # Equities whose most recent Morningstar market cap is not null have
    # fundamental data and therefore are not ETFs.
    have_market_cap = morningstar.valuation.market_cap.latest.notnull()

    # Filter for stocks that pass all of our previous filters.
    tradeable_stocks = (
        primary_share
        & common_stock
        & not_depositary
        & not_otc
        & not_wi
        & not_lp_name
        & not_lp_balance_sheet
        & have_market_cap
    )
    return tradeable_stocks

import numpy
TRADING_DAYS_IN_YEAR=252
Q1=-1
Q2=0-(TRADING_DAYS_IN_YEAR/4)
Q3=0-(TRADING_DAYS_IN_YEAR/4)*2
Q4=0-(TRADING_DAYS_IN_YEAR/4)*3

class last_year_eps(CustomFactor):
    inputs = [morningstar.earnings_report.basic_eps]  
    window_length=TRADING_DAYS_IN_YEAR
    def compute(self, today, asset_ids, out, value):
        out[:] = value[Q1] + value[Q2] + value[Q3] + value[Q4] 
        
        
class one_years_eps(CustomFactor):
    inputs = [morningstar.earnings_report.basic_eps]  
    year=1
    
    window_length=TRADING_DAYS_IN_YEAR*year
    def compute(self, today, asset_ids, out, value):
        year=1
        offset=TRADING_DAYS_IN_YEAR*year*-1
        out[:] = value[offset-Q1] + value[offset-Q2] + value[offset-Q3] + value[offset-Q4]
        
class two_years_eps(CustomFactor):
    inputs = [morningstar.earnings_report.basic_eps]  
    year=2
    
    window_length=TRADING_DAYS_IN_YEAR*year
    def compute(self, today, asset_ids, out, value):
        year=2
        offset=TRADING_DAYS_IN_YEAR*year*-1
        out[:] = value[offset-Q1] + value[offset-Q2] + value[offset-Q3] + value[offset-Q4]
        
class three_years_eps(CustomFactor):
    inputs = [morningstar.earnings_report.basic_eps]  
    year=3
    offset=TRADING_DAYS_IN_YEAR*year*-1
    window_length=TRADING_DAYS_IN_YEAR*year
    def compute(self, today, asset_ids, out, value):
        out[:] = value[offset-Q1] + value[offset-Q2] + value[offset-Q3] + value[offset-Q4]

class four_years_eps(CustomFactor):
    inputs = [morningstar.earnings_report.basic_eps]  
    year=4
    offset=TRADING_DAYS_IN_YEAR*year*-1
    window_length=TRADING_DAYS_IN_YEAR*year
    def compute(self, today, asset_ids, out, value):
        out[:] = value[offset-Q1] + value[offset-Q2] + value[offset-Q3] + value[offset-Q4]

class five_years_eps(CustomFactor):
    inputs = [morningstar.earnings_report.basic_eps]  
    year=5
    offset=TRADING_DAYS_IN_YEAR*year*-1
    window_length=TRADING_DAYS_IN_YEAR*year
    def compute(self, today, asset_ids, out, value):
        out[:] = value[offset-Q1] + value[offset-Q2] + value[offset-Q3] + value[offset-Q4]
def initialize(context):
    """
    Called once at the start of the algorithm.
    """   
    log.info("initialzing")
    context.prime = False
    
    # Rebalance every day, 1 hour after market open.
    schedule_function(my_rebalance, date_rules.month_start(), time_rules.market_open(hours=1))
     
    # Record tracking variables at the end of each day.
    schedule_function(my_record_vars, date_rules.every_day(), time_rules.market_close())
     

    # Create our dynamic stock selector.
    attach_pipeline(make_pipeline(), 'my_pipeline')
         
def make_pipeline():
  
    log.info("making a pipeline")
   #ABX is showing up in the data here as +10 pe_ratio, though google finance
    #now- they just had positive earnings today (april 15)so maybe morningstar is
    # super fast to update
    
    positive_pe =morningstar.valuation_ratios.pe_ratio.latest > 0
    screen_criteria = morningstar.valuation_ratios.pe_ratio.latest.percentile_between(0, 25, mask=positive_pe)
    raw_pe = morningstar.valuation_ratios.pe_ratio.latest
    
    #current asset ratio > 1.5
    high_current_assets = morningstar.operation_ratios.current_ratio.latest > 1.5
    screen_criteria = screen_criteria & high_current_assets
    raw_current_ratio = morningstar.operation_ratios.current_ratio.latest
    
    #long term debt to working capital ratio is les than 110%
    long_term_debt = morningstar.balance_sheet.long_term_debt.latest
    working_capital = morningstar.balance_sheet.working_capital.latest
    debt_capital_ratio = long_term_debt/ working_capital
    debt_capital_screen = debt_capital_ratio <= 1.1
    screen_criteria = screen_criteria & debt_capital_screen

    dividend_stocks = morningstar.earnings_report.dividend_per_share.latest >0
    screen_criteria = screen_criteria & dividend_stocks
 
    screen_criteria = screen_criteria &get_tradeable_stocks()
    
    price_book = morningstar.valuation_ratios.pb_ratio.latest >1.2
    screen_criteria = screen_criteria & price_book
    
    #eps for each of hte last five years is positive
    last_year_eps_raw= last_year_eps(mask=screen_criteria)
    last_year_pos = last_year_eps_raw> 0
    
    screen_criteria = screen_criteria & last_year_pos
    
    one_year_eps_pos = one_years_eps(mask=screen_criteria) >0
    screen_criteria = screen_criteria & one_year_eps_pos
    
    two_year_eps_raw = two_years_eps(mask=screen_criteria) 
    two_year_eps_pos = two_year_eps_raw >0
    screen_criteria = screen_criteria & two_year_eps_pos
    
    eps_increasing = last_year_eps_raw > two_year_eps_raw
    
    screen_criteria = screen_criteria & eps_increasing
    
    return Pipeline(
        columns={
            
        },
        screen=screen_criteria
    )
 
def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    
     
def my_assign_weights(context, data):
    """
    Assign weights to securities that we want to order.
    """
    pass
 
def my_rebalance(context,data):
    """
        This method takes in the scores found by get_piotroski_scores and orders our portfolio accordingly
    """
    log.info("rebalancing...")
    context.output = pipeline_output('my_pipeline')
    log.info("retrieved pipeline output...")
  
    # These are the securities that we are interested in trading each day.
    context.security_list = context.output.index
    
    if context.prime == False:
        order_target_percent(symbol('SPY'),1) #hold SPY as a default 
    context.prime = True
    
    weight= 1.0/len(context.security_list)
   
    for stock in context.security_list:
        log.info("Buying %s" % (stock.symbol))
        order_target_percent(stock, weight)
        
     #: Exit any positions we might have
    for stock in context.portfolio.positions:
        if data.can_trade(stock) and stock not in context.security_list:
            log.info("Exiting our positions on %s" % (stock.symbol))
            order_target_percent(stock, 0)
    
 
def my_record_vars(context, data):
    """
    Plot variables at the end of each day.
    """
    pass
 
def handle_data(context,data):
    """
    Called every minute.
    """
    pass
