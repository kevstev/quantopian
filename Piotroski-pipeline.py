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

class TTM_operating_cash_flow(CustomFactor):
    inputs = [morningstar.cash_flow_statement.operating_cash_flow]  
    window_length=TRADING_DAYS_IN_YEAR
    def compute(self, today, asset_ids, out, cash_flow):
        out[:] = cash_flow[Q1] + cash_flow[Q2] + cash_flow[Q3] + cash_flow[Q4] 

class TTM_roa(CustomFactor):
    inputs = [morningstar.operation_ratios.roa]  
    window_length=TRADING_DAYS_IN_YEAR
    def compute(self, today, asset_ids, out, value):
        out[:] = value[Q1] + value[Q2] + value[Q3] + value[Q4] 

class roa_YOY(CustomFactor):
    inputs= [morningstar.operation_ratios.roa]
    window_length=TRADING_DAYS_IN_YEAR*2
    def compute (self, today, asset_ids, out, roa):
        last_year_roa= roa[Q1] + roa[Q2] +roa[Q3] +roa[Q4] 
        one_year_offset=0-TRADING_DAYS_IN_YEAR
        prior_year_roa= roa[one_year_offset -Q1] +roa[one_year_offset -Q2] +roa[one_year_offset -Q3] + roa[one_year_offset -Q4]
        out[:] = last_year_roa - prior_year_roa

class long_term_debt_YOY(CustomFactor):
    inputs= [morningstar.operation_ratios.long_term_debt_equity_ratio]
    window_length=TRADING_DAYS_IN_YEAR*2
    def compute (self, today, asset_ids, out, value):
        last_year_value= value[Q1] + value[Q2] +value[Q3] +value[Q4] 
        one_year_offset=0-TRADING_DAYS_IN_YEAR
        prior_year_value= value[one_year_offset -Q1] +value[one_year_offset -Q2] +value[one_year_offset -Q3] + value[one_year_offset -Q4]
        out[:] = last_year_value - prior_year_value

class accrued_cash(CustomFactor):
    inputs=[morningstar.cash_flow_statement.cash_flow_from_continuing_operating_activities, morningstar.cash_flow_statement.net_income]
    window_length=1
    def compute(self, today, asset_ids, out, cash, income):
        out[:]=cash-income

class current_ratio_YOY(CustomFactor):
    inputs= [morningstar.operation_ratios.current_ratio]
    window_length=TRADING_DAYS_IN_YEAR*2
    def compute (self, today, asset_ids, out, value):
        last_year_value= value[Q1] + value[Q2] +value[Q3] +value[Q4] 
        one_year_offset=0-TRADING_DAYS_IN_YEAR
        prior_year_value= value[one_year_offset -Q1] +value[one_year_offset -Q2] +value[one_year_offset -Q3] + value[one_year_offset -Q4]
        out[:] = last_year_value - prior_year_value

class asset_turnover_YOY(CustomFactor):
    inputs= [morningstar.operation_ratios.assets_turnover]
    window_length=TRADING_DAYS_IN_YEAR*2
    def compute (self, today, asset_ids, out, value):
        last_year_value= value[Q1] + value[Q2] +value[Q3] +value[Q4] 
        one_year_offset=0-TRADING_DAYS_IN_YEAR
        prior_year_value= value[one_year_offset -Q1] +value[one_year_offset -Q2] +value[one_year_offset -Q3] + value[one_year_offset -Q4]
        out[:] = last_year_value - prior_year_value

class oustanding_shares_YOY(CustomFactor):
    inputs= [morningstar.valuation.shares_outstanding]
    window_length=TRADING_DAYS_IN_YEAR*2
    def compute (self, today, asset_ids, out, value):
        last_year_value= value[Q1] + value[Q2] +value[Q3] +value[Q4] 
        one_year_offset=0-TRADING_DAYS_IN_YEAR
        prior_year_value= value[one_year_offset -Q1] +value[one_year_offset -Q2] +value[one_year_offset -Q3] + value[one_year_offset -Q4]
        out[:] = last_year_value - prior_year_value

class gross_margin_YOY(CustomFactor):
    inputs= [morningstar.operation_ratios.gross_margin]
    window_length=TRADING_DAYS_IN_YEAR*2
    def compute (self, today, asset_ids, out, value):
        last_year_value= value[Q1] + value[Q2] +value[Q3] +value[Q4] 
        one_year_offset=0-TRADING_DAYS_IN_YEAR
        prior_year_value= value[one_year_offset -Q1] +value[one_year_offset -Q2] +value[one_year_offset -Q3] + value[one_year_offset -Q4]
        out[:] = last_year_value - prior_year_value
        
def initialize(context):
    """
    Called once at the start of the algorithm.
    """   
    log.info("initialzing")
    # Rebalance every day, 1 hour after market open.
    schedule_function(my_rebalance, date_rules.month_start(), time_rules.market_open(hours=1))
     
    # Record tracking variables at the end of each day.
    schedule_function(my_record_vars, date_rules.every_day(), time_rules.market_close())
     

    # Create our dynamic stock selector.
    attach_pipeline(make_pipeline(), 'my_pipeline')
         
def make_pipeline():
  
    log.info("making a pipeline")
    screen_criteria = get_tradeable_stocks()
    
    #return on assets is greater than zero past year
    positive_roa = TTM_roa() > 0
    screen_criteria= screen_criteria & positive_roa
    
    #positive cash_flow past year
    positive_cash_flow = TTM_operating_cash_flow() > 0
    screen_criteria = screen_criteria & positive_cash_flow
    
    #remove microcaps
    market_cap = morningstar.valuation.market_cap.latest > 5e8 #500million
    screen_criteria = screen_criteria & market_cap #2305
    
    #roa this year > last year
    increase_in_roa = roa_YOY(mask=screen_criteria) >0
    screen_criteria= screen_criteria & increase_in_roa #1277
    
    # is cash flow greater than income after taxes- is the company accruing cash?
    # This might be better changed to net_income_from_continuing_operations? 
    accruals = accrued_cash() >0
    screen_criteria= screen_criteria & accruals

    #decreasing long term debt
    #should this be long_term_debt_capital_ratio?
    decreasing_debt = long_term_debt_YOY(mask=screen_criteria) <=0
    screen_criteria = screen_criteria & decreasing_debt #623
    
    #increasing current_ratio
    increased_current_ratio = current_ratio_YOY(mask=screen_criteria) >=0
    screen_criteria = screen_criteria & increased_current_ratio
    
    # same or lesser shares_outstanding
    shares_outstanding =oustanding_shares_YOY(mask=screen_criteria) <= 0
    screen_criteria = screen_criteria & shares_outstanding #316
    
    increasing_gross_margin = gross_margin_YOY(mask=screen_criteria) >=0
    screen_criteria = screen_criteria & increasing_gross_margin
    
    #is this correct?
    increasing_asset_turnover = asset_turnover_YOY(mask=screen_criteria) >=0
    screen_criteria = screen_criteria & increasing_asset_turnover #190
    
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
