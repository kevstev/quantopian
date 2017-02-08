"""
This is a template algorithm on Quantopian for you to adapt and fill in.
"""
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume
from quantopian.pipeline.filters.morningstar import Q500US
from quantopian.pipeline.data import morningstar

 
def initialize(context):
    """
    Called once at the start of the algorithm.
    """   
    my_pipe=make_pipeline()

    # Rebalance every day, 1 hour after market open.
    schedule_function(my_rebalance, date_rules.every_day(), time_rules.market_open(hours=1))
     
    # Record tracking variables at the end of each day.
    schedule_function(my_record_vars, date_rules.every_day(), time_rules.market_close())
     
    # Create our dynamic stock selector.
    attach_pipeline(make_pipeline(), 'my_pipeline')
         
def make_pipeline():
    """
    A function to create our dynamic stock selector (pipeline). Documentation on
    pipeline can be found here: https://www.quantopian.com/help#pipeline-title
    """
    
    # Base universe set to the Q500US
    base_universe = Q500US()

    # Factor of yesterday's close price.
    yesterday_close = USEquityPricing.close.latest
     
    pipe = Pipeline(
        screen = base_universe,
        columns = {
            'close': yesterday_close,
        }
    )
    return pipe
 
def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    df_fundamentals = get_fundamentals(
        query(
            # put your query in here by typing "fundamentals."
            fundamentals.balance_sheet.current_assets,
            fundamentals.balance_sheet.total_liabilities,
            fundamentals.balance_sheet.preferred_stock,
            fundamentals.valuation.shares_outstanding,
            fundamentals.earnings_report.normalized_basic_eps,
            fundamentals.cash_flow_statement.operating_cash_flow,
            fundamentals.asset_classification.morningstar_sector_code,
            fundamentals.valuation_ratios.pe_ratio
            
            
        )
        .filter(
            fundamentals.cash_flow_statement.operating_cash_flow >0,
            fundamentals.asset_classification.morningstar_sector_code!= 103,
             fundamentals.earnings_report.normalized_basic_eps >0
        )
        .order_by(
            # sort your query
        )
        .limit(100)
    )
    
    context.output = pipeline_output('my_pipeline')
  
    # These are the securities that we are interested in trading each day.
    context.security_list = context.output.index
     
def my_assign_weights(context, data):
    """
    Assign weights to securities that we want to order.
    """
    pass
 
def my_rebalance(context,data):
    """
    Execute orders according to our schedule_function() timing. 
    """
    pass
 
def my_record_vars(context, data):
    """
    Plot variables at the end of each day.
    """
    pass
 
def handle_data(context,data):
    """
    Called every minute.
    """
    for stock in data: print stock
