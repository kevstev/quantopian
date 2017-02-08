"""
This is a template algorithm on Quantopian for you to adapt and fill in.
"""
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume
 
def initialize(context):
    """
    Called once at the start of the algorithm.
    """   
    # Rebalance every day, 1 hour after market open.
    schedule_function(my_rebalance, date_rules.every_day(), time_rules.market_open(hours=1))
     
    # Record tracking variables at the end of each day.
    schedule_function(my_record_vars, date_rules.every_day(), time_rules.market_close())
     
    # Create our dynamic stock selector.
    attach_pipeline(make_pipeline(), 'my_pipeline')
    
    context.stocks = [symbol('UWTI'),symbol('DWTI')] 
    
         
def make_pipeline():
    """
    A function to create our dynamic stock selector (pipeline). Documentation on
    pipeline can be found here: https://www.quantopian.com/help#pipeline-title
    """
    
     
    # Create a dollar volume factor.
    dollar_volume = AverageDollarVolume(window_length=1)
 
    # Pick the top 1% of stocks ranked by dollar volume.
    high_dollar_volume = dollar_volume.percentile_between(99, 100)
     
    pipe = Pipeline(
        screen = high_dollar_volume,
        columns = {
            'dollar_volume': dollar_volume
        }
    )
    return pipe
 
def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    context.output = pipeline_output('my_pipeline')
  
    # These are the securities that we are interested in trading each day.
    value = 0
    for sec in context.portfolio.positions:  
        shares = context.portfolio.positions[sec.sid].amount;
        price = data.current(sec, 'price');
        value += shares * price;
    
    context.security_list = context.output.index
    total_value = context.portfolio.cash + value
    log.info('current cash:' + str(context.portfolio.cash) + "total current value: " + str(total_value))
    log_position(context, data)
        
     
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
    UWTI_Position = context.portfolio.positions[symbol('UWTI')].amount *        data.current(symbol('UWTI'), 'price')
    DWTI_Position = context.portfolio.positions[symbol('DWTI')].amount * data.current(symbol('DWTI'), 'price')
    
    if (abs(UWTI_Position) + abs(DWTI_Position)) < 1000000:
        order(symbol('UWTI'),-1000)
        
    #    return
    
  #  if (DWTI_Position >= UWTI_Position):   
  #      if len(get_open_orders(symbol('DWTI')))== 0:
   #         log.info("ordering DWTI")
   #         log_position(context, data)
   #         order(symbol('DWTI'),-100) 
    
   # if DWTI_Position <= UWTI_Position:
   #     if len(get_open_orders(symbol('UWTI')))== 0:
   #         delta_position = UWTI_Position - DWTI_Position
   #         #log.info("delta position: " + str(delta_position))
   #         log.info("ordering UWTI delta position:"  + str(delta_position))
   #         log_position(context, data)
    #        order(symbol('UWTI'),-1000)
            #order_value(symbol('UWTI'),delta_position * -1)
                     

def has_orders(context):
    # Return true if there are pending orders.
    has_orders = False
    open_orders = get_open_orders()
    for stock in open_orders:
        orders = open_orders[stock]
        if orders:
            for oo in orders:
                message = 'Open order for {amount} shares in {stock}'  
                message = message.format(amount=oo.amount, stock=stock)
                log.info(message)
                has_orders = True
            return has_orders
       
def log_position(context, data):
    
    for sec in context.portfolio.positions:  
        shares = context.portfolio.positions[sec.sid].amount;
        price = data.current(sec, 'price');
        value = shares * price;
        
        log.info(sec.symbol +  " price: " + str(price) + " shares:" + str(shares) + " value: " + str(value))
        

       
