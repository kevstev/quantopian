# Put any initialization logic here.  The context object will be passed to
# the other methods in your algorithm.
def initialize(context):
        # Implement your algorithm logic here.
    log.info("test-initialize")
    
def before_trading_start(context, data):
    log.info("Test-before_trading_start")
    
    dividendList = get_fundamentals(
         query(
             fundamentals.company_reference.short_name,
             fundamentals.company_reference.primary_symbol,
             fundamentals.valuation_ratios.payout_ratio,
             fundamentals.earnings_report.dividend_per_share,
             fundamentals.valuation_ratios.dividend_yield,
             fundamentals.valuation_ratios.dividend_yield_as_of,
             fundamentals.valuation_ratios.forward_dividend_yield
        )
         .filter(
             fundamentals.earnings_report.dividend_per_share > 0
         )
         .order_by(
             fundamentals.valuation_ratios.forward_dividend_yield.desc()
         )
         .limit(100)
    )
    
    for stock in dividendList:
        log.info(str(dividendList[stock]['primary_symbol'] ) + ", "
                 + dividendList[stock]['short_name'] + ", "
                 + str(dividendList[stock]['dividend_per_share']) + ", "
                 + str(dividendList[stock]['dividend_yield'] * 100) + ", "
                 + str(dividendList[stock]['forward_dividend_yield'] * 100) + ", "
                 + str(dividendList[stock]['dividend_yield_as_of'])
)
    
    
    
# Will be called on every trade event for the securities you specify. 
def handle_data(context, data):
    log.info("test-handle_data")
  
    # data[sid(X)] holds the trade event data for that security.
    # context.portfolio holds the current portfolio state.

    # Place orders with the order(SID, amount) method.

    # TODO: implement your own logic here.
    order(sid(24), 50)


