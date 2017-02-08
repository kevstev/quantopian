from quantopian.pipeline import CustomFactor, Pipeline
import numpy

class operating_cash_flow(CustomFactor):
    inputs = [morningstar.cash_flow_statement.operating_cash_flow]  
    window_length=1
    def compute(self, today, asset_ids, out, cash_flow):
        out[:] = cash_flow

class net_eps(CustomFactor):
    inputs = [morningstar.earnings_report.normalized_basic_eps]
    window_length=1
    def compute(self, today, asset_ids, out, eps):
        out[:] = sum(eps)

class sector(CustomFactor):
    inputs = [morningstar.asset_classification.morningstar_sector_code]
    window_length=1
    def compute(self, today,asset_ids, out, sector_code):
        out[:]= sector_code

class net_current_assets_per_share(CustomFactor):
    inputs=[morningstar.balance_sheet.current_assets,
            morningstar.balance_sheet.total_liabilities,
            morningstar.balance_sheet.preferred_stock,
            morningstar.valuation.shares_outstanding
            ]
    window_length=1
    def compute(self, today, asset_ids, out, current_assets, total_liabilities, preferred_stock, shares_outstanding):
        out[:]=((current_assets - (total_liabilities + preferred_stock))

class UniverseFilter(CustomFactor):  
    """  
    Return 1.0 for the following class of assets, otherwise 0.0:  
      * No Financials (103), Real Estate (104), Basic Materials (101) and ADR  
        (Basic Materials are too much sensitive to exogenous macroeconomical shocks.)  
      * Only primary common stocks  
      * Exclude When Distributed(WD), When Issued(WI) and VJ - usuallly companies in bankruptcy  
      * Exclude Halted stocks (_V, _H)  
      * Only NYSE, AMEX and Nasdaq  
    """  
    window_length = 1  
    inputs = [morningstar.share_class_reference.is_primary_share,  
              morningstar.share_class_reference.is_depositary_receipt,  
              morningstar.asset_classification.morningstar_sector_code  
              ]  
    def compute(self, today, assets, out, is_primary_share, is_depositary_receipt, sector_code):  
        criteria = is_primary_share[-1] # Only primary Common Stock  
        criteria = criteria & (~is_depositary_receipt[-1]) # No ADR  
        criteria = criteria & (sector_code[-1] != 101) # No Basic Materials  
        criteria = criteria & (sector_code[-1] != 103) # No Financials  
        criteria = criteria & (sector_code[-1] != 104) # No Real Estate  
        def accept_symbol(equity):  
            symbol = equity.symbol  
            if symbol.endswith("_PR") or symbol.endswith("_WI") or symbol.endswith("_WD") or symbol.endswith("_VJ") or symbol.endswith("_V") or symbol.endswith("_H"):  
                return False  
            else:  
                return True  
        def accept_exchange(equity):  
            exchange = equity.exchange  
            if exchange == "NEW YORK STOCK EXCHANGE":  
                return True  
            elif exchange == "AMERICAN STOCK EXCHANGE":  
                return True  
            elif exchange.startswith("NASDAQ"):  
                return True  
            else:  
                return False  
        vsid = numpy.vectorize(sid)  
        equities = vsid(assets)  
        # Exclude When Distributed(WD), When Issued(WI) and VJ (bankruptcy) and Halted stocks (V, H)  
        vaccept_symbol = np.vectorize(accept_symbol)  
        accept_symbol = vaccept_symbol(equities)  
        criteria = criteria & (accept_symbol)  
        # Only NYSE, AMEX and Nasdaq  
        vaccept_exchange = np.vectorize(accept_exchange)  
        accept_exchange = vaccept_exchange(equities)  
        criteria = criteria & (accept_exchange)  
        out[:] = criteria.astype(float)  

from quantopian.pipeline import Pipeline
from quantopian.research import run_pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume, SimpleMovingAverage
from quantopian.pipeline.data import morningstar
from quantopian.pipeline import filters


def make_pipeline():
  
    positive_cash_flow = operating_cash_flow() >0
    positive_eps = net_eps() >0
    non_financial = sector()!=103
    is_primary_share = morningstar.share_class_reference.is_primary_share.latest 
    is_not_depositary = ~morningstar.share_class_reference.is_depositary_receipt.latest
    ncav = net_current_assets_per_share() > USEquityPricing.close.latest
    #universe_filter = UniverseFilter()

    screen_criteria= positive_cash_flow & positive_eps & non_financial & is_primary_share & is_not_depositary & ncav
    
    dollar_volume = AverageDollarVolume(window_length=30)
    high_dollar_volume = (dollar_volume > 10000000)
    
   # mean_close_10 = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=10, mask=high_dollar_volume)
    #mean_close_30 = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=30, mask=high_dollar_volume)
    #percent_difference = (mean_close_10 - mean_close_30) / mean_close_30
    #mean_crossover_filter = mean_close_10 < mean_close_30
    #latest_close = USEquityPricing.close.latest
    
    return Pipeline(
        columns={
            'ncav': ncav
 
        },
        screen=screen_criteria
    )

my_pipe = make_pipeline()

result = run_pipeline(my_pipe, '2015-05-05', '2015-05-05')
print 'Number of securities that passed the filter: %d' % len(result)
print result