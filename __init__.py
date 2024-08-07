# from SEC.sec import SEC
from SEC.sec import SEC

import pandas as pd
from AssetCompare.Periphery.asset import Asset

if __name__ == "__main__":

    ticker = "AAPL"
    form_type = "10-Q"
    asset = Asset(ticker, quarter=True)
    asset.set_income_statement()
    # sec = SEC(ticker, form_type)
    # # sec.get_revenues(ticker, 0)
    # sec.process_all_statements()
    # # sec.test()
