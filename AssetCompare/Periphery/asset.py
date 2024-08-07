from SEC.sec import SEC

import pandas as pd

import datetime as dt


class Asset:
    def __init__(
        self, ticker: str, annual: bool = False, quarter: bool = False
    ) -> None:
        self.ticker = ticker.upper()

        self.annual = annual
        self.quarter = quarter

        if annual:
            self.period = "A"
            self.filings_path = f"Filings\\Companies\\{self.ticker}\\10-K"
        elif quarter:
            self.period = "Q"
            self.filings_path = f"Filings\\Companies\\{self.ticker}\\10-Q"

        self.annual_data = SEC(self.ticker, "10-K")
        self.quarter_data = SEC(self.ticker, "10-Q")
        # Financial Statements
        self.income_statement = pd.DataFrame()
        self.balance_sheet = pd.DataFrame()
        self.cash_flow = pd.DataFrame()

    """
    =====================================================
    Financial Statments
    =====================================================
    """

    def set_income_statement(self):
        path = f"{self.filings_path}\\{self.ticker}_{self.period}_income_statement.csv"
        try:
            df = pd.read_csv(path)
        except FileNotFoundError:
            if self.quarter:
                self.quarter_data.process_all_statements()
                df = pd.read_csv(path)
            elif self.annual:
                self.annual_data.process_all_statements()
                df = pd.read_csv(path)
        df.rename(columns={"Unnamed: 0": "index"}, inplace=True)
        df.set_index("index", inplace=True)

        print(f"DF: {df}")
        index = self._index_keyword_search("Revenue", 0, df.index.to_list())
        print(f"Index: {index}")

        # Add Q4 data.
        if self.quarter:
            annual_path = f"Filings\\Companies\\{self.ticker}\\10-K\\{self.ticker}_A_income_statement.csv"
            try:
                annual_data = pd.read_csv(annual_path)
            except FileNotFoundError:
                self.annual_data.process_all_statements()
                annual_data = pd.read_csv(annual_path)
            # Fix index names.
            annual_data.rename(columns={"Unnamed: 0": "index"}, inplace=True)
            annual_data.set_index("index", inplace=True)
            merge = pd.concat([df, annual_data], axis=1)

            merge = self._sort_df_by_date(merge)

            fiscal_period = self.get_fiscal_periods()

            print(f"Fiscal: {fiscal_period}")

            if fiscal_period.empty:
                quarters = self._organize_quarters(annual_data.columns[-1], df.columns)
                self.write_fiscal_period(quarters)
                fiscal_period = self.get_fiscal_periods()
            # print(f"Quarters: {quarters}")

    """
    =====================================================
    Utilities
    =====================================================
    """

    def _organize_quarters(self, annual_anchor: str, fiscal_periods: list):
        """
        annual_anchor: str
            Date of the most recent Q4.

        fiscal_periods: list
            List of fiscal periods for the company. NOTE: Assumes that dates are in 'str' type.
        """
        less_than = []
        annual_anchor_dt = dt.datetime.strptime(annual_anchor, "%Y-%m-%d")
        annual_anchor_str = annual_anchor.split("-")
        annual_anchor_str = f"{annual_anchor_str[1]}-{annual_anchor_str[2]}"
        # Iterate in reverse so recent dates come first in the loop.
        for f in fiscal_periods[::-1]:
            f_dt = dt.datetime.strptime(f, "%Y-%m-%d")
            if f_dt < annual_anchor_dt:
                year, month, day = f.split("-")
                f = f"{month}-{day}"
                less_than.append(f)

        quarters = less_than[:3]

        return {
            "Q1": quarters[2],
            "Q2": quarters[1],
            "Q3": quarters[0],
            "Q4": annual_anchor_str,
        }

    def get_fiscal_periods(self):
        path = f"Filings\\FiscalPeriods\\fiscal_periods.csv"

        try:
            df = pd.read_csv(path)
            df.rename(columns={"Unnamed: 0": "index"}, inplace=True)
            df.set_index("index", inplace=True)
            print(f"NEW: {df}")
            data = df.loc[self.ticker]
            return data
        except FileNotFoundError:
            return pd.DataFrame()

    def write_fiscal_period(self, quarters: dict):
        path = f"Filings\\FiscalPeriods\\fiscal_periods.csv"
        df = pd.DataFrame({f"{self.ticker}": quarters}).T
        df.to_csv(path)

    def _sort_df_by_date(self, df: pd.DataFrame):

        df.columns = pd.to_datetime(df.columns)
        try:
            df = df.reindex(sorted(df.columns), axis=1)
        except ValueError as e:
            print(f"DF: {df}      \n\n{e}")
            exit()
        return df

    def _index_keyword_search(
        self, keyword: str, keyword_location: int, indexes: list, split_char: str = " "
    ):

        counter = 0
        for i in indexes:
            i = i.split(split_char)[keyword_location]

            if i == keyword:
                return counter

            counter += 1

        return -1
