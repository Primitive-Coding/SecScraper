import os
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Periphery
from SEC.Periphery.mappings import statement_keys_map
from SEC.Periphery.edgar import Edgar

pd.options.display.float_format = lambda x: (
    "{:,.0f}".format(x) if int(x) == x else "{:,.2f}".format(x)
)

headers = {"User-Agent": "hederatracker@gmail.com"}


class SEC:
    def __init__(
        self, ticker: str, form_type: str, save: bool = True, update: bool = False
    ) -> None:
        self.ticker = ticker.upper()
        self.form_type = form_type.upper()
        if form_type == "10-K":
            self.ten_k = True
            self.file = "A"
        elif form_type == "10-Q":
            self.ten_k = False
            self.file = "Q"

        self.save = save
        self.update = update

        self.edgar = Edgar(headers, self.save, self.update)

    """
    =====================================================
    Filings
    =====================================================
    """

    def get_filings(self):
        acc = self.edgar.get_filtered_filings(
            self.ticker, ten_k=self.ten_k, just_accession_numbers=True
        )
        acc_num = acc.iloc[0].replace("-", "")
        filings = self.edgar.get_statement_file_names_in_filing_summary(
            self.ticker, acc_num
        )
        return filings

    """
    =====================================================
    Financial Statements
    =====================================================
    """

    def process_all_statements(self):
        filings_path = f"Filings\\Companies\\{self.ticker.upper()}"
        if self.form_type.upper() == "10-Q":
            folder_path = f"{filings_path}\\10-Q"
            os.makedirs(folder_path, exist_ok=True)
            ten_k = False
        elif self.form_type.upper() == "10-K":
            folder_path = f"{filings_path}\\10-K"
            os.makedirs(folder_path, exist_ok=True)
            ten_k = True
        acc = self.edgar.get_filtered_filings(
            self.ticker, ten_k=self.ten_k, just_accession_numbers=True
        )
        index = 0
        income_statement = pd.DataFrame()
        balance_sheet = pd.DataFrame()
        cash_flow = pd.DataFrame()
        for a in acc:
            # Format acc number
            a = a.replace("-", "")
            income_statement = self.process_statement(
                income_statement, a, income_statement=True
            )
            balance_sheet = self.process_statement(balance_sheet, a, balance_sheet=True)
            cash_flow = self.process_statement(cash_flow, a, cash_flow=True)

            if index == 10:
                break
            index += 1

        # Reverse dataframes so newest filings are on the right side.
        income_statement = income_statement[income_statement.columns[::-1]]
        balance_sheet = balance_sheet[balance_sheet.columns[::-1]]
        cash_flow = cash_flow[cash_flow.columns[::-1]]
        # Rename indexes
        label_dict = self.edgar.get_label_dictionary(self.ticker)
        income_statement = self.edgar.rename_statement(income_statement, label_dict)
        balance_sheet = self.edgar.rename_statement(balance_sheet, label_dict)
        cash_flow = self.edgar.rename_statement(cash_flow, label_dict)
        # Sort by dates
        income_statement = self._sort_df_by_date(income_statement)
        balance_sheet = self._sort_df_by_date(balance_sheet)
        cash_flow = self._sort_df_by_date(cash_flow)
        # Export to csv files.
        income_statement.to_csv(
            f"{folder_path}\\{self.ticker}_{self.file}_income_statement.csv"
        )
        balance_sheet.to_csv(
            f"{folder_path}\\{self.ticker}_{self.file}_balance_sheet.csv"
        )
        cash_flow.to_csv(f"{folder_path}\\{self.ticker}_{self.file}_cash_flow.csv")

    """--------------- Balance Sheet ---------------"""

    def get_balance_sheet(self, acc_num: int = 0):
        if acc_num == 0:
            acc = self.edgar.get_filtered_filings(
                self.ticker, ten_k=self.ten_k, just_accession_numbers=True
            )
            acc_num = acc.iloc[0].replace("-", "")

        statement = self.edgar.process_one_statement(
            self.ticker, acc_num, "balance_sheet"
        )
        return statement

    """--------------- Income Statement ---------------"""

    def get_income_statement(self, acc_num: int = 0):
        if acc_num == 0:
            acc = self.edgar.get_filtered_filings(
                self.ticker, ten_k=self.ten_k, just_accession_numbers=True
            )
            acc_num = acc.iloc[0].replace("-", "")

        statement = self.edgar.process_one_statement(
            self.ticker, acc_num, "income_statement"
        )
        return statement

    """--------------- Cash Flow ---------------"""

    def get_cash_flow(self, acc_num: int = 0):
        if acc_num == 0:
            acc = self.edgar.get_filtered_filings(
                self.ticker, ten_k=self.ten_k, just_accession_numbers=True
            )
            acc_num = acc.iloc[0].replace("-", "")

        statement = self.edgar.process_one_statement(
            self.ticker, acc_num, "income_statement"
        )
        return statement

    """--------------- Revenues ---------------"""

    def get_revenues(self, acc_num: int = 0):
        if acc_num == 0:
            acc = self.edgar.get_filtered_filings(
                self.ticker, ten_k=self.ten_k, just_accession_numbers=True
            )
            acc_num = acc.iloc[0].replace("-", "")

        self.edgar.get_revenues_table(self.ticker, acc_num)

    def get_segments(self, acc_num: int = 0):
        if acc_num == 0:
            acc = self.edgar.get_filtered_filings(
                self.ticker, ten_k=self.ten_k, just_accession_numbers=True
            )
            acc_num = acc.iloc[0].replace("-", "")

        self.edgar.get_segments_table(self.ticker, acc_num)

    """--------------- Process Statement ---------------"""

    def process_statement(
        self,
        statement: pd.DataFrame,
        acc_num: str,
        income_statement: bool = False,
        balance_sheet: bool = False,
        cash_flow: bool = False,
    ):
        if self.ten_k:
            if statement.empty:
                statement = self._query_statement(
                    acc_num, income_statement, balance_sheet, cash_flow
                )
                cols = statement.columns
                statement.columns = [ts.strftime("%Y-%m-%d") for ts in cols]

            else:
                new_statement = self._query_statement(
                    acc_num, income_statement, balance_sheet, cash_flow
                )
                # Get columns
                new_cols = new_statement.columns
                prev_cols = statement.columns
                # Convert to strings
                new_cols = [ts.strftime("%Y-%m-%d") for ts in new_cols]
                try:
                    prev_cols = [ts.strftime("%Y-%m-%d") for ts in prev_cols]
                except AttributeError:
                    pass
                for c in new_cols:
                    if c not in prev_cols:
                        try:
                            new_statement = new_statement[c].to_frame(name=c)
                        except AttributeError:
                            new_statement = new_statement[c]
                        try:
                            statement = pd.concat([statement, new_statement], axis=1)
                        except pd.errors.InvalidIndexError:
                            # Get indexes in new dataframe that are not present in old.
                            diff_index = [
                                item
                                for item in new_statement.index.to_list()
                                if item not in statement.index.to_list()
                            ]
                            # Fill previous dataframe with "nan" values for indexes that are new.
                            for p in prev_cols:
                                for d in diff_index:
                                    statement.loc[d, p] = np.nan

                            new_indexes = set(new_statement.index.to_list())
                            new_statement = new_statement.loc[list(new_indexes)]
                            # Consolidate duplicate rows. Put their sum into a single row.
                            consolidated_df = new_statement.groupby(
                                new_statement.index
                            ).sum()
                            # Merge dataframe with new indexes.
                            statement = pd.concat([statement, consolidated_df], axis=1)

        elif not self.ten_k:
            if statement.empty:
                statement = self._query_statement(
                    acc_num, income_statement, balance_sheet, cash_flow
                )
                cols = statement.columns
                statement.columns = [ts.strftime("%Y-%m-%d") for ts in cols]

            else:
                new_statement = self._query_statement(
                    acc_num, income_statement, balance_sheet, cash_flow
                )
                try:
                    new_statement.columns = [
                        ts.strftime("%Y-%m-%d") for ts in new_statement.columns
                    ]
                # Typically raises if no new statements are found.
                except AttributeError:
                    return statement

                # Get columns
                new_cols = new_statement.columns
                prev_cols = statement.columns.to_list()

                try:
                    prev_cols = [ts.strftime("%Y-%m-%d") for ts in prev_cols]
                except AttributeError:
                    pass

                if len(new_cols) > 2:
                    new_statement = new_statement.iloc[:, :2]
                    new_cols = new_statement.columns  # Reassign new columns

                for c in new_cols:
                    if c not in prev_cols:
                        try:
                            new_slice = new_statement[c].to_frame(name=c)
                        except AttributeError:
                            print(f"Attribute: {new_statement}    [{c}]")
                            new_slice = new_statement[c]
                        try:
                            statement = pd.concat([statement, new_slice], axis=1)
                        except pd.errors.InvalidIndexError:
                            # Get indexes in new dataframe that are not present in old.
                            diff_index = [
                                item
                                for item in new_statement.index.to_list()
                                if item not in statement.index.to_list()
                            ]
                            # Fill previous dataframe with "nan" values for indexes that are new.
                            for p in prev_cols:
                                for d in diff_index:
                                    statement.loc[d, p] = np.nan

                            new_indexes = set(new_statement.index.to_list())
                            new_statement = new_statement.loc[list(new_indexes)]
                            # Consolidate duplicate rows. Put their sum into a single row.
                            consolidated_df = new_statement.groupby(
                                new_statement.index
                            ).sum()
                            # Merge dataframe with new indexes.
                            statement = pd.concat(
                                [statement, consolidated_df[c]], axis=1
                            )
        return statement

    def _sort_df_by_date(self, df: pd.DataFrame):

        df.columns = pd.to_datetime(df.columns)
        try:
            df = df.reindex(sorted(df.columns), axis=1)
        except ValueError as e:
            print(f"DF: {df}      \n\n{e}")
            exit()
        return df

    def _query_statement(
        self,
        acc_num,
        income_statement: bool = False,
        balance_sheet: bool = False,
        cash_flow: bool = False,
    ):
        if income_statement:
            statement = self.get_income_statement(acc_num)
        elif balance_sheet:
            statement = self.get_balance_sheet(acc_num)
        elif cash_flow:
            statement = self.get_cash_flow(acc_num)
        return statement

    """
    =====================================================
    =====================================================
    """
