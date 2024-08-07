import os

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

import logging
import calendar

# Periphery
from SEC.Periphery.mappings import statement_keys_map


class Edgar:
    def __init__(self, headers, save: bool = False, update: bool = False) -> None:
        self.headers = headers
        self.save = save
        self.update = update
        self.cik_path = "SEC\\Periphery\\Storage\\cik.csv"

    """
    =====================================================
    CIK
    =====================================================
    """

    def get_cik(self, ticker: str):
        ticker = ticker.upper()

        if self.update:
            df = self._query_cik_data()
            df.to_csv(self.cik_path)
            cik = df.loc[ticker, "cik"]
        else:
            try:
                # Setting col 'cik' to str allows "0's" to be read in. Without this it would cut the leading 0's. Ex: 0000123456 -> 123456
                df = pd.read_csv(self.cik_path, dtype={"cik": str})
                df.rename(columns={"Unnamed: 0": "index"}, inplace=True)
                df.set_index("index", inplace=True)
                cik = df.loc[ticker, "cik"]
            except FileNotFoundError:
                df = self._query_cik_data()
                if self.save:
                    df.to_csv(self.cik_path)
                cik = df.loc[ticker, "cik"]
        return cik

    def _query_cik_data(self):
        ticker_json = requests.get(
            "https://www.sec.gov/files/company_tickers.json", headers=self.headers
        ).json()
        cik_df = pd.DataFrame(columns=["cik", "name"])
        for k, v in ticker_json.items():
            cik = str(v["cik_str"])
            cik = cik.zfill(10)
            ticker = v["ticker"]
            name = v["title"]
            cik_df.loc[ticker, "cik"] = cik
            cik_df.loc[ticker, "name"] = name
        cik_df = cik_df.sort_index()
        return cik_df

    def get_submission_data_for_ticker(self, ticker, only_filings_df=False):
        """
        Get the data in json form for a given ticker. For example: 'cik', 'entityType', 'sic', 'sicDescription', 'insiderTransactionForOwnerExists', 'insiderTransactionForIssuerExists', 'name', 'tickers', 'exchanges', 'ein', 'description', 'website', 'investorWebsite', 'category', 'fiscalYearEnd', 'stateOfIncorporation', 'stateOfIncorporationDescription', 'addresses', 'phone', 'flags', 'formerNames', 'filings'

        Args:
            ticker (str): The ticker symbol of the company.

        Returns:
            json: The submissions for the company.
        """
        cik = self.get_cik(ticker)
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        company_json = requests.get(url, headers=self.headers).json()
        if only_filings_df:
            return pd.DataFrame(company_json["filings"]["recent"])
        else:
            return company_json

    """
    =====================================================
    Accession Number
    =====================================================
    """

    def get_latest_accession_number(self, ticker, ten_k: bool):
        filings = self.get_filtered_filings(
            ticker, ten_k=ten_k, just_accession_numbers=True
        )
        return filings.iloc[0]

    def get_filtered_filings(self, ticker, ten_k=True, just_accession_numbers=False):
        company_filings_df = self.get_submission_data_for_ticker(
            ticker, only_filings_df=True
        )
        if ten_k:
            df = company_filings_df[company_filings_df["form"] == "10-K"]
        else:
            df = company_filings_df[company_filings_df["form"] == "10-Q"]
        if just_accession_numbers:
            df = df.set_index("reportDate")
            accession_df = df["accessionNumber"]
            return accession_df
        else:
            return df

    def get_facts(self, ticker):
        cik = self.get_cik(ticker)
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
        company_facts = requests.get(url, headers=self.headers).json()
        return company_facts

    def facts_DF(self, ticker):
        facts = self.get_facts(ticker)
        us_gaap_data = facts["facts"]["us-gaap"]
        df_data = []
        for fact, details in us_gaap_data.items():
            for unit in details["units"]:
                for item in details["units"][unit]:
                    row = item.copy()
                    row["fact"] = fact
                    df_data.append(row)

        df = pd.DataFrame(df_data)
        df["end"] = pd.to_datetime(df["end"])
        df["start"] = pd.to_datetime(df["start"])
        df = df.drop_duplicates(subset=["fact", "end", "val"])
        df.set_index("end", inplace=True)
        labels_dict = {fact: details["label"] for fact, details in us_gaap_data.items()}
        return df, labels_dict

    def annual_facts(self, ticker):
        accession_nums = self.get_filtered_filings(
            ticker, ten_k=True, just_accession_numbers=True
        )
        df, label_dict = self.facts_DF(ticker)
        ten_k = df[df["accn"].isin(accession_nums)]
        ten_k = ten_k[ten_k.index.isin(accession_nums.index)]
        pivot = ten_k.pivot_table(values="val", columns="fact", index="end")
        pivot.rename(columns=label_dict, inplace=True)
        return pivot.T

    def quarterly_facts(self, ticker):
        accession_nums = self.get_filtered_filings(
            ticker, ten_k=False, just_accession_numbers=True
        )
        df, label_dict = self.facts_DF(ticker)
        ten_q = df[df["accn"].isin(accession_nums)]
        ten_q = ten_q[ten_q.index.isin(accession_nums.index)].reset_index(drop=False)
        ten_q = ten_q.drop_duplicates(subset=["fact", "end"], keep="last")
        pivot = ten_q.pivot_table(values="val", columns="fact", index="end")
        pivot.rename(columns=label_dict, inplace=True)
        return pivot.T

    def save_dataframe_to_csv(
        self, dataframe, folder_name, ticker, statement_name, frequency
    ):
        directory_path = os.path.join(folder_name, ticker)
        os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, f"{statement_name}_{frequency}.csv")
        dataframe.to_csv(file_path)
        return None

    def _get_file_name(self, report):
        html_file_name_tag = report.find("HtmlFileName")
        xml_file_name_tag = report.find("XmlFileName")

        if html_file_name_tag:
            return html_file_name_tag.text
        elif xml_file_name_tag:
            return xml_file_name_tag.text
        else:
            return ""

    def _is_statement_file(self, short_name_tag, long_name_tag, file_name):
        return (
            short_name_tag is not None
            and long_name_tag is not None
            and file_name  # Check if file_name is not an empty string
            and "Statement" in long_name_tag.text
        )

    def get_statement_file_names_in_filing_summary(
        self, ticker, accession_number, external: bool = False
    ):
        try:
            session = requests.Session()
            cik = self.get_cik(ticker)
            base_link = (
                f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}"
            )
            filing_summary_link = f"{base_link}/FilingSummary.xml"
            filing_summary_response = session.get(
                filing_summary_link, headers=self.headers
            ).content.decode("utf-8")

            filing_summary_soup = BeautifulSoup(filing_summary_response, "lxml-xml")
            statement_file_names_dict = {}
            ext = []
            for report in filing_summary_soup.find_all("Report"):
                file_name = self._get_file_name(report)
                short_name, long_name = report.find("ShortName"), report.find(
                    "LongName"
                )
                if external:
                    statement_file_names_dict[short_name.text.lower()] = file_name
                elif not external:
                    if self._is_statement_file(short_name, long_name, file_name):
                        statement_file_names_dict[short_name.text.lower()] = file_name

            return statement_file_names_dict

        except requests.RequestException as e:
            print(f"An error occurred: {e}")
            return {}

    def get_statement_soup(
        self,
        ticker,
        accession_number,
        statement_name,
    ):
        """
        the statement_name should be one of the following:
        'balance_sheet'
        'income_statement'
        'cash_flow_statement'
        """
        session = requests.Session()

        cik = self.get_cik(ticker)
        base_link = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}"
        print(f"[Base]: {base_link}")
        statement_file_name_dict = self.get_statement_file_names_in_filing_summary(
            ticker, accession_number
        )

        statement_link = None
        for possible_key in statement_keys_map.get(statement_name.lower(), []):
            file_name = statement_file_name_dict.get(possible_key.lower())
            if file_name:
                statement_link = f"{base_link}/{file_name}"
                break

        if not statement_link:
            raise ValueError(f"Could not find statement file name for {statement_name}")

        try:
            statement_response = session.get(statement_link, headers=self.headers)
            statement_response.raise_for_status()  # Check if the request was successful

            if statement_link.endswith(".xml"):
                return BeautifulSoup(
                    statement_response.content, "lxml-xml", from_encoding="utf-8"
                )
            else:
                return BeautifulSoup(statement_response.content, "lxml")

        except requests.RequestException as e:
            raise ValueError(f"Error fetching the statement: {e}")

    def get_external_soup(
        self, ticker: str, accession_number: str, statement_name: str
    ):
        session = requests.Session()
        cik = self.get_cik(ticker)
        base_link = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}"
        statement_file_name_dict = self.get_statement_file_names_in_filing_summary(
            ticker, accession_number, external=True
        )
        # print(statement_file_name_dict)

        statement_link = None
        for possible_key in statement_keys_map.get(statement_name, []):
            file_name = statement_file_name_dict.get(possible_key.lower())
            if file_name:
                statement_link = f"{base_link}/{file_name}"
                print(f"Statement: {statement_link}")
                break

        if not statement_link:
            raise ValueError(f"Could not find statement file name for {statement_name}")

        try:
            statement_response = session.get(statement_link, headers=self.headers)
            statement_response.raise_for_status()  # Check if the request was successful

            if statement_link.endswith(".xml"):
                return BeautifulSoup(
                    statement_response.content, "lxml-xml", from_encoding="utf-8"
                )
            else:
                return BeautifulSoup(statement_response.content, "lxml")

        except requests.RequestException as e:
            raise ValueError(f"Error fetching the statement: {e}")

    def extract_columns_values_and_dates_from_statement(self, soup: BeautifulSoup):
        """
        Extracts columns, values, and dates from an HTML soup object representing a financial statement.

        Args:
            soup (BeautifulSoup): The BeautifulSoup object of the HTML document.

        Returns:
            tuple: Tuple containing columns, values_set, and date_time_index.
        """
        columns = []
        values_set = []
        date_time_index = self.get_datetime_index_dates_from_statement(soup)

        for table in soup.find_all("table"):
            unit_multiplier = 1
            special_case = False

            # Check table headers for unit multipliers and special cases
            table_header = table.find("th")
            if table_header:
                header_text = table_header.get_text()
                # Determine unit multiplier based on header text
                if "in Thousands" in header_text:
                    unit_multiplier = 1
                elif "in Millions" in header_text:
                    unit_multiplier = 1000
                # Check for special case scenario
                if "unless otherwise specified" in header_text:
                    special_case = True

            # Process each row of the table
            for row in table.select("tr"):
                onclick_elements = row.select("td.pl a, td.pl.custom a")
                if not onclick_elements:
                    continue

                # Extract column title from 'onclick' attribute
                onclick_attr = onclick_elements[0]["onclick"]
                column_title = onclick_attr.split("defref_")[-1].split("',")[0]
                columns.append(column_title)

                # Initialize values array with NaNs
                values = [np.nan] * len(date_time_index)

                # Process each cell in the row
                for i, cell in enumerate(row.select("td.text, td.nump, td.num")):
                    if "text" in cell.get("class"):
                        continue

                    # Clean and parse cell value
                    value = self.keep_numbers_and_decimals_only_in_string(
                        cell.text.replace("$", "")
                        .replace(",", "")
                        .replace("(", "")
                        .replace(")", "")
                        .strip()
                    )
                    if value:
                        value = float(value)
                        # Adjust value based on special case and cell class
                        if special_case:
                            value /= 1000
                        else:
                            if "nump" in cell.get("class"):
                                values[i] = value * unit_multiplier
                            else:
                                values[i] = -value * unit_multiplier

                values_set.append(values)

        return columns, values_set, date_time_index

    def get_datetime_index_dates_from_statement(
        self,
        soup: BeautifulSoup,
    ) -> pd.DatetimeIndex:
        """
        Extracts datetime index dates from the HTML soup object of a financial statement.

        Args:
            soup (BeautifulSoup): The BeautifulSoup object of the HTML document.

        Returns:
            pd.DatetimeIndex: A Pandas DatetimeIndex object containing the extracted dates.
        """
        table_headers = soup.find_all("th", {"class": "th"})
        dates = [str(th.div.string) for th in table_headers if th.div and th.div.string]
        dates = [self.standardize_date(date).replace(".", "") for date in dates]
        index_dates = pd.to_datetime(dates)
        return index_dates

    def standardize_date(self, date: str) -> str:
        """
        Standardizes date strings by replacing abbreviations with full month names.

        Args:
            date (str): The date string to be standardized.

        Returns:
            str: The standardized date string.
        """
        for abbr, full in zip(calendar.month_abbr[1:], calendar.month_name[1:]):
            date = date.replace(abbr, full)
        return date

    def keep_numbers_and_decimals_only_in_string(self, mixed_string: str):
        """
        Filters a string to keep only numbers and decimal points.

        Args:
            mixed_string (str): The string containing mixed characters.

        Returns:
            str: String containing only numbers and decimal points.
        """
        num = "1234567890."
        allowed = list(filter(lambda x: x in num, mixed_string))
        return "".join(allowed)

    def create_dataframe_of_statement_values_columns_dates(
        self, values_set, columns, index_dates
    ) -> pd.DataFrame:
        """
        Creates a DataFrame from statement values, columns, and index dates.

        Args:
            values_set (list): List of values for each column.
            columns (list): List of column names.
            index_dates (pd.DatetimeIndex): DatetimeIndex for the DataFrame index.

        Returns:
            pd.DataFrame: DataFrame constructed from the given data.
        """
        transposed_values_set = list(zip(*values_set))
        df = pd.DataFrame(transposed_values_set, columns=columns, index=index_dates)
        return df

    def process_one_statement(
        self, ticker, accession_number, statement_name, external: bool = False
    ):
        """
        Processes a single financial statement identified by ticker, accession number, and statement name.

        Args:
            ticker (str): The stock ticker.
            accession_number (str): The SEC accession number.
            statement_name (str): Name of the financial statement.

        Returns:
            pd.DataFrame or None: DataFrame of the processed statement or None if an error occurs.
        """
        try:
            soup = self.get_statement_soup(
                ticker,
                accession_number,
                statement_name,
            )
        except Exception as e:
            logging.error(
                f"Failed to get statement soup: {e} for accession number: {accession_number}"
            )
            return None

        if soup:
            try:
                # Extract data and create DataFrame
                columns, values, dates = (
                    self.extract_columns_values_and_dates_from_statement(soup)
                )
                df = self.create_dataframe_of_statement_values_columns_dates(
                    values, columns, dates
                )

                if not df.empty:
                    # Remove duplicate columns
                    df = df.T.drop_duplicates()
                else:
                    logging.warning(
                        f"Empty DataFrame for accession number: {accession_number}"
                    )
                    return None

                return df
            except Exception as e:
                logging.error(f"Error processing statement: {e}")
                return None

    def get_label_dictionary(self, ticker):
        facts = self.get_facts(ticker)
        us_gaap_data = facts["facts"]["us-gaap"]
        labels_dict = {fact: details["label"] for fact, details in us_gaap_data.items()}
        return labels_dict

    def rename_statement(self, statement, label_dictionary):
        # Extract the part after the first "_" and then map it using the label dictionary
        statement.index = statement.index.map(
            lambda x: label_dictionary.get(x.split("_", 1)[-1], x)
        )
        return statement

    """
    =====================================================
    External Table Handling
    =====================================================
    """

    def get_segments_table(self, ticker, acc_num):
        label = "segments"

        soup = self.get_external_soup(ticker, acc_num, label)
        # print(f"Soup: {soup}")

        headers = soup.find_all("th")

        # print(f"Headers: {headers}")

    def get_revenues_table(self, ticker, acc_num):
        label = "revenues"
        soup = self.get_external_soup(ticker, acc_num, label)
        # print(f"Soup: {soup}")

        headers = soup.find_all("th")
        cols = soup.find_all("td")

        print(f"Headers: {headers}   Cols: {cols}")
