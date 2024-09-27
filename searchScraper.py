from typing import List

from bs4 import BeautifulSoup
import time
import requests
from random import randint
from html.parser import HTMLParser

from collections import OrderedDict
import json
import csv
import logging


USER_AGENT = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/61.0.3163.100 Safari/537.36'}
# USER_AGENT = {
#     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
# }
NUM_RESULTS = 10
INPUT_DIR = 'input'
OUTPUT_DIR = 'output'
GOOGLE_RESULTS_FILENAME = 'Google_Result1.json'
QUERIES_FILENAME = '100QueriesSet1.txt'
BING_RESULTS_FILENAME = 'hw1.json'
STATS_FILENAME = 'hw1.csv'


def get_path(dir: str, filename: str) -> str:
    return f'{dir}/{filename}'


def get_clean_url(url: str) -> str:
    return url.removeprefix('http://').removeprefix('https://').removeprefix('www.').removesuffix('/')


def are_similar_urls(url1: str, url2: str) -> bool:
    return get_clean_url(url1) == get_clean_url(url2)


class Bing:
    @staticmethod
    def _getSearchURL(query: str, count=30):
        return f"http://www.bing.com/search?q={'+'.join(query.split())}&count={count}"

    @staticmethod
    def search(query: str, sleep=True, raw_count=30, res_count=None, unique=True):
        if sleep:  # Prevents loading too many pages too soon
            time.sleep(randint(5, 15))
        url = Bing._getSearchURL(query, count=raw_count)
        results = Bing._scrape_search_result(url, res_count=res_count, check_unique=unique)
        return results

    @staticmethod
    def _scrape_search_result(url: str, res_count: int = None, check_unique=True):
        text = requests.get(url, headers=USER_AGENT).text  # ?: getting trimmed result here for some queries
        # with open("output/text.txt", 'w') as f:
        #     f.write(text)
        soup = BeautifulSoup(text, "html.parser")
        raw_results = soup.find_all("li", attrs={"class": "b_algo"})  # Bing selectors
        logging.info(f"Raw results length: {len(raw_results)}")

        res = []
        clean_urls = set()

        for result in raw_results:
            link = result.find("a").get('href')
            logging.info(f"Link: {link}")

            if check_unique:
                clean_url = get_clean_url(link)
                if clean_url in clean_urls:  # a dup exists
                    continue
                clean_urls.add(clean_url)
            res.append(link)
            if res_count and len(res) >= res_count:
                break
        return res


class Task:
    def __init__(self, res_count, google_res_file, bing_res_file, query_file, stats_file):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        self.res_count = res_count
        self.google_res_file = google_res_file
        self.bing_res_file = bing_res_file
        self.query_file = query_file
        self.stats_file = stats_file

    def get_bing_results(self, scrape=False, sleep=True,
                         query_start=0, query_end=None) -> OrderedDict:
        # try to read existing results
        try:
            with open(self.bing_res_file, "r") as f:
                logging.info(f"Reading {self.bing_res_file}")
                queries_results = json.load(f, object_pairs_hook=OrderedDict)
        except FileNotFoundError:
            queries_results = OrderedDict()

        if scrape:
            with open(self.query_file, 'r') as f:
                queries = f.readlines()
                for query in queries[query_start:query_end]:
                    query = query.strip()
                    logging.info(f"Scraping query: {query}")
                    query_results = Bing.search(query, sleep=sleep, raw_count=30, res_count=self.res_count)
                    logging.info(f"Query results length: {len(query_results)}")
                    queries_results[query] = query_results
            with open(self.bing_res_file, "w") as f:
                logging.info(f"Saving results of all queries to {self.bing_res_file}")
                json.dump(queries_results, f, indent=4)

        return queries_results

    def read_google_results(self, res_count=None, scrape=False, sleep=True,
                            query_start=0, query_end=None) -> OrderedDict:
        # try to read existing results
        try:
            with open(self.google_res_file, "r") as f:
                logging.info(f"Reading {self.google_res_file}")
                return json.load(f, object_pairs_hook=OrderedDict)
        except FileNotFoundError:
            return None

    @staticmethod
    def _query_stats(google_res: List[str], bing_res: List[str]) -> (int, float, float):
        n = len(google_res)
        google_ranks = {get_clean_url(link): index + 1 for index, link in enumerate(google_res)}
        bing_ranks = {get_clean_url(link): index + 1 for index, link in enumerate(bing_res)}
        n_overlap = 0
        sum_squared_d = 0
        for link in google_ranks:
            if link in bing_ranks:
                n_overlap += 1
                sum_squared_d += (google_ranks[link] - bing_ranks[link]) ** 2
        percent_overlap = n_overlap / n * 100
        if n_overlap == 0 or (n_overlap == 1 and sum_squared_d > 0):
            spearman_coef = 0
        elif n_overlap == 1 and sum_squared_d == 0:
            spearman_coef = 1
        else:
            spearman_coef = 1 - 6 * sum_squared_d / (n_overlap * (n_overlap ** 2 - 1))
        return n_overlap, percent_overlap, spearman_coef

    def write_all_stats(self, google_results: OrderedDict, bing_results: OrderedDict):
        with open(self.stats_file, mode='w') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(["Queries", "Number of Overlapping Results", "Percent Overlap", "Spearman Coefficient"])
            sum_n_overlap = 0
            sum_percent_overlap = 0
            sum_spearman_coef = 0
            n_queries = len(google_results)
            logging.info(f"Calculating stats for {n_queries} queries")
            for query in google_results:
                n_overlap, percent_overlap, spearman_coef = Task._query_stats(google_results[query], bing_results[query])
                sum_n_overlap += n_overlap
                sum_percent_overlap += percent_overlap
                sum_spearman_coef += spearman_coef
                row = [query, n_overlap, percent_overlap, spearman_coef]
                csv_writer.writerow(row)
            csv_writer.writerow(["Averages", sum_n_overlap/n_queries, sum_percent_overlap/n_queries,
                                 sum_spearman_coef/n_queries])




if __name__ == '__main__':
    task = Task(res_count=NUM_RESULTS,
                google_res_file=get_path(INPUT_DIR, GOOGLE_RESULTS_FILENAME),
                bing_res_file=get_path(OUTPUT_DIR, BING_RESULTS_FILENAME),
                query_file=get_path(INPUT_DIR, QUERIES_FILENAME),
                stats_file=get_path(OUTPUT_DIR, STATS_FILENAME))
    # bing_res = task.get_bing_results(scrape=True)
    # bing_res = task.get_bing_results(scrape=True, query_start=0, query_end=1)
    bing_res = task.get_bing_results()
    google_res = task.read_google_results()
    task.write_all_stats(google_res, bing_res)

