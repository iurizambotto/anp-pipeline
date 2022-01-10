# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import datetime
import pandas as pd
import re
import requests as req
from sqlalchemy import create_engine
import logging
import unicodedata

from variables import regex, months, states

engine = create_engine("postgresql://username:secret@db:5432/pipeline")
logging.basicConfig(level=logging.DEBUG)

def _get_table_last_update(engine):
    query = "select keyword, max(last_update) last_update from anp_updates group by 1"
    table_last_update = pd.read_sql(query, engine).to_dict(orient="records")
    logging.info("Get tables last update")
    return table_last_update


def get_table_name(keyword):
    nfkd_form = unicodedata.normalize("NFKD", keyword)
    only_ascii = nfkd_form.encode("ASCII", "ignore")
    removed_text = only_ascii.decode().replace("de ", "")
    table_name = "_".join(removed_text.lower().split())
    return table_name


def _get_anp_links(keywords, link):
    logging.info("Get ANP links")
    Web = req.get(link)
    S = BeautifulSoup(Web.text, "lxml")

    news = S.find_all(id="parent-fieldname-text")[0]
    links = news.find_all("li")

    links_final = {}
    for keyword in keywords:
        for line in links:
            elements = line.find_all("a", class_="internal-link")
            regex_last_update = re.search(regex, str(line))
            if regex_last_update:
                last_update = regex_last_update.group(0)
            for element in elements:
                if (
                    keyword.lower() in str(element).lower()
                    and "metadados" not in str(element).lower()
                    and "metros" in str(element).lower()
                ):
                    # print(last_update, element)
                    links_final[keyword] = {
                        "link": element["href"],
                        "last_update": last_update,
                    }
    return links_final


def _treat_dataframe(dataframe):
    logging.info("Treat dataframe")
    dataframe.columns = [column.lower() for column in dataframe.columns]
    dataframe["month"] = dataframe["mês"].apply(lambda x: months[x.lower()])
    dataframe["uf"] = dataframe["unidade da federação"].apply(
        lambda x: states[x.lower()]
    )
    dataframe["day"] = 1
    dataframe["year_month"] = pd.to_datetime(
        (dataframe.ano * 10000 + dataframe.month * 100 + dataframe.day).apply(
            str
        ),
        format="%Y%m%d",
    )
    dataframe["volume"] = dataframe.vendas.str.replace(",", ".").astype(
        "float64"
    )
    dataframe["unit"] = "m3"
    dataframe = dataframe.rename(columns={"produto": "product"})
    dataframe["created_at"] = datetime.datetime.now()
    dataframe = dataframe[
        ["year_month", "uf", "product", "unit", "volume", "created_at"]
    ]
    dataframe = dataframe.sort_values(
        by=["year_month", "uf", "product"]
    ).reset_index()
    return dataframe


def _save_data(keywords, links):
    logging.info("Start to save dataframes")
    last_update = []
    for keyword in keywords:
        df = pd.read_csv(links[keyword]["link"], sep=";")
        df1 = _treat_dataframe(df)
        table_name = get_table_name(keyword)
        df1 = df1.drop("index", axis=1)
        df1.to_sql(
            table_name, engine, if_exists="replace", index=False
        )
        logging.info("Saved to database")
        last_update_str = links[keyword]["last_update"].replace("/", "_")
        _save_to_s3(
            df1,
            f"s3://anp-pipeline/{table_name}/anp_pipeline_{last_update_str}.parquet"
            )
        logging.info("Saved to raw zone in s3")
        last_update += [
            {
                "keyword": table_name,
                "last_update": links[keyword]["last_update"],
            }
        ]

    pd.DataFrame(last_update).to_sql(
        "anp_updates", engine, if_exists="replace", index=False
    )

def _save_to_s3(dataframe, path):
    dataframe.to_parquet(path, index=False)

if __name__ == "__main__":
    link = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/vendas-de-derivados-de-petroleo-e-biocombustiveis"
    keywords = ["Vendas de derivados petróleo", "Vendas de óleo diesel"]
    links = _get_anp_links(keywords, link)

    table_last_update = _get_table_last_update(engine)
    if table_last_update:
        for keyword in keywords:
            table_name = get_table_name(keyword)
            anp_last_update = links.get(keyword).get("last_update")
            for table in table_last_update:
                if table.get("keyword") == table_name:
                    if not table.get("last_update") == anp_last_update:
                        print("DIFFERENT DATES", table)
                        _save_data(keywords, links)
                    else:
                        print("EQUAL DATES", table)
    else:
        _save_data(keywords, links)
