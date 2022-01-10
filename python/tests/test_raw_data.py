import pandas as pd
from sqlalchemy import create_engine

from ..raw_data import _get_excel_data, _treat_dataframes, _get_raw_dataframes

engine = create_engine('postgresql://username:secret@localhost:5432/pipeline')
link = "https://github.com/iurizambotto/anp-pipeline/raw/main/data/vendas-combustiveis-m3.xls"
df = _get_excel_data(link)
dfs = _treat_dataframes(df)
df_raw_dev_petro, df_raw_oleo_diesel = _get_raw_dataframes(dfs)


class TestDevPetroTable:
    def test_dev_petro_table(self):
        query = "select * from vendas_derivados_petroleo"
        df_table = pd.read_sql(query, engine)

        raw_value_jan_2000 = int(df_raw_dev_petro[df_raw_dev_petro["Dados"] == "Janeiro"][2000].values[0])
        table_value_jan_2000 = int(df_table[(df_table.year_month.dt.month==1) & (df_table.year_month.dt.year==2000)].groupby(by="year_month").volume.sum().values[0])
        
        raw_value_jun_2012 = int(df_raw_dev_petro[df_raw_dev_petro["Dados"] == "Junho"][2012].values[0])
        table_value_jun_2012 = int(df_table[(df_table.year_month.dt.month==6) & (df_table.year_month.dt.year==2012)].groupby(by="year_month").volume.sum().values[0])
        
        assert raw_value_jan_2000 == table_value_jan_2000
        assert raw_value_jun_2012 == table_value_jun_2012


class TestOleoDieselTable:
    def test_oleo_diesel_table(self):
        query = "select * from vendas_oleo_diesel"
        df_table = pd.read_sql(query, engine)

        raw_value_jan_2013 = int(df_raw_oleo_diesel[df_raw_oleo_diesel["Dados"] == "Janeiro"][2013].values[0])
        table_value_jan_2013 = int(df_table[(df_table.year_month.dt.month==1) & (df_table.year_month.dt.year==2013)].groupby(by="year_month").volume.sum().values[0])
        
        raw_value_set_2018 = int(df_raw_oleo_diesel[df_raw_oleo_diesel["Dados"] == "Setembro"][2018].values[0])
        table_value_set_2018 = int(df_table[(df_table.year_month.dt.month==9) & (df_table.year_month.dt.year==2018)].groupby(by="year_month").volume.sum().values[0])
        
        assert raw_value_jan_2013 == table_value_jan_2013
        assert raw_value_set_2018 == table_value_set_2018
