import pandas as pd


def _get_excel_data(link):
    dataframe = pd.read_excel(link)
    dataframe = dataframe[~dataframe[dataframe.columns[1]].isna()]
    dataframe = (
        dataframe.reset_index()
        .drop("index", axis=1)
        .drop("Unnamed: 0", axis=1)
    )
    return dataframe


def _treat_dataframes(dataframe):
    dataframes = []
    for i in range(1, 4):
        if not i == 2:
            ind = (
                dataframe[dataframe[dataframe.columns[0]] == "Dados"]
                .head(i)
                .index.values[-1]
            )
            ind1 = (
                dataframe[dataframe[dataframe.columns[0]] == "Total do Ano"]
                .head(i)
                .index.values[-1]
            )
            dataframes += [dataframe[ind:ind1]]
    return dataframes


def _get_raw_dataframes(dfs):
    dataframes = []
    for dataframe in dfs:
        renamed_columns = dataframe.head(1).to_dict(orient="records")[0]
        for k in renamed_columns:
            if str(renamed_columns[k]) == "nan":
                renamed_columns[k] = k
        dataframe = dataframe.rename(renamed_columns, axis=1)
        dataframe = dataframe[1:]
        dataframes += [dataframe]
    return dataframes[0], dataframes[1]
