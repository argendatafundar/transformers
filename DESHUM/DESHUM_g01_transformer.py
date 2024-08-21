from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
latest_year(by='anio'),
	rename_cols(map={'iso3': 'geocodigo', 'idh': 'valor'}),
	drop_col(col=['pais_nombre', 'continente_fundar', 'es_agregacion'], axis=1),
	replace_value(col='geocodigo', curr_value='ZZA.VHHD', new_value='DESHUM_ZZA.VHHD'),
	replace_value(col='geocodigo', curr_value='ZZB.HHD', new_value='DESHUM_ZZB.HHD'),
	replace_value(col='geocodigo', curr_value='ZZC.MHD', new_value='DESHUM_ZZC.MHD'),
	replace_value(col='geocodigo', curr_value='ZZD.LHD', new_value='DESHUM_ZZD.LHD'),
	replace_value(col='geocodigo', curr_value='ZZE.AS', new_value='DESHUM_ZZE.AS'),
	replace_value(col='geocodigo', curr_value='ZZF.EAP', new_value='DESHUM_ZZF.EAP'),
	replace_value(col='geocodigo', curr_value='ZZG.ECA', new_value='DESHUM_ZZG.ECA'),
	replace_value(col='geocodigo', curr_value='ZZH.LAC', new_value='DESHUM_ZZH.LAC'),
	replace_value(col='geocodigo', curr_value='ZZI.SA', new_value='DESHUM_ZZI.SA'),
	replace_value(col='geocodigo', curr_value='ZZJ.SSA', new_value='DESHUM_ZZJ.SSA'),
	replace_value(col='geocodigo', curr_value='ZZK.WORLD', new_value='WLD')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 6171 entries, 0 to 6170
#  Data columns (total 6 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               6171 non-null   object 
#   1   pais_nombre        6171 non-null   object 
#   2   continente_fundar  5808 non-null   object 
#   3   es_agregacion      5808 non-null   float64
#   4   anio               6171 non-null   int64  
#   5   idh                6171 non-null   float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   anio |   idh |
#  |---:|:-------|:--------------|:--------------------|----------------:|-------:|------:|
#  |  0 | AFG    | Afganistán    | Asia                |               0 |   1990 | 0.284 |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 204 entries, 32 to 6170
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   iso3               204 non-null    object 
#   1   pais_nombre        204 non-null    object 
#   2   continente_fundar  193 non-null    object 
#   3   es_agregacion      193 non-null    float64
#   4   idh                204 non-null    float64
#  
#  |    | iso3   | pais_nombre   | continente_fundar   |   es_agregacion |   idh |
#  |---:|:-------|:--------------|:--------------------|----------------:|------:|
#  | 32 | AFG    | Afganistán    | Asia                |               0 | 0.462 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'idh': 'valor'})
#  Index: 204 entries, 32 to 6170
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigo          204 non-null    object 
#   1   pais_nombre        204 non-null    object 
#   2   continente_fundar  193 non-null    object 
#   3   es_agregacion      193 non-null    float64
#   4   valor              204 non-null    float64
#  
#  |    | geocodigo   | pais_nombre   | continente_fundar   |   es_agregacion |   valor |
#  |---:|:------------|:--------------|:--------------------|----------------:|--------:|
#  | 32 | AFG         | Afganistán    | Asia                |               0 |   0.462 |
#  
#  ------------------------------
#  
#  drop_col(col=['pais_nombre', 'continente_fundar', 'es_agregacion'], axis=1)
#  Index: 204 entries, 32 to 6170
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  204 non-null    object 
#   1   valor      204 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 32 | AFG         |   0.462 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='ZZA.VHHD', new_value='DESHUM_ZZA.VHHD')
#  Index: 204 entries, 32 to 6170
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  204 non-null    object 
#   1   valor      204 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 32 | AFG         |   0.462 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='ZZB.HHD', new_value='DESHUM_ZZB.HHD')
#  Index: 204 entries, 32 to 6170
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  204 non-null    object 
#   1   valor      204 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 32 | AFG         |   0.462 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='ZZC.MHD', new_value='DESHUM_ZZC.MHD')
#  Index: 204 entries, 32 to 6170
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  204 non-null    object 
#   1   valor      204 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 32 | AFG         |   0.462 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='ZZD.LHD', new_value='DESHUM_ZZD.LHD')
#  Index: 204 entries, 32 to 6170
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  204 non-null    object 
#   1   valor      204 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 32 | AFG         |   0.462 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='ZZE.AS', new_value='DESHUM_ZZE.AS')
#  Index: 204 entries, 32 to 6170
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  204 non-null    object 
#   1   valor      204 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 32 | AFG         |   0.462 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='ZZF.EAP', new_value='DESHUM_ZZF.EAP')
#  Index: 204 entries, 32 to 6170
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  204 non-null    object 
#   1   valor      204 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 32 | AFG         |   0.462 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='ZZG.ECA', new_value='DESHUM_ZZG.ECA')
#  Index: 204 entries, 32 to 6170
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  204 non-null    object 
#   1   valor      204 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 32 | AFG         |   0.462 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='ZZH.LAC', new_value='DESHUM_ZZH.LAC')
#  Index: 204 entries, 32 to 6170
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  204 non-null    object 
#   1   valor      204 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 32 | AFG         |   0.462 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='ZZI.SA', new_value='DESHUM_ZZI.SA')
#  Index: 204 entries, 32 to 6170
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  204 non-null    object 
#   1   valor      204 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 32 | AFG         |   0.462 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='ZZJ.SSA', new_value='DESHUM_ZZJ.SSA')
#  Index: 204 entries, 32 to 6170
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  204 non-null    object 
#   1   valor      204 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 32 | AFG         |   0.462 |
#  
#  ------------------------------
#  
#  replace_value(col='geocodigo', curr_value='ZZK.WORLD', new_value='WLD')
#  Index: 204 entries, 32 to 6170
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  204 non-null    object 
#   1   valor      204 non-null    float64
#  
#  |    | geocodigo   |   valor |
#  |---:|:------------|--------:|
#  | 32 | AFG         |   0.462 |
#  
#  ------------------------------
#  