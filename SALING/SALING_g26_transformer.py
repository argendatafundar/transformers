from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str = None, curr_value: str = None, new_value: str = None, mapping = None):
    if mapping is not None:
        df = df.replace(mapping).copy()
    elif col is not None and curr_value is not None and new_value is not None:
        df = df.replace({col: curr_value}, new_value)
    else:
        raise ValueError("Either 'mapping' must be provided, or all of 'col', 'curr_value', and 'new_value' must be provided")
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'tipo_prima': 'categoria', 'prima': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100),
	replace_value(col='categoria', curr_value=None, new_value=None, mapping={'Controlando por variables sociodemográficas': 'Con control', 'Sin control por otras variables': 'Sin controles'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        42 non-null     int64  
#   1   prima       42 non-null     float64
#   2   tipo_prima  42 non-null     object 
#  
#  |    |   anio |    prima | tipo_prima                                  |
#  |---:|-------:|---------:|:--------------------------------------------|
#  |  0 |   2003 | 0.438929 | Controlando por variables sociodemográficas |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_prima': 'categoria', 'prima': 'valor'})
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       42 non-null     int64  
#   1   valor      42 non-null     float64
#   2   categoria  42 non-null     object 
#  
#  |    |   anio |   valor | categoria                                   |
#  |---:|-------:|--------:|:--------------------------------------------|
#  |  0 |   2003 | 43.8929 | Controlando por variables sociodemográficas |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       42 non-null     int64  
#   1   valor      42 non-null     float64
#   2   categoria  42 non-null     object 
#  
#  |    |   anio |   valor | categoria                                   |
#  |---:|-------:|--------:|:--------------------------------------------|
#  |  0 |   2003 | 43.8929 | Controlando por variables sociodemográficas |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value=None, new_value=None, mapping={'Controlando por variables sociodemográficas': 'Con control', 'Sin control por otras variables': 'Sin controles'})
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       42 non-null     int64  
#   1   valor      42 non-null     float64
#   2   categoria  42 non-null     object 
#  
#  |    |   anio |   valor | categoria   |
#  |---:|-------:|--------:|:------------|
#  |  0 |   2003 | 43.8929 | Con control |
#  
#  ------------------------------
#  