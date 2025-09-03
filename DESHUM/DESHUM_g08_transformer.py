from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
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

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='geonombreFundar == "Argentina"'),
	rename_cols(map={'idh_tipo': 'categoria'}),
	replace_value(col=None, curr_value=None, new_value=None, mapping={'categoria': {'IDH años esperados de escolarización': 'Esperanza de escolarización', 'IDH ingreso per cápita': 'Ingreso per cápita', 'IDH años de escolarización 25+': 'Años de escolarización 25+', 'IDH esperanza de vida': 'Esperanza de vida', 'IDH escolarización agregado': 'Escolarización agregado', 'IDH agregado': 'IDH Agregado'}})
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 40788 entries, 0 to 40787
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  40590 non-null  object 
#   1   geonombreFundar  40590 non-null  object 
#   2   country          40788 non-null  object 
#   3   anio             40788 non-null  int64  
#   4   idh_tipo         40788 non-null  object 
#   5   valor            38647 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   | country     |   anio | idh_tipo                             |    valor |
#  |---:|:------------------|:------------------|:------------|-------:|:-------------------------------------|---------:|
#  |  0 | AFG               | Afganistán        | Afghanistan |   1990 | IDH años esperados de escolarización | 0.163137 |
#  
#  ------------------------------
#  
#  query(condition='geonombreFundar == "Argentina"')
#  Index: 198 entries, 990 to 1187
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  198 non-null    object 
#   1   geonombreFundar  198 non-null    object 
#   2   country          198 non-null    object 
#   3   anio             198 non-null    int64  
#   4   idh_tipo         198 non-null    object 
#   5   valor            198 non-null    float64
#  
#  |     | geocodigoFundar   | geonombreFundar   | country   |   anio | idh_tipo                             |    valor |
#  |----:|:------------------|:------------------|:----------|-------:|:-------------------------------------|---------:|
#  | 990 | ARG               | Argentina         | Argentina |   1990 | IDH años esperados de escolarización | 0.744027 |
#  
#  ------------------------------
#  
#  rename_cols(map={'idh_tipo': 'categoria'})
#  Index: 198 entries, 990 to 1187
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  198 non-null    object 
#   1   geonombreFundar  198 non-null    object 
#   2   country          198 non-null    object 
#   3   anio             198 non-null    int64  
#   4   categoria        198 non-null    object 
#   5   valor            198 non-null    float64
#  
#  |     | geocodigoFundar   | geonombreFundar   | country   |   anio | categoria                            |    valor |
#  |----:|:------------------|:------------------|:----------|-------:|:-------------------------------------|---------:|
#  | 990 | ARG               | Argentina         | Argentina |   1990 | IDH años esperados de escolarización | 0.744027 |
#  
#  ------------------------------
#  
#  replace_value(col=None, curr_value=None, new_value=None, mapping={'categoria': {'IDH años esperados de escolarización': 'Esperanza de escolarización', 'IDH ingreso per cápita': 'Ingreso per cápita', 'IDH años de escolarización 25+': 'Años de escolarización 25+', 'IDH esperanza de vida': 'Esperanza de vida', 'IDH escolarización agregado': 'Escolarización agregado', 'IDH agregado': 'IDH Agregado'}})
#  Index: 198 entries, 990 to 1187
#  Data columns (total 6 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  198 non-null    object 
#   1   geonombreFundar  198 non-null    object 
#   2   country          198 non-null    object 
#   3   anio             198 non-null    int64  
#   4   categoria        198 non-null    object 
#   5   valor            198 non-null    float64
#  
#  |     | geocodigoFundar   | geonombreFundar   | country   |   anio | categoria                   |    valor |
#  |----:|:------------------|:------------------|:----------|-------:|:----------------------------|---------:|
#  | 990 | ARG               | Argentina         | Argentina |   1990 | Esperanza de escolarización | 0.744027 |
#  
#  ------------------------------
#  