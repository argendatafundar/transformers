from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='geonombreFundar == "Argentina"'),
	rename_cols(map={'idha_subdimension': 'categoria'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 20880 entries, 0 to 20879
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    20880 non-null  object 
#   1   geonombreFundar    20880 non-null  object 
#   2   anio               20880 non-null  int64  
#   3   idha_subdimension  20880 non-null  object 
#   4   valor              18425 non-null  float64
#  
#  |    | geocodigoFundar   | geonombreFundar   |   anio | idha_subdimension      |   valor |
#  |---:|:------------------|:------------------|-------:|:-----------------------|--------:|
#  |  0 | AFG               | Afganistán        |   1870 | Años de escolarización |     nan |
#  
#  ------------------------------
#  
#  query(condition='geonombreFundar == "Argentina"')
#  Index: 120 entries, 1920 to 2039
#  Data columns (total 5 columns):
#   #   Column             Non-Null Count  Dtype  
#  ---  ------             --------------  -----  
#   0   geocodigoFundar    120 non-null    object 
#   1   geonombreFundar    120 non-null    object 
#   2   anio               120 non-null    int64  
#   3   idha_subdimension  120 non-null    object 
#   4   valor              120 non-null    float64
#  
#  |      | geocodigoFundar   | geonombreFundar   |   anio | idha_subdimension      |     valor |
#  |-----:|:------------------|:------------------|-------:|:-----------------------|----------:|
#  | 1920 | ARG               | Argentina         |   1870 | Años de escolarización | 0.0388517 |
#  
#  ------------------------------
#  
#  rename_cols(map={'idha_subdimension': 'categoria'})
#  Index: 120 entries, 1920 to 2039
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   geocodigoFundar  120 non-null    object 
#   1   geonombreFundar  120 non-null    object 
#   2   anio             120 non-null    int64  
#   3   categoria        120 non-null    object 
#   4   valor            120 non-null    float64
#  
#  |      | geocodigoFundar   | geonombreFundar   |   anio | categoria              |     valor |
#  |-----:|:------------------|:------------------|-------:|:-----------------------|----------:|
#  | 1920 | ARG               | Argentina         |   1870 | Años de escolarización | 0.0388517 |
#  
#  ------------------------------
#  