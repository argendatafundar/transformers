from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname+'_map'
    df_ = df.copy()
    df_[mapcol] = df_[colname].map(precedence)
    df_ = df_.sort_values(by=[*prefix, mapcol, *suffix])
    return df_.drop(mapcol, axis=1)

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	rename_cols(map={'valor_en_mtco2e': 'valor', 'sector': 'indicador'}),
	replace_value(col='indicador', curr_value='Procesos industriales y uso de productos', new_value='PIUP'),
	sort_values_by_comparison(colname='indicador', precedence={'Energía': 0, 'AGSyOUT': 1, 'PIUP': 2, 'Residuos': 3}, prefix=['anio'], suffix=[])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 132 entries, 0 to 131
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             132 non-null    int64  
#   1   geonombreFundar  132 non-null    object 
#   2   geocodigoFundar  132 non-null    object 
#   3   sector           132 non-null    object 
#   4   valor_en_mtco2e  132 non-null    float64
#  
#  |    |   anio | geonombreFundar   | geocodigoFundar   | sector   |   valor_en_mtco2e |
#  |---:|-------:|:------------------|:------------------|:---------|------------------:|
#  |  0 |   1990 | Argentina         | ARG               | AGSyOUT  |           151.182 |
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_mtco2e': 'valor', 'sector': 'indicador'})
#  RangeIndex: 132 entries, 0 to 131
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             132 non-null    int64  
#   1   geonombreFundar  132 non-null    object 
#   2   geocodigoFundar  132 non-null    object 
#   3   indicador        132 non-null    object 
#   4   valor            132 non-null    float64
#  
#  |    |   anio | geonombreFundar   | geocodigoFundar   | indicador   |   valor |
#  |---:|-------:|:------------------|:------------------|:------------|--------:|
#  |  0 |   1990 | Argentina         | ARG               | AGSyOUT     | 151.182 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Procesos industriales y uso de productos', new_value='PIUP')
#  RangeIndex: 132 entries, 0 to 131
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             132 non-null    int64  
#   1   geonombreFundar  132 non-null    object 
#   2   geocodigoFundar  132 non-null    object 
#   3   indicador        132 non-null    object 
#   4   valor            132 non-null    float64
#  
#  |    |   anio | geonombreFundar   | geocodigoFundar   | indicador   |   valor |
#  |---:|-------:|:------------------|:------------------|:------------|--------:|
#  |  0 |   1990 | Argentina         | ARG               | AGSyOUT     | 151.182 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Energía': 0, 'AGSyOUT': 1, 'PIUP': 2, 'Residuos': 3}, prefix=['anio'], suffix=[])
#  Index: 132 entries, 66 to 131
#  Data columns (total 5 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             132 non-null    int64  
#   1   geonombreFundar  132 non-null    object 
#   2   geocodigoFundar  132 non-null    object 
#   3   indicador        132 non-null    object 
#   4   valor            132 non-null    float64
#  
#  |    |   anio | geonombreFundar   | geocodigoFundar   | indicador   |   valor |
#  |---:|-------:|:------------------|:------------------|:------------|--------:|
#  | 66 |   1990 | Argentina         | ARG               | Energía     |  102.32 |
#  
#  ------------------------------
#  