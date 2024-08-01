from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname+'_map'
    df_ = df.copy()
    df_[mapcol] = df_[colname].map(precedence)
    df_ = df_.sort_values(by=[*prefix, mapcol, *suffix])
    return df_.drop(mapcol, axis=1)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'sector': 'indicador', 'valor_en_ggco2e': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=0.001),
	sort_values_by_comparison(colname='indicador', precedence={'Energía': 0, 'AGSyOUT': 1, 'PIUP': 2, 'Residuos': 3, 'Otros': 4}, prefix=['anio'], suffix=[])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 825 entries, 0 to 824
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             825 non-null    int64  
#   1   sector           825 non-null    object 
#   2   valor_en_ggco2e  825 non-null    float64
#  
#  |    |   anio | sector   |   valor_en_ggco2e |
#  |---:|-------:|:---------|------------------:|
#  |  0 |   1850 | AGSyOUT  |         4.896e+06 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador', 'valor_en_ggco2e': 'valor'})
#  RangeIndex: 825 entries, 0 to 824
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       825 non-null    int64  
#   1   indicador  825 non-null    object 
#   2   valor      825 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1850 | AGSyOUT     |    4896 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=0.001)
#  RangeIndex: 825 entries, 0 to 824
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       825 non-null    int64  
#   1   indicador  825 non-null    object 
#   2   valor      825 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1850 | AGSyOUT     |    4896 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Energía': 0, 'AGSyOUT': 1, 'PIUP': 2, 'Residuos': 3, 'Otros': 4}, prefix=['anio'], suffix=[])
#  Index: 825 entries, 1 to 822
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       825 non-null    int64  
#   1   indicador  825 non-null    object 
#   2   valor      825 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  1 |   1850 | Energía     |     508 |
#  
#  ------------------------------
#  