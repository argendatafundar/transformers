from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
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
rename_cols(map={'valor_en_mtco2e': 'valor', 'sector': 'indicador'}),
	sort_values_by_comparison(colname='indicador', precedence={'Energía': 0, 'AGSyOUT': 1, 'Procesos industriales y uso de productos': 2, 'Residuos': 3}, prefix=['anio'], suffix=[])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 116 entries, 0 to 115
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             116 non-null    int64  
#   1   sector           116 non-null    object 
#   2   valor_en_mtco2e  116 non-null    float64
#  
#  |    |   anio | sector   |   valor_en_mtco2e |
#  |---:|-------:|:---------|------------------:|
#  |  0 |   1990 | AGSyOUT  |            151.97 |
#  
#  ------------------------------
#  
#  rename_cols(map={'valor_en_mtco2e': 'valor', 'sector': 'indicador'})
#  RangeIndex: 116 entries, 0 to 115
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       116 non-null    int64  
#   1   indicador  116 non-null    object 
#   2   valor      116 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1990 | AGSyOUT     |  151.97 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Energía': 0, 'AGSyOUT': 1, 'Procesos industriales y uso de productos': 2, 'Residuos': 3}, prefix=['anio'], suffix=[])
#  Index: 116 entries, 1 to 115
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       116 non-null    int64  
#   1   indicador  116 non-null    object 
#   2   valor      116 non-null    float64
#  
#  |    |   anio | indicador   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  1 |   1990 | Energía     |   95.45 |
#  
#  ------------------------------
#  