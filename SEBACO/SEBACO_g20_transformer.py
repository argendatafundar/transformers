from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'descripcion': 'indicador'}),
	rename_cols(map={'prop_sobre_sbc_expo': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 102 non-null    int64  
#   1   descripcion          102 non-null    object 
#   2   prop_sobre_sbc_expo  102 non-null    float64
#  
#  |    |   anio | descripcion           |   prop_sobre_sbc_expo |
#  |---:|-------:|:----------------------|----------------------:|
#  |  0 |   2006 | Propiedad intelectual |             0.0319314 |
#  
#  ------------------------------
#  
#  rename_cols(map={'descripcion': 'indicador'})
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column               Non-Null Count  Dtype  
#  ---  ------               --------------  -----  
#   0   anio                 102 non-null    int64  
#   1   indicador            102 non-null    object 
#   2   prop_sobre_sbc_expo  102 non-null    float64
#  
#  |    |   anio | indicador             |   prop_sobre_sbc_expo |
#  |---:|-------:|:----------------------|----------------------:|
#  |  0 |   2006 | Propiedad intelectual |             0.0319314 |
#  
#  ------------------------------
#  
#  rename_cols(map={'prop_sobre_sbc_expo': 'valor'})
#  RangeIndex: 102 entries, 0 to 101
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       102 non-null    int64  
#   1   indicador  102 non-null    object 
#   2   valor      102 non-null    float64
#  
#  |    |   anio | indicador             |     valor |
#  |---:|-------:|:----------------------|----------:|
#  |  0 |   2006 | Propiedad intelectual | 0.0319314 |
#  
#  ------------------------------
#  