from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

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
query(condition='tipo_energia != "Total"'),
	rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'}),
	sort_values_by_comparison(colname='indicador', precedence={'Otras renovables': 0, 'Biocombustibles': 1, 'Solar': 2, 'Eólica': 3, 'Nuclear': 4, 'Hidro': 5, 'Gas natural': 6, 'Petróleo': 7, 'Carbón': 8, 'Biomasa tradicional': 9}, prefix=['anio'], suffix=[])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 836 entries, 0 to 835
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          836 non-null    int64  
#   1   tipo_energia  836 non-null    object 
#   2   valor_en_twh  836 non-null    float64
#   3   porcentaje    836 non-null    float64
#  
#  |    |   anio | tipo_energia     |   valor_en_twh |   porcentaje |
#  |---:|-------:|:-----------------|---------------:|-------------:|
#  |  0 |   1800 | Otras renovables |              0 |            0 |
#  
#  ------------------------------
#  
#  query(condition='tipo_energia != "Total"')
#  Index: 760 entries, 0 to 759
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          760 non-null    int64  
#   1   tipo_energia  760 non-null    object 
#   2   valor_en_twh  760 non-null    float64
#   3   porcentaje    760 non-null    float64
#  
#  |    |   anio | tipo_energia     |   valor_en_twh |   porcentaje |
#  |---:|-------:|:-----------------|---------------:|-------------:|
#  |  0 |   1800 | Otras renovables |              0 |            0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'})
#  Index: 760 entries, 0 to 759
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        760 non-null    int64  
#   1   indicador   760 non-null    object 
#   2   valor       760 non-null    float64
#   3   porcentaje  760 non-null    float64
#  
#  |    |   anio | indicador        |   valor |   porcentaje |
#  |---:|-------:|:-----------------|--------:|-------------:|
#  |  0 |   1800 | Otras renovables |       0 |            0 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Otras renovables': 0, 'Biocombustibles': 1, 'Solar': 2, 'Eólica': 3, 'Nuclear': 4, 'Hidro': 5, 'Gas natural': 6, 'Petróleo': 7, 'Carbón': 8, 'Biomasa tradicional': 9}, prefix=['anio'], suffix=[])
#  Index: 760 entries, 0 to 759
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        760 non-null    int64  
#   1   indicador   760 non-null    object 
#   2   valor       760 non-null    float64
#   3   porcentaje  760 non-null    float64
#  
#  |    |   anio | indicador        |   valor |   porcentaje |
#  |---:|-------:|:-----------------|--------:|-------------:|
#  |  0 |   1800 | Otras renovables |       0 |            0 |
#  
#  ------------------------------
#  