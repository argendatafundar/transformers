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

@transformer.convert
def to_pandas(df: pl.DataFrame, dummy = True):
    df = df.to_pandas()
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	to_pandas(dummy=True),
	query(condition='tipo_energia != "Total"'),
	rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'}),
	sort_values_by_comparison(colname='indicador', precedence={'Otras renovables': 0, 'Biocombustibles': 1, 'Solar': 2, 'Eólica': 3, 'Nuclear': 4, 'Hidro': 5, 'Gas natural': 6, 'Petróleo': 7, 'Carbón': 8, 'Biomasa tradicional': 9}, prefix=['anio'], suffix=[])
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 847 entries, 0 to 846
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          847 non-null    int64  
#   1   tipo_energia  847 non-null    object 
#   2   valor_en_twh  847 non-null    float64
#   3   porcentaje    847 non-null    float64
#  
#  |    |   anio | tipo_energia     |   valor_en_twh |   porcentaje |
#  |---:|-------:|:-----------------|---------------:|-------------:|
#  |  0 |   1800 | Otras renovables |              0 |            0 |
#  
#  ------------------------------
#  
#  query(condition='tipo_energia != "Total"')
#  Index: 770 entries, 0 to 769
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          770 non-null    int64  
#   1   tipo_energia  770 non-null    object 
#   2   valor_en_twh  770 non-null    float64
#   3   porcentaje    770 non-null    float64
#  
#  |    |   anio | tipo_energia     |   valor_en_twh |   porcentaje |
#  |---:|-------:|:-----------------|---------------:|-------------:|
#  |  0 |   1800 | Otras renovables |              0 |            0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'})
#  Index: 770 entries, 0 to 769
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        770 non-null    int64  
#   1   indicador   770 non-null    object 
#   2   valor       770 non-null    float64
#   3   porcentaje  770 non-null    float64
#  
#  |    |   anio | indicador        |   valor |   porcentaje |
#  |---:|-------:|:-----------------|--------:|-------------:|
#  |  0 |   1800 | Otras renovables |       0 |            0 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Otras renovables': 0, 'Biocombustibles': 1, 'Solar': 2, 'Eólica': 3, 'Nuclear': 4, 'Hidro': 5, 'Gas natural': 6, 'Petróleo': 7, 'Carbón': 8, 'Biomasa tradicional': 9}, prefix=['anio'], suffix=[])
#  Index: 770 entries, 0 to 769
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        770 non-null    int64  
#   1   indicador   770 non-null    object 
#   2   valor       770 non-null    float64
#   3   porcentaje  770 non-null    float64
#  
#  |    |   anio | indicador        |   valor |   porcentaje |
#  |---:|-------:|:-----------------|--------:|-------------:|
#  |  0 |   1800 | Otras renovables |       0 |            0 |
#  
#  ------------------------------
#  