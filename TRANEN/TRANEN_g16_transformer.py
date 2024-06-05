from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def filter_rows(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    df= df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)
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
replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	rename_cols(map={'tipo_energia': 'indicador', 'valor_en_mw': 'valor', 'iso3': 'geocodigo'}),
	filter_rows(condition='geocodigo == "ARG"'),
	wide_to_long(primary_keys=['anio', 'geocodigo'], value_name='valor', var_name='indicador'),
	replace_value(col='indicador', curr_value='emision_anual_co2_ton', new_value='Emisiones anuales de CO2'),
	replace_value(col='indicador', curr_value='energia_por_unidad_pib_kwh', new_value='Intensidad energética (por unidad de PIB medido en dólares de 2011 PPA)'),
	replace_value(col='indicador', curr_value='pib_per_cap_usd_ppa_2011', new_value='PIB per cápita en dólares PPA 2011'),
	replace_value(col='indicador', curr_value='poblacion', new_value='Población'),
	replace_value(col='indicador', curr_value='emision_anual_kgco2_por_kwh', new_value='Intensidad de carbono (CO2/kWh)')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 201096 entries, 0 to 201095
#  Data columns (total 7 columns):
#   #   Column                       Non-Null Count   Dtype  
#  ---  ------                       --------------   -----  
#   0   anio                         201096 non-null  int64  
#   1   iso3                         201096 non-null  object 
#   2   emision_anual_co2_ton        24157 non-null   float64
#   3   energia_por_unidad_pib_kwh   7832 non-null    float64
#   4   pib_per_cap_usd_ppa_2011     21314 non-null   float64
#   5   poblacion                    54971 non-null   float64
#   6   emision_anual_kgco2_por_kwh  9329 non-null    float64
#  
#  |    |   anio | iso3   |   emision_anual_co2_ton |   energia_por_unidad_pib_kwh |   pib_per_cap_usd_ppa_2011 |   poblacion |   emision_anual_kgco2_por_kwh |
#  |---:|-------:|:-------|------------------------:|-----------------------------:|---------------------------:|------------:|------------------------------:|
#  |  0 |   1949 | AFG    |                   14656 |                          nan |                        nan | 7.35689e+06 |                           nan |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 201096 entries, 0 to 201095
#  Data columns (total 7 columns):
#   #   Column                       Non-Null Count   Dtype  
#  ---  ------                       --------------   -----  
#   0   anio                         201096 non-null  int64  
#   1   iso3                         201096 non-null  object 
#   2   emision_anual_co2_ton        24157 non-null   float64
#   3   energia_por_unidad_pib_kwh   7832 non-null    float64
#   4   pib_per_cap_usd_ppa_2011     21314 non-null   float64
#   5   poblacion                    54971 non-null   float64
#   6   emision_anual_kgco2_por_kwh  9329 non-null    float64
#  
#  |    |   anio | iso3   |   emision_anual_co2_ton |   energia_por_unidad_pib_kwh |   pib_per_cap_usd_ppa_2011 |   poblacion |   emision_anual_kgco2_por_kwh |
#  |---:|-------:|:-------|------------------------:|-----------------------------:|---------------------------:|------------:|------------------------------:|
#  |  0 |   1949 | AFG    |                   14656 |                          nan |                        nan | 7.35689e+06 |                           nan |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'indicador', 'valor_en_mw': 'valor', 'iso3': 'geocodigo'})
#  RangeIndex: 201096 entries, 0 to 201095
#  Data columns (total 7 columns):
#   #   Column                       Non-Null Count   Dtype  
#  ---  ------                       --------------   -----  
#   0   anio                         201096 non-null  int64  
#   1   geocodigo                    201096 non-null  object 
#   2   emision_anual_co2_ton        24157 non-null   float64
#   3   energia_por_unidad_pib_kwh   7832 non-null    float64
#   4   pib_per_cap_usd_ppa_2011     21314 non-null   float64
#   5   poblacion                    54971 non-null   float64
#   6   emision_anual_kgco2_por_kwh  9329 non-null    float64
#  
#  |    |   anio | geocodigo   |   emision_anual_co2_ton |   energia_por_unidad_pib_kwh |   pib_per_cap_usd_ppa_2011 |   poblacion |   emision_anual_kgco2_por_kwh |
#  |---:|-------:|:------------|------------------------:|-----------------------------:|---------------------------:|------------:|------------------------------:|
#  |  0 |   1949 | AFG         |                   14656 |                          nan |                        nan | 7.35689e+06 |                           nan |
#  
#  ------------------------------
#  
#  filter_rows(condition='geocodigo == "ARG"')
#  Index: 798 entries, 6384 to 7181
#  Data columns (total 7 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   anio                         798 non-null    int64  
#   1   geocodigo                    798 non-null    object 
#   2   emision_anual_co2_ton        136 non-null    float64
#   3   energia_por_unidad_pib_kwh   58 non-null     float64
#   4   pib_per_cap_usd_ppa_2011     153 non-null    float64
#   5   poblacion                    259 non-null    float64
#   6   emision_anual_kgco2_por_kwh  58 non-null     float64
#  
#  |      |   anio | geocodigo   |   emision_anual_co2_ton |   energia_por_unidad_pib_kwh |   pib_per_cap_usd_ppa_2011 |   poblacion |   emision_anual_kgco2_por_kwh |
#  |-----:|-------:|:------------|------------------------:|-----------------------------:|---------------------------:|------------:|------------------------------:|
#  | 6384 |   1949 | ARG         |             1.53646e+07 |                          nan |                       8045 | 1.67006e+07 |                           nan |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['anio', 'geocodigo'], value_name='valor', var_name='indicador')
#  RangeIndex: 3990 entries, 0 to 3989
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       3990 non-null   int64  
#   1   geocodigo  3990 non-null   object 
#   2   indicador  3990 non-null   object 
#   3   valor      664 non-null    float64
#  
#  |    |   anio | geocodigo   | indicador             |       valor |
#  |---:|-------:|:------------|:----------------------|------------:|
#  |  0 |   1949 | ARG         | emision_anual_co2_ton | 1.53646e+07 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='emision_anual_co2_ton', new_value='Emisiones anuales de CO2')
#  RangeIndex: 3990 entries, 0 to 3989
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       3990 non-null   int64  
#   1   geocodigo  3990 non-null   object 
#   2   indicador  3990 non-null   object 
#   3   valor      664 non-null    float64
#  
#  |    |   anio | geocodigo   | indicador                |       valor |
#  |---:|-------:|:------------|:-------------------------|------------:|
#  |  0 |   1949 | ARG         | Emisiones anuales de CO2 | 1.53646e+07 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='energia_por_unidad_pib_kwh', new_value='Intensidad energética (por unidad de PIB medido en dólares de 2011 PPA)')
#  RangeIndex: 3990 entries, 0 to 3989
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       3990 non-null   int64  
#   1   geocodigo  3990 non-null   object 
#   2   indicador  3990 non-null   object 
#   3   valor      664 non-null    float64
#  
#  |    |   anio | geocodigo   | indicador                |       valor |
#  |---:|-------:|:------------|:-------------------------|------------:|
#  |  0 |   1949 | ARG         | Emisiones anuales de CO2 | 1.53646e+07 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='pib_per_cap_usd_ppa_2011', new_value='PIB per cápita en dólares PPA 2011')
#  RangeIndex: 3990 entries, 0 to 3989
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       3990 non-null   int64  
#   1   geocodigo  3990 non-null   object 
#   2   indicador  3990 non-null   object 
#   3   valor      664 non-null    float64
#  
#  |    |   anio | geocodigo   | indicador                |       valor |
#  |---:|-------:|:------------|:-------------------------|------------:|
#  |  0 |   1949 | ARG         | Emisiones anuales de CO2 | 1.53646e+07 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='poblacion', new_value='Población')
#  RangeIndex: 3990 entries, 0 to 3989
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       3990 non-null   int64  
#   1   geocodigo  3990 non-null   object 
#   2   indicador  3990 non-null   object 
#   3   valor      664 non-null    float64
#  
#  |    |   anio | geocodigo   | indicador                |       valor |
#  |---:|-------:|:------------|:-------------------------|------------:|
#  |  0 |   1949 | ARG         | Emisiones anuales de CO2 | 1.53646e+07 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='emision_anual_kgco2_por_kwh', new_value='Intensidad de carbono (CO2/kWh)')
#  RangeIndex: 3990 entries, 0 to 3989
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       3990 non-null   int64  
#   1   geocodigo  3990 non-null   object 
#   2   indicador  3990 non-null   object 
#   3   valor      664 non-null    float64
#  
#  |    |   anio | geocodigo   | indicador                |       valor |
#  |---:|-------:|:------------|:-------------------------|------------:|
#  |  0 |   1949 | ARG         | Emisiones anuales de CO2 | 1.53646e+07 |
#  
#  ------------------------------
#  