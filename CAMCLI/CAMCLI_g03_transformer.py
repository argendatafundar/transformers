from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def join_geonomencladores(df: DataFrame, geonomenclador: DataFrame):
    geonomenclador = geonomenclador[['geocodigo', 'iso_2']].rename(columns={'iso_2': 'iso'})
    return df.merge(geonomenclador, on='geocodigo', how='left')
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo', 'valor_en_ton': 'valor'}),
	join_geonomencladores(geonomenclador=            geocodigo                                  name_long  \
0                 ABW                                      Aruba   
1                 AFE               África Oriental y Meridional   
2                 AFG                                 Afganistán   
3                 AFW                África Occidental y Central   
4                 AGO                                     Angola   
...               ...                                        ...   
1015  DESHUM_AHDI.SAS                 Asia del Sur (AHDI) - PNUD   
1016  DESHUM_AHDI.SSA          África Subsahariana (AHDI) - PNUD   
1017  DESHUM_AHDI.WEU            Europa Occidental (AHDI) - PNUD   
1018  DESHUM_AHDI.WLD                        Mundo (AHDI) - PNUD   
1019  DESHUM_AHDI.WOF  Ramificaciones de Occidente (AHDI) - PNUD   

                                     name_short iso_2  
0                                         Aruba    AW  
1                  África Oriental y Meridional   NaN  
2                                    Afganistán    AF  
3                   África Occidental y Central   NaN  
4                                        Angola    AO  
...                                         ...   ...  
1015                 Asia del Sur (AHDI) - PNUD   NaN  
1016          África Subsahariana (AHDI) - PNUD   NaN  
1017            Europa Occidental (AHDI) - PNUD   NaN  
1018                        Mundo (AHDI) - PNUD   NaN  
1019  Ramificaciones de Occidente (AHDI) - PNUD   NaN  

[1020 rows x 4 columns])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 23046 entries, 0 to 23045
#  Data columns (total 3 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   iso3          23046 non-null  object 
#   1   anio          23046 non-null  int64  
#   2   valor_en_ton  23046 non-null  float64
#  
#  |    | iso3   |   anio |   valor_en_ton |
#  |---:|:-------|-------:|---------------:|
#  |  0 | AFG    |   1949 |     0.00199215 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'valor_en_ton': 'valor'})
#  RangeIndex: 23046 entries, 0 to 23045
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  23046 non-null  object 
#   1   anio       23046 non-null  int64  
#   2   valor      23046 non-null  float64
#  
#  |    | geocodigo   |   anio |      valor |
#  |---:|:------------|-------:|-----------:|
#  |  0 | AFG         |   1949 | 0.00199215 |
#  
#  ------------------------------
#  
#  join_geonomencladores(geonomenclador=            geocodigo                                  name_long  \
#  0                 ABW                                      Aruba   
#  1                 AFE               África Oriental y Meridional   
#  2                 AFG                                 Afganistán   
#  3                 AFW                África Occidental y Central   
#  4                 AGO                                     Angola   
#  ...               ...                                        ...   
#  1015  DESHUM_AHDI.SAS                 Asia del Sur (AHDI) - PNUD   
#  1016  DESHUM_AHDI.SSA          África Subsahariana (AHDI) - PNUD   
#  1017  DESHUM_AHDI.WEU            Europa Occidental (AHDI) - PNUD   
#  1018  DESHUM_AHDI.WLD                        Mundo (AHDI) - PNUD   
#  1019  DESHUM_AHDI.WOF  Ramificaciones de Occidente (AHDI) - PNUD   
#  
#                                       name_short iso_2  
#  0                                         Aruba    AW  
#  1                  África Oriental y Meridional   NaN  
#  2                                    Afganistán    AF  
#  3                   África Occidental y Central   NaN  
#  4                                        Angola    AO  
#  ...                                         ...   ...  
#  1015                 Asia del Sur (AHDI) - PNUD   NaN  
#  1016          África Subsahariana (AHDI) - PNUD   NaN  
#  1017            Europa Occidental (AHDI) - PNUD   NaN  
#  1018                        Mundo (AHDI) - PNUD   NaN  
#  1019  Ramificaciones de Occidente (AHDI) - PNUD   NaN  
#  
#  [1020 rows x 4 columns])
#  RangeIndex: 23046 entries, 0 to 23045
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  23046 non-null  object 
#   1   anio       23046 non-null  int64  
#   2   valor      23046 non-null  float64
#   3   iso        22766 non-null  object 
#  
#  |    | geocodigo   |   anio |      valor | iso   |
#  |---:|:------------|-------:|-----------:|:------|
#  |  0 | AFG         |   1949 | 0.00199215 | AF    |
#  
#  ------------------------------
#  