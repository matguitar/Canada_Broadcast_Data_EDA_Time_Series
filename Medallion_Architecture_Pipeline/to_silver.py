from pyspark.sql.functions import col, date_format, split, hour, minute, second, to_timestamp, concat_ws, quarter
from functools import reduce

# UDF's para processar os dados

def ajuste_data(df):
    # Extraímos a parte do tempo uma única vez para evitar repetição de split
    time_part = lambda c: split(col(c), " ").getItem(1)
    
    return df.withColumns({
        # Concatena a data do log com o horário original
        "StartTime": to_timestamp(concat_ws(" ", date_format("logDate", "yyyy-MM-dd"), time_part("StartTime"))),
        "EndTime":   to_timestamp(concat_ws(" ", date_format("logDate", "yyyy-MM-dd"), time_part("EndTime"))),
        
        # Cálculo de duração convertido para segundos
        "Duration_seconds": hour(time_part("Duration")) * 3600 + 
                    minute(time_part("Duration")) * 60 + 
                    second(time_part("Duration")),
        
        # Extração de Ano, Mês, Dia e Trimestre
        "Year": date_format("LogEntryDate", "yyyy"),
        "Month": date_format("LogEntryDate", "MM"),
        "Day": date_format("LogEntryDate", "dd"),
        "Quarter": quarter(col("LogEntryDate"))
    })

@dlt.table(
    name="broadcast_silver",
    comment="Logs de broadcast com datas e durações ajustadas"
)
def data_ajustada():
    dados1 = ajuste_data(spark.table("workspace.broadcast.broadcast_logs_january_2018"))
    dados2 = ajuste_data(spark.table("workspace.broadcast.broadcast_logs_february_2018"))
    dados3 = ajuste_data(spark.table("workspace.broadcast.broadcast_logs_march_2018"))
    dados4 = ajuste_data(spark.table("workspace.broadcast.broadcast_logs_april_2018"))
    return dados1.unionByName(dados2).unionByName(dados3).unionByName(dados4)

display(data_ajustada())
