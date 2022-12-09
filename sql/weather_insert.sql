{% set weather_data = ti.xcom_pull(task_ids='ingest_weather') %}

INSERT worksample.weather_historic(city, temperature, wind_speed, humidity, uv_index, pressure, measurement_time)  VALUES (
        'New York',
        {{ weather_data['temperature']}},
        {{ weather_data['wind_speed']}},
        {{ weather_data['humidity']}},
        {{ weather_data['uv_index']}},
        {{ weather_data['pressure']}},
        current_datetime() 
    )