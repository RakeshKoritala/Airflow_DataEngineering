SELECT * except (rank)
          FROM (
            SELECT
                  *,
                  ROW_NUMBER() OVER( PARTITION BY vehicle_id ORDER BY DATETIME(date,TIME(hour,minute,0)) DESC ) as rank
            FROM vehicle_analytics.history ) as latest  
    WHERE rank =1;  