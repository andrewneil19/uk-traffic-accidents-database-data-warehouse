import pandas as pd
import tabulate as tbl
import urllib

# Note sqlalchemy instead of pyodbc
from sqlalchemy import create_engine

# define connection string
conn_str = "Driver={ODBC Driver 17 for SQL Server};Server=andy-neil-forem\\SQLEXPRESS;"
conn_str = conn_str + "Database=Term_Project_Traffic_Accidents_OLAP_DW;Trusted_Connection=yes;"
conn_str = urllib.parse.quote_plus(conn_str)
conn_str = "mssql+pyodbc:///?odbc_connect=%s" % conn_str

engine = create_engine(conn_str)

# Test connection
with engine.connect() as connection:
    print("Connected to SQL Server successfully!")


def print_query_results(myQuery):
    with engine.connect() as conn:
        df_result = pd.read_sql(myQuery, conn)
        # Create a table to hold records and column names using tabulate
        result_table = tbl.tabulate(df_result, tablefmt="grid", showindex="False")
    # Print the original query
    #print('\n' + myQuery + '\n')  # Print query
    print('\n' + result_table + '\n') # print output


def main():
    # 1 - Dice - Show accident info but only where the road speed limit was greater than 50
    # and road_class is “Motorway”.
    query1 = (
        "SELECT fa.fact_accident_id, fa.accident_date, fa.accident_time "
        "FROM FactAccident fa "
        "JOIN DimRoad dr ON fa.road_id = dr.road_id "
        "WHERE dr.speed_limit > 50 AND dr.road_class = 'Motorway'"
    )
    # print_query_results(query1)

    # 2 - Slice - Show driver info and the age of the vehicle that the driver was driving, where
    # the vehicle age was between 40 and 50 years old
    query2 = (
        "SELECT dr.*, fv.age_of_vehicle "
        "FROM DimDriver dr "
        "JOIN FactVehicle fv ON dr.driver_id = fv.driver_id "
        "WHERE fv.age_of_vehicle BETWEEN 40 AND 50 "
        "ORDER BY fv.age_of_vehicle DESC"
    )
    #print_query_results(query2)

    # 3 - Rollup - Find total accident count for urban and rural areas. Exclude unallocated area
    query3 = (
        "SELECT COALESCE(dl.urban_rural_area, 'All areas') AS 'Area Type', "
	    "COUNT(*) AS 'Accident Count' "
        "FROM FactAccident fa JOIN DimLocation dl ON fa.location_id = dl.location_id "
        "GROUP BY ROLLUP (dl.urban_rural_area) "
        "HAVING dl.urban_rural_area != 'Unallocated';"
    )
    #print_query_results(query3)

    # 4 - Drilldown - Find total accident count for urban and rural areas, but now more detailed: for each year
    # Exclude unallocated area
    query4 = (
        "SELECT dd.year, dl.urban_rural_area AS 'Area Type', "
	    "COUNT(*) AS 'Accident Count' "
        "FROM FactAccident fa JOIN DimLocation dl ON fa.location_id = dl.location_id "
        "JOIN DimDate dd on fa.date_key = dd.date_key "
        "GROUP BY dd.year, dl.urban_rural_area "
        "HAVING dl.urban_rural_area != 'Unallocated' "
        "ORDER BY year, dl.urban_rural_area; "
    )
    #print_query_results(query4)

    # 5 - Top N - Find the top 10 makes/models of cars in accidents
    query5 = (
        "SELECT TOP 10 dvd.make_name, dvd.model_name, COUNT(*) AS 'Accident Count' "
        "FROM FactVehicle fv JOIN DimAccidentDetail dad ON fv.accident_detail_id = dad.accident_detail_id "
        "JOIN DimVehicleDetail dvd ON fv.vehicle_detail_id = dvd.vehicle_detail_id "
        "GROUP BY dvd.make_name, dvd.model_name "
        "ORDER BY COUNT(*) DESC;"
    )
    print_query_results(query5)


if __name__ == "__main__":
    main()
    engine.dispose()  # Close the connection