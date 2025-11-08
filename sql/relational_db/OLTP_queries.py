import pandas as pd
import tabulate as tbl
import urllib

# Note sqlalchemy instead of pyodbc
from sqlalchemy import create_engine

# define connection string
conn_str = "Driver={ODBC Driver 17 for SQL Server};Server=andy-neil-forem\\SQLEXPRESS;"
conn_str = conn_str + "Database=Term_Project_Traffic_Accidents_OLTP;Trusted_Connection=yes;"
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
    # 1 - List of accidents where police had been at the scene
    query1 = (
        "SELECT * "
        "FROM Accident "
        "WHERE police_attended = 1;"
    )
    # print_query_results(query1)

    # 2 - Use the AccidentsRoads view to show accident info but only where the road speed limit
    # was greater than 50
    query2 = (
        "SELECT * FROM AccidentsRoads "
        "WHERE speed_limit > 50 "
        "AND road_class = 'Motorway' "
        "ORDER BY speed_limit DESC;"
    )
    #print_query_results(query2)

    # 3 - Show the average age of vehicles for Volvo cars, displaying the make and model
    query3 = (
        "SELECT make_name, model_name, AVG(age_of_vehicle) AS Avg_vehicle_age "
        "FROM Vehicle v "
        "JOIN VehicleModel vmodel ON v.model_id = vmodel.model_id "
        "JOIN VehicleMake vmake ON v.make_id = vmake.make_id "
        "WHERE make_name = 'Volvo' "
        "GROUP BY make_name, model_name "
        "ORDER BY make_name;"
    )
    #print_query_results(query3)

    # 4 - Using view DriverInfo, find the total count of drivers in each sex category (must have actual sex recorded)
    query4 = (
        "SELECT sex AS sex_of_driver, COUNT(*) AS count "
        "FROM DriverInfo "
        "WHERE sex in ('Male','Female') "
        "GROUP by sex;"
    )
    #print_query_results(query4)

    # 5 - Show accidents that occurred in Scotland where the number of vehicles was greater
    # than the average number of vehicles involved in all accidents
    query5 = (
        "SELECT a.accident_index, a.number_of_vehicles, l.latitude, l.longitude "
        "FROM Accident a "
        "JOIN Location l ON a.latitude = l.latitude AND a.longitude = l.longitude "
        "WHERE a.number_of_vehicles > "
        "(SELECT AVG(a2.number_of_vehicles) "
        "FROM Accident a2 "
        "JOIN Location l2 ON a2.latitude = l2.latitude AND a2.longitude = l2.longitude "
        "WHERE l2.in_Scotland = 'Yes') "
        "AND l.in_Scotland = 'Yes' "
        "ORDER BY a.number_of_vehicles DESC;"
    )
    print_query_results(query5)


if __name__ == "__main__":
    main()
    engine.dispose()  # Close the connection