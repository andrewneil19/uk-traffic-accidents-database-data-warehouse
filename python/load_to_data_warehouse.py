import pandas as pd
import urllib
import datetime
# Import important sqlalchemy classes
from sqlalchemy import create_engine, Column, Integer, String, Date, Time, Float, ForeignKey
from sqlalchemy.orm import DeclarativeBase, sessionmaker, relationship

# Define connection strings
conn_string_db = "Driver={ODBC Driver 17 for SQL Server};Server=andy-neil-forem\\SQLEXPRESS;Database=Term_Project_Traffic_Accidents_OLTP;Trusted_Connection=yes;"
conn_string_db = urllib.parse.quote_plus(conn_string_db)
conn_string_db = "mssql+pyodbc:///?odbc_connect=%s" % conn_string_db
conn_string_dw = "Driver={ODBC Driver 17 for SQL Server};Server=andy-neil-forem\\SQLEXPRESS;Database=Term_Project_Traffic_Accidents_OLAP_DW;Trusted_Connection=yes;"
conn_string_dw = urllib.parse.quote_plus(conn_string_dw)
conn_string_dw = "mssql+pyodbc:///?odbc_connect=%s" % conn_string_dw


# Define base class for all ORM models
class Base(DeclarativeBase):
    pass


# Define all ORM classes at module level
class DimDate(Base):
    __tablename__ = 'DimDate'
    date_key = Column(Integer, primary_key=True, autoincrement=False)
    date = Column(Date)
    year = Column(Integer)
    month = Column(Integer)
    month_name = Column(String)
    week = Column(Integer)
    week_day = Column(Integer)
    week_day_name = Column(String)
    day_number = Column(Integer)


class DimLocation(Base):
    __tablename__ = 'DimLocation'
    location_id = Column(Integer, primary_key=True, autoincrement=True)
    location_easting_OSGR = Column(Integer)
    location_northing_OSGR = Column(Integer)
    LSOA = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    urban_rural_area = Column(String)
    local_authority_district = Column(String)
    local_authority_highway = Column(String)
    in_Scotland = Column(String)


class DimCondition(Base):
    __tablename__ = 'DimCondition'
    condition_id = Column(Integer, primary_key=True, autoincrement=True)   # surrogate key
    src_condition_id = Column(Integer)   #natural key
    weather_conditions = Column(String)
    road_surface_conditions = Column(String)
    light_conditions = Column(String)
    carriageway_hazards = Column(String)
    special_conditions = Column(String)


class DimRoad(Base):
    __tablename__ = 'DimRoad'
    road_id = Column(Integer, primary_key=True, autoincrement=True)   #SK
    src_road_id = Column(Integer)  #NK
    road_class = Column(String)
    road_number = Column(Integer)
    road_type = Column(String)
    speed_limit = Column(Integer)
    junction_control = Column(String)
    junction_detail = Column(String)


class DimAccidentDetail(Base):
    __tablename__ = 'DimAccidentDetail'
    accident_detail_id = Column(Integer, primary_key=True, autoincrement=True)
    accident_index = Column(String)
    police_attended = Column(Integer)
    police_force = Column(String)
    ped_crossing_human_control = Column(String)
    ped_crossing_physical_facilities = Column(String)


class FactAccident(Base):
    __tablename__ = 'FactAccident'
    fact_accident_id = Column(Integer, primary_key=True, autoincrement=True)
    accident_detail_id = Column(Integer, ForeignKey('DimAccidentDetail.accident_detail_id'))
    date_key = Column(Integer, ForeignKey('DimDate.date_key'))
    road_id = Column(Integer, ForeignKey('DimRoad.road_id'))
    condition_id = Column(Integer, ForeignKey('DimCondition.condition_id'))
    location_id = Column(Integer, ForeignKey('DimLocation.location_id'))
    accident_date = Column(Date)
    accident_time = Column(Time)
    number_of_casualties = Column(Integer)
    number_of_vehicles = Column(Integer)

    # Relationships addition
    accident_detail = relationship("DimAccidentDetail")
    date = relationship("DimDate")
    road = relationship("DimRoad")
    condition = relationship("DimCondition")
    location = relationship("DimLocation")

class DimDriver(Base):
    __tablename__ = "DimDriver"
    driver_id = Column(Integer, primary_key=True, autoincrement=True)
    src_driver_id = Column(Integer)
    age_band_of_driver = Column(String)
    driver_home_area_type = Column(String)
    driver_IMD_decile = Column(Integer)
    sex = Column(String)
    journey_purpose = Column(String)

class DimVehicleDetail(Base):
    __tablename__ = "DimVehicleDetail"
    vehicle_detail_id = Column(Integer, primary_key=True, autoincrement=True)
    src_vehicle_id = Column(Integer)
    make_name = Column(String)
    model_name = Column(String)
    propulsion_code = Column(String)
    vehicle_type = Column(String)
    skidding_and_overturning = Column(String)
    towing_and_articulation = Column(String)
    vehicle_leaving_carriageway = Column(String)
    vehicle_location_restricted_lane = Column(Integer)
    vehicle_manoeuvre = Column(String)
    vehicle_reference = Column(Integer)
    vehicle_left_hand_drive = Column(String)
    first_point_of_impact = Column(String)
    hit_object_in_carriageway = Column(String)
    hit_object_off_carriageway = Column(String)
    vehicle_junction_location = Column(String)


class FactVehicle(Base):
    __tablename__ = 'FactVehicle'
    fact_vehicle_id = Column(Integer, primary_key=True, autoincrement=True)
    accident_detail_id = Column(Integer, ForeignKey('DimAccidentDetail.accident_detail_id'))
    driver_id = Column(Integer, ForeignKey('DimDriver.driver_id'))
    vehicle_detail_id = Column(Integer, ForeignKey('DimVehicleDetail.vehicle_detail_id'))
    age_of_vehicle = Column(Integer)
    engine_capacity_CC = Column(Integer)

    # Optional: Add relationships
    accident_detail = relationship("DimAccidentDetail")
    driver = relationship("DimDriver")
    vehicle_detail = relationship("DimVehicleDetail")


# Date functions needed for DimDate and FactAccident
def create_Date_Key(inDate):
    in_Year = inDate.year
    in_Month = inDate.strftime("%m")
    in_DayNumber = inDate.strftime("%d")
    out_Date_Key = int(str(in_Year) + str(in_Month) + str(in_DayNumber))
    return out_Date_Key


def addNew_Date(chkDate, conStrdw):
    engine_dw = create_engine(conStrdw)

    sqlQuery = 'SELECT * FROM DimDate'
    dFrame = pd.read_sql_query(sqlQuery, engine_dw)

    chk_Date = chkDate.strftime("%Y-%m-%d")
    chk_Year = chkDate.year
    chk_Month = chkDate.strftime("%m")
    chk_MonthName = chkDate.strftime('%B')
    chk_Week = int(chkDate.strftime('%U')) + 1
    chk_WeekDay = chkDate.weekday() + 2
    chk_WeekDayName = chkDate.strftime('%A')
    chk_DayNumber = chkDate.strftime("%d")
    chk_Date_Key = int(str(chk_Year) + str(chk_Month) + str(chk_DayNumber))

    # Create a session
    Session = sessionmaker(bind=engine_dw)
    session = Session()

    # Check if the date key exists in the dataframe
    if chk_Date_Key not in dFrame['date_key'].values:
        # If primary key does not exist, create new record
        new_record = DimDate(
            date_key=chk_Date_Key,
            date=chk_Date,
            year=chk_Year,
            month=chk_Month,
            month_name=chk_MonthName,
            week=chk_Week,
            week_day=chk_WeekDay,
            week_day_name=chk_WeekDayName,
            day_number=chk_DayNumber
        )
        session.add(new_record)
        session.commit()

        with open('DW_log.txt', 'a') as f:
            dt = datetime.datetime.now()
            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            out_str = f'TimeStamp: {dt_str} --- Added date {chk_Date} with key {chk_Date_Key} to DimDate table \n'
            f.write(out_str)

    '''
    # Skipping this part - way too many records in dataset so a ton of writes to DW_log.txt
    # were occurring
    else:
        # Log skipped date to file
        with open('DW_log.txt', 'a') as f:
            dt = datetime.datetime.now()
            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            out_str = f'TimeStamp: {dt_str} --- Skipping duplicate date key: {chk_Date_Key} \n'
            f.write(out_str)
    '''
    session.close()


# ETL functions
def pipe_Location(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)
    sqlQuery = 'SELECT * FROM Location'
    dFrame = pd.read_sql_query(sqlQuery, engine_db)

    # Create session
    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0

    for _, row in dFrame.iterrows():
        # Check if record exists - lat/long was source key
        if not session.query(DimLocation).filter_by(
                latitude=row['latitude'],
                longitude=row['longitude']
        ).first():
            # Create new record
            new_record = DimLocation(
                location_easting_OSGR=row['location_easting_OSGR'],
                location_northing_OSGR=row['location_northing_OSGR'],
                LSOA=row['LSOA_of_accident_location'],
                latitude=row['latitude'],
                longitude=row['longitude'],
                urban_rural_area=row['urban_or_rural_area'],
                local_authority_district=row['local_authority_district'],
                local_authority_highway=row['local_authority_highway'],
                in_Scotland=row['in_Scotland']
            )
            session.add(new_record)
            counter += 1
        else:
            print(f"Skipping duplicate location: lat={row['latitude']}, long={row['longitude']}")

    session.commit()
    session.close()

    # Log results
    with open('DW_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = f'TimeStamp: {dt_str} --- Number of new location records loaded into DimLocation = {counter} \n'
        f.write(out_str)

#######
def pipe_Condition(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)
    sqlQuery = 'SELECT * FROM Condition'
    dFrame = pd.read_sql_query(sqlQuery, engine_db)

    # Create session
    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0

    for _, row in dFrame.iterrows():
        if not session.query(DimCondition).filter_by(src_condition_id=row['condition_id']).first():
            new_record = DimCondition(
                src_condition_id=row['condition_id'],
                weather_conditions=row['weather_conditions'],
                road_surface_conditions=row['road_surface_conditions'],
                light_conditions=row['light_conditions'],
                carriageway_hazards=row['carriageway_hazards'],
                special_conditions=row['special_conditions_at_site']
            )
            session.add(new_record)
            counter += 1
        else:
            print(f"Skipping duplicate condition: {row['condition_id']}")

    session.commit()
    session.close()

    # Log results
    with open('DW_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = f'TimeStamp: {dt_str} --- Number of new condition records loaded into DimCondition = {counter} \n'
        f.write(out_str)

#######
def pipe_Road(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)
    sqlQuery = (
        "SELECT DISTINCT r.road_id, r.road_class, r.road_number, r.road_type, r.speed_limit, "
        "j.junction_control, j.junction_detail "
        "FROM Road r "
        "JOIN Accident a ON r.road_id = a.road_id "
        "JOIN Junction j ON a.junction_id = j.junction_id"
    )
    dFrame = pd.read_sql_query(sqlQuery, engine_db)

    # Create session
    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0

    for _, row in dFrame.iterrows():
        # Check if combination exists
        existing = session.query(DimRoad).filter_by(
            src_road_id=row['road_id'],
            junction_control=row['junction_control'],
            junction_detail=row['junction_detail']
        ).first()

        if not existing:
            new_record = DimRoad(
                src_road_id=row['road_id'],
                road_class=row['road_class'],
                road_number=row['road_number'],
                road_type=row['road_type'],
                speed_limit=row['speed_limit'],
                junction_control=row['junction_control'],
                junction_detail=row['junction_detail']
            )
            session.add(new_record)
            counter += 1
        else:
            print("Skipping duplicate road-junction")

    session.commit()
    session.close()

    # Log results
    with open('DW_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = f'TimeStamp: {dt_str} --- Number of new road-junction records loaded into DimRoad = {counter} \n'
        f.write(out_str)

#######
def pipe_Accident_Detail(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)
    sqlQuery = (
        "SELECT accident_index, police_attended, pedestrian_crossing_human_control, "
        "pedestrian_crossing_physical_facilities, police_force "
        "FROM Accident"
    )
    dFrame = pd.read_sql_query(sqlQuery, engine_db)

    # Create session
    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0
    message = ""

    for _, row in dFrame.iterrows():
        if not session.query(DimAccidentDetail).filter_by(accident_index=row['accident_index']).first():
            new_record = DimAccidentDetail(
                accident_index=row['accident_index'],
                police_attended=row['police_attended'],
                police_force=row['police_force'],
                # Get errors if don't handle nulls below
                ped_crossing_human_control=int(row['pedestrian_crossing_human_control']) if pd.notnull(
                    row['pedestrian_crossing_human_control']) else None,
                ped_crossing_physical_facilities=row['pedestrian_crossing_physical_facilities'] if pd.notnull(
                    row['pedestrian_crossing_physical_facilities']) else None
            )
            session.add(new_record)
            # example of catching invalid row
            try:
                session.commit()
                counter += 1
            except Exception as e:
                session.rollback()
                message = "Error adding/processing one or more accident_index"
        else:
            print(f"Skipping duplicate accident record: index={row['accident_index']}")

    session.commit()
    session.close()

    # Log results
    with open('DW_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = message + '\n' + f'TimeStamp: {dt_str} --- Number of new accident-detail records loaded into DimAccidentDetail = {counter} \n'
        f.write(out_str)

#######
def pipe_Fact_Accident(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)
    sqlQuery = (
        "SELECT accident_index, road_id, condition_id, accident_date, "
        "accident_time, number_of_casualties, number_of_vehicles, "
        "latitude, longitude "
        "FROM Accident"
    )
    dFrame = pd.read_sql_query(sqlQuery, engine_db)

    # Create session
    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0

    for _, row in dFrame.iterrows():
        # Add the date to DimDate if it doesn't exist
        addNew_Date(row['accident_date'], conStrdw)
        date_key = create_Date_Key(row['accident_date'])

        # Get dimension key for accident detail
        accident_detail = session.query(DimAccidentDetail).filter_by(accident_index=row['accident_index']).first()
        if not accident_detail:
            print(f"No accident detail found for index: {row['accident_index']}")
            continue

        # Get dimension key for road
        road_record = session.query(DimRoad).filter_by(src_road_id=row['road_id']).first()
        if not road_record:
            print(f"No road record found for id: {row['road_id']}")
            continue

        # Get dimension key for condition
        condition_record = session.query(DimCondition).filter_by(src_condition_id=row['condition_id']).first()
        if not condition_record:
            print(f"No condition record found for id: {row['condition_id']}")
            continue

        # Get dimension key for location
        location_record = session.query(DimLocation).filter_by(
            latitude=row['latitude'],
            longitude=row['longitude']
        ).first()
        if not location_record:
            print("No location record found for lat/long")
            continue

        # Create new fact record
        new_record = FactAccident(
            accident_detail_id=accident_detail.accident_detail_id,
            date_key=date_key,
            road_id=road_record.road_id,
            condition_id=condition_record.condition_id,
            location_id=location_record.location_id,
            accident_date=row['accident_date'],
            accident_time=row['accident_time'],
            number_of_casualties=row['number_of_casualties'],
            number_of_vehicles=row['number_of_vehicles']
        )
        session.add(new_record)
        counter += 1

    session.commit()
    session.close()

    # Log results
    with open('DW_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = f'TimeStamp: {dt_str} --- Number of new records loaded into FactAccident = {counter} \n'
        f.write(out_str)

#######
def pipe_Driver(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)
    sqlQuery = "SELECT * FROM Driver"

    dFrame = pd.read_sql_query(sqlQuery, engine_db)

    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0
    message = ""

    for _, row in dFrame.iterrows():
        # Check if exists
        existing = session.query(DimDriver).filter_by(
            src_driver_id=row['driver_id']
        ).first()

        if not existing:
            # Handle NaN in driver_IMD_decile
            imd_decile = None
            if pd.notnull(row['driver_IMD_decile']):
                imd_decile = int(row['driver_IMD_decile'])

            new_record = DimDriver(
                src_driver_id=row['driver_id'],
                age_band_of_driver=row['age_band_of_driver'],
                driver_home_area_type=row['driver_home_area_type'],
                driver_IMD_decile=imd_decile,  # Use None for NaN
                sex=row['sex'],
                journey_purpose=row['journey_purpose'],
            )
            try:
                session.add(new_record)
                session.commit()
                counter += 1
            except Exception as e:
                session.rollback()
                message += f"Error adding driver ID {row['driver_id']}: {str(e)}\n"

        else:
            print(f"Skipping duplicate driver: driver_id={row['driver_id']}")

    session.close()

    # Log results
    with open('DW_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        if message:
            f.write(message + '\n')
        out_str = f'TimeStamp: {dt_str} --- Number of new driver records loaded into DimDriver = {counter} \n'
        f.write(out_str)

#######
def pipe_Vehicle_Detail(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)

    sqlQuery = (
        "SELECT v.vehicle_id, v.accident_index, mk.make_name, md.model_name, " 
        "v.propulsion_code, v.vehicle_type, v.skidding_and_overturning, "
        "v.towing_and_articulation, v.vehicle_leaving_carriageway, "
        "v.vehicle_location_restricted_lane, v.vehicle_manoeuvre, " 
        "v.vehicle_reference, v.vehicle_left_hand_drive, "
        "v.first_point_of_impact, v.hit_object_in_carriageway, "
        "v.hit_object_off_carriageway, v.vehicle_junction_location "
        "FROM Vehicle v "
        "LEFT JOIN VehicleMake mk ON v.make_id = mk.make_id "
        "LEFT JOIN VehicleModel md ON v.model_id = md.model_id"
    )
    dFrame = pd.read_sql_query(sqlQuery, engine_db)

    # Create session
    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0
    message = ""

    for _, row in dFrame.iterrows():
        # Check if vehicle ID already exists
        if not session.query(DimVehicleDetail).filter_by(src_vehicle_id=row['vehicle_id']).first():
            # Create new record
            new_record = DimVehicleDetail(
                src_vehicle_id=row['vehicle_id'],
                make_name=row['make_name'] if pd.notnull(row['make_name']) else None,
                model_name=row['model_name'] if pd.notnull(row['model_name']) else None,
                propulsion_code=row['propulsion_code'] if pd.notnull(row['propulsion_code']) else None,
                vehicle_type=row['vehicle_type'],
                skidding_and_overturning=row['skidding_and_overturning'] if pd.notnull(
                    row['skidding_and_overturning']) else None,
                towing_and_articulation=row['towing_and_articulation'],
                vehicle_leaving_carriageway=row['vehicle_leaving_carriageway'],
                vehicle_location_restricted_lane=row['vehicle_location_restricted_lane'] if pd.notnull(
                    row['vehicle_location_restricted_lane']) else None,
                vehicle_manoeuvre=row['vehicle_manoeuvre'],
                vehicle_reference=row['vehicle_reference'],
                vehicle_left_hand_drive=row['vehicle_left_hand_drive'],
                first_point_of_impact=row['first_point_of_impact'],
                hit_object_in_carriageway=row['hit_object_in_carriageway'] if pd.notnull(
                    row['hit_object_in_carriageway']) else None,
                hit_object_off_carriageway=row['hit_object_off_carriageway'] if pd.notnull(
                    row['hit_object_off_carriageway']) else None,
                vehicle_junction_location=row['vehicle_junction_location']
            )
            session.add(new_record)
            counter += 1

        else:
            message += f"Skipping duplicate vehicle detail: ID={row['vehicle_id']}"


    session.commit()
    session.close()


    with open('DW_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        if message:
            f.write(message + '\n')
        out_str = f'TimeStamp: {dt_str} --- Number of new vehicle detail records loaded to DimVehicleDetail = {counter} \n'
        f.write(out_str)

#######

def pipe_Fact_Vehicle(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)

    # Simplified query to get the fact data
    sqlQuery = (
        "SELECT v.vehicle_id, a.accident_index, v.driver_id, v.age_of_vehicle, v.engine_capacity_CC "
        "FROM Vehicle v "
        "JOIN Accident a ON v.accident_index = a.accident_index"
    )
    dFrame = pd.read_sql_query(sqlQuery, engine_db)

    # Create session
    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0

    for _, row in dFrame.iterrows():
        # Get the accident_detail_id from DimAccidentDetail using accident_index
        accident_detail = session.query(DimAccidentDetail).filter_by(
            accident_index=row['accident_index']
        ).first()

        if not accident_detail:
            print(f"No accident detail found for index: {row['accident_index']}")
            continue

        # Get the driver_id from DimDriver
        driver = session.query(DimDriver).filter_by(
            src_driver_id=row['driver_id']
        ).first()

        if not driver:
            print(f"No driver found for id: {row['driver_id']}")
            continue

        vehicle_detail = session.query(DimVehicleDetail).filter_by(
            src_vehicle_id=row['vehicle_id']
        ).first()

        if not vehicle_detail:
            print(f"No vehicle detail found for id: {row['vehicle_id']}")
            continue

        # Create new record
        new_record = FactVehicle(
            accident_detail_id=accident_detail.accident_detail_id,
            driver_id=driver.driver_id,
            vehicle_detail_id=vehicle_detail.vehicle_detail_id,
            age_of_vehicle=row['age_of_vehicle'] if pd.notnull(row['age_of_vehicle']) else None,
            engine_capacity_CC=row['engine_capacity_CC'] if pd.notnull(row['engine_capacity_CC']) else None

        )
        session.add(new_record)
        counter += 1

    session.commit()
    session.close()


    with open('DW_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = f'TimeStamp: {dt_str} --- Number of new vehicle records loaded into FactVehicle = {counter} \n'
        f.write(out_str)

#####################################################################################
# Load it all here
'''pipe_Location(conn_string_db, conn_string_dw)
pipe_Condition(conn_string_db, conn_string_dw)
pipe_Road(conn_string_db, conn_string_dw)
pipe_Accident_Detail(conn_string_db, conn_string_dw)
pipe_Fact_Accident(conn_string_db, conn_string_dw)
pipe_Driver(conn_string_db, conn_string_dw)
pipe_Vehicle_Detail(conn_string_db, conn_string_dw)
'''
pipe_Fact_Vehicle(conn_string_db, conn_string_dw)
