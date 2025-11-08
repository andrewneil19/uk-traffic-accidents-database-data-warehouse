import pandas as pd
import urllib
import datetime
# Import important sqlalchemy classes
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Date, Time, ForeignKey
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy import ForeignKeyConstraint

# Define the connection strings to db
# Conn strings edited for security
conn_string_db = "Driver={ODBC Driver 17 for SQL Server};Server=your-server\\SQLEXPRESS;Database=Term_Project_Traffic_Accidents_OLTP;Trusted_Connection=yes;"
conn_string_db = urllib.parse.quote_plus(conn_string_db)
conn_string_db = "mssql+pyodbc:///?odbc_connect=%s" % conn_string_db


# Create declarative base class
class Base(DeclarativeBase):
    pass


# Define ORM models for tables
class Location(Base):
    __tablename__ = 'Location'

    # Using latitude and longitude as composite primary key
    latitude = Column(Float, primary_key=True)
    longitude = Column(Float, primary_key=True)
    location_easting_OSGR = Column(Integer)
    location_northing_OSGR = Column(Integer)
    LSOA_of_accident_location = Column(String(255))
    urban_or_rural_area = Column(String(255))
    in_Scotland = Column(String(3))
    local_authority_district = Column(String(255))
    local_authority_highway = Column(String(255))


class Road(Base):
    __tablename__ = 'Road'

    road_id = Column(Integer, primary_key=True)
    road_class = Column(String(255))
    road_number = Column(Integer)
    road_type = Column(String(255))
    speed_limit = Column(Integer)


class Junction(Base):
    __tablename__ = 'Junction'

    junction_id = Column(Integer, primary_key=True)
    junction_control = Column(String(255))
    junction_detail = Column(String(255))


class Condition(Base):
    __tablename__ = 'Condition'

    condition_id = Column(Integer, primary_key=True)
    carriageway_hazards = Column(String(255))
    light_conditions = Column(String(255))
    special_conditions_at_site = Column(String(255))
    weather_conditions = Column(String(255))
    road_surface_conditions = Column(String(255))


class VehicleMake(Base):
    __tablename__ = 'VehicleMake'

    make_id = Column(Integer, primary_key=True)
    make_name = Column(String(255))


class VehicleModel(Base):
    __tablename__ = 'VehicleModel'

    model_id = Column(Integer, primary_key=True)
    model_name = Column(String(255))


class Driver(Base):
    __tablename__ = 'Driver'

    driver_id = Column(Integer, primary_key=True)
    age_band_of_driver = Column(String(255))
    driver_home_area_type = Column(String(255))
    driver_IMD_decile = Column(Integer)
    sex = Column(String(50))
    journey_purpose = Column(String(255))


class Accident(Base):
    __tablename__ = 'Accident'

    accident_index = Column(String(255), primary_key=True)
    latitude = Column(Float)
    longitude = Column(Float)
    road_id = Column(Integer, ForeignKey('Road.road_id'))
    junction_id = Column(Integer, ForeignKey('Junction.junction_id'))
    condition_id = Column(Integer, ForeignKey('Condition.condition_id'))
    accident_date = Column(Date)
    accident_time = Column(Time)
    police_attended = Column(Integer)
    number_of_casualties = Column(Integer)
    number_of_vehicles = Column(Integer)
    pedestrian_crossing_human_control = Column(Integer)
    pedestrian_crossing_physical_facilities = Column(Integer)
    police_force = Column(String(255))

    __table_args__ = (
        ForeignKeyConstraint(
            ['latitude', 'longitude'],
            ['Location.latitude', 'Location.longitude']
        ),
    )

    # Relationships
    location = relationship("Location", foreign_keys=[latitude, longitude],
                            primaryjoin="and_(Accident.latitude==Location.latitude, "
                                        "Accident.longitude==Location.longitude)")
    road = relationship("Road")
    junction = relationship("Junction")
    condition = relationship("Condition")


class Vehicle(Base):
    __tablename__ = 'Vehicle'

    vehicle_id = Column(Integer, primary_key=True)
    accident_index = Column(String(255), ForeignKey('Accident.accident_index'))
    make_id = Column(Integer, ForeignKey('VehicleMake.make_id'))
    model_id = Column(Integer, ForeignKey('VehicleModel.model_id'))
    driver_id = Column(Integer, ForeignKey('Driver.driver_id'))
    age_of_vehicle = Column(Integer)
    propulsion_code = Column(String(255))
    vehicle_type = Column(String(255))
    engine_capacity_CC = Column(Integer)
    skidding_and_overturning = Column(String(255))
    towing_and_articulation = Column(String(255))
    vehicle_leaving_carriageway = Column(String(255))
    vehicle_location_restricted_lane = Column(Integer)
    vehicle_manoeuvre = Column(String(255))
    vehicle_reference = Column(Integer)
    vehicle_left_hand_drive = Column(String(255))
    first_point_of_impact = Column(String(255))
    hit_object_in_carriageway = Column(String(255))
    hit_object_off_carriageway = Column(String(255))
    vehicle_junction_location = Column(String(255))

    # Relationships
    accident = relationship("Accident")
    make = relationship("VehicleMake")
    model = relationship("VehicleModel")
    driver = relationship("Driver")


class TrafficAccidentDataLoader:
    def __init__(self, connection_string):
        # Create engine and session
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)

    def load_data(self, accidents_file, vehicles_file):
       # Main data loading
        print("Starting data loading process...")

        # Read CSVs
        accident_df = pd.read_csv(accidents_file)
        vehicle_df = pd.read_csv(vehicles_file)

        # Create session
        session = self.Session()

        try:
            # Load all Location records
            print("Loading Location data...")
            self._load_locations(accident_df, session)

            # Load all Road records
            print("Loading Road data...")
            self._load_roads(accident_df, session)

            # Load all Junction records
            print("Loading Junction data...")
            self._load_junctions(accident_df, session)

            # Load all Condition records
            print("Loading Condition data...")
            self._load_conditions(accident_df, session)

            # Load all VehicleMake and VehicleModel records
            print("Loading Vehicle Make/Model data...")
            self._load_vehicle_makes_models(vehicle_df, session)

            # Load all Accident records
            print("Loading Accident data...")
            self._load_accidents(accident_df, session)

            # Load all Driver and Vehicle records
            print("Loading Vehicle and Driver data...")
            self._load_vehicles_drivers(vehicle_df, session)

            # Commit all changes
            session.commit()
            print("All data successfully loaded!")

        except Exception as e:
            # Roll back in case of error
            session.rollback()
            print(f"Error during data loading: {str(e)}")
            raise
        finally:
            session.close()

    def _load_locations(self, df, session):
        # Load Location data row by row

        processed_locations = set()
        count = 0
        skipped = 0

        for _, row in df.iterrows():
            # Skip rows with NaN in critical fields (latitude, longitude)
            if pd.isna(row['Latitude']) or pd.isna(row['Longitude']):
                skipped += 1
                continue

            location_key = (row['Latitude'], row['Longitude'])

            # Skip if already processed
            if location_key in processed_locations:
                continue

            try:
                # Create a new location object
                location = Location(
                    latitude=float(row['Latitude']),
                    longitude=float(row['Longitude']),
                    location_easting_OSGR=int(float(row['Location_Easting_OSGR'])) if not pd.isna(
                        row['Location_Easting_OSGR']) else 0,
                    location_northing_OSGR=int(float(row['Location_Northing_OSGR'])) if not pd.isna(
                        row['Location_Northing_OSGR']) else 0,
                    LSOA_of_accident_location=str(row['LSOA_of_Accident_Location']) if not pd.isna(
                        row['LSOA_of_Accident_Location']) else 'Unknown',
                    urban_or_rural_area=str(row['Urban_or_Rural_Area']) if not pd.isna(
                        row['Urban_or_Rural_Area']) else 'Unknown',
                    in_Scotland=str(row.get('InScotland', 'No')) if not pd.isna(row.get('InScotland')) else 'No',
                    local_authority_district=str(row['Local_Authority_(District)']) if not pd.isna(
                        row['Local_Authority_(District)']) else 'Unknown',
                    local_authority_highway=str(row['Local_Authority_(Highway)']) if not pd.isna(
                        row['Local_Authority_(Highway)']) else 'Unknown'
                )

                # Add one location at a time
                session.add(location)
                count += 1
                session.flush()
                processed_locations.add(location_key)

            except Exception as e:
                # Log the problematic row
                print(f"Error processing location: Lat={row['Latitude']}, Long={row['Longitude']}")
                print(f"Error details: {str(e)}")
                session.rollback()

        print(f"Successfully loaded {count} unique locations (skipped {skipped} incomplete records)")

    def _load_roads(self, df, session):
         # Get unique combinations of road attributes
        road_attributes = df[['1st_Road_Class', '1st_Road_Number', 'Road_Type', 'Speed_limit']].drop_duplicates()

        count = 0
        skipped = 0

        for _, row in road_attributes.iterrows():
            # Skip rows with NaN in required fields
            if pd.isna(row['1st_Road_Class']) or pd.isna(row['Road_Type']):
                skipped += 1
                continue

            try:
                # Handle NaN values for numeric fields
                road_number = 0
                if not pd.isna(row['1st_Road_Number']):
                    road_number = int(float(row['1st_Road_Number']))

                speed_limit = None
                if not pd.isna(row['Speed_limit']):
                    speed_limit = int(float(row['Speed_limit']))

                # Create road object
                road = Road(
                    road_class=str(row['1st_Road_Class']),
                    road_number=road_number,
                    road_type=str(row['Road_Type']),
                    speed_limit=speed_limit
                )

                session.add(road)
                count += 1
                session.flush()


            except Exception as e:
                print(f"Error processing road: Class={row['1st_Road_Class']}, Number={row['1st_Road_Number']}")
                print(f"Error details: {str(e)}")
                session.rollback()

        print(f"Successfully loaded {count} unique roads (skipped {skipped} incomplete records)")

    def _load_junctions(self, df, session):
        junction_attributes = df[['Junction_Control', 'Junction_Detail']].drop_duplicates()

        count = 0

        for _, row in junction_attributes.iterrows():
            junction = Junction(
                junction_control=row['Junction_Control'],
                junction_detail=row['Junction_Detail']
            )
            session.add(junction)
            count += 1

        session.flush()

        print(f"Successfully loaded {count} unique junctions")

    def _load_conditions(self, df, session):
        """Load Condition data row by row with NaN handling"""
        # Get unique combinations of condition attributes
        condition_attributes = df[['Weather_Conditions', 'Road_Surface_Conditions',
                                   'Light_Conditions', 'Special_Conditions_at_Site',
                                   'Carriageway_Hazards']].drop_duplicates()

        count = 0
        skipped = 0

        for _, row in condition_attributes.iterrows():
            # Skip rows with NaN in required fields
            if (pd.isna(row['Weather_Conditions']) or
                    pd.isna(row['Road_Surface_Conditions']) or
                    pd.isna(row['Light_Conditions']) or
                    pd.isna(row['Special_Conditions_at_Site'])):
                skipped += 1
                continue

            try:
                # Create condition object
                condition = Condition(
                    weather_conditions=str(row['Weather_Conditions']),
                    road_surface_conditions=str(row['Road_Surface_Conditions']),
                    light_conditions=str(row['Light_Conditions']),
                    special_conditions_at_site=str(row['Special_Conditions_at_Site']),
                    carriageway_hazards=str(row['Carriageway_Hazards']) if not pd.isna(
                        row['Carriageway_Hazards']) else None
                )

                session.add(condition)
                count += 1
                session.flush()


            except Exception as e:

                print(
                    f"Error processing condition: Weather={row['Weather_Conditions']}, Surface={row['Road_Surface_Conditions']}")
                print(f"Error details: {str(e)}")
                session.rollback()

        print(f"Successfully loaded {count} unique conditions (skipped {skipped} incomplete records)")

    def _load_vehicle_makes_models(self, df, session):
        # Get unique vehicle makes
        make_series = df['make'].dropna().unique()

        count_make = 0
        count_model = 0

        for make_name in make_series:
            make = VehicleMake(make_name=make_name)
            session.add(make)
            count_make += 1

        # Get unique vehicle models
        model_series = df['model'].dropna().unique()
        for model_name in model_series:
            model = VehicleModel(model_name=model_name)
            session.add(model)
            count_model += 1


        session.flush()
        print(f"Successfully loaded {count_make} unique makes and {count_model} unique models")

    def _load_accidents(self, df, session):
        count = 0
        skipped = 0

        # Create a default condition if needed
        default_condition = None

        for _, row in df.iterrows():
            # Skip rows with missing essential data
            if (pd.isna(row['Accident_Index']) or pd.isna(row['Latitude']) or
                    pd.isna(row['Longitude']) or pd.isna(row['Date'])):
                skipped += 1
                continue

            try:
                with session.no_autoflush:
                    # Find related records
                    road = None
                    junction = None
                    condition = None

                    try:
                        road = session.query(Road).filter_by(
                            road_class=str(row['1st_Road_Class']),
                            road_number=int(float(row['1st_Road_Number'])) if not pd.isna(row['1st_Road_Number']) else 0
                        ).first()
                    except Exception as e:
                        print(f"Error finding road for accident {row['Accident_Index']}: {str(e)}")
                        # Get any road as fallback
                        road = session.query(Road).first()

                    try:
                        junction = session.query(Junction).filter_by(
                            junction_control=str(row['Junction_Control']),
                            junction_detail=str(row['Junction_Detail'])
                        ).first()
                    except Exception as e:
                        print(f"Error finding junction for accident {row['Accident_Index']}: {str(e)}")
                        # Get any junction as fallback
                        junction = session.query(Junction).first()

                    try:
                        condition = session.query(Condition).filter_by(
                            weather_conditions=str(row['Weather_Conditions']),
                            road_surface_conditions=str(row['Road_Surface_Conditions']),
                            light_conditions=str(row['Light_Conditions'])
                        ).first()
                    except Exception as e:
                        print(f"Error finding condition for accident {row['Accident_Index']}: {str(e)}")

                    # If no condition was found, create default one if it doesn't exist yet
                    if condition is None:
                        if default_condition is None:
                            print("Creating default condition for missing conditions")
                            default_condition = Condition(
                                weather_conditions="Unknown",
                                road_surface_conditions="Unknown",
                                light_conditions="Unknown",
                                special_conditions_at_site="Unknown",
                                carriageway_hazards="Unknown"
                            )
                            session.add(default_condition)
                            session.flush()  # Get ID for the default condition

                        condition = default_condition

                    # If  couldn't find related records, skip this accident
                    if not road or not junction or not condition:
                        print(f"Skipping accident {row['Accident_Index']} due to missing related records")
                        skipped += 1
                        continue

                    # Parse date and time
                    accident_date = pd.to_datetime(row['Date']).date()

                    # Handle time
                    accident_time = datetime.time(0, 0)  # Default
                    if 'Time' in row and not pd.isna(row['Time']):
                        time_str = row['Time']
                        try:
                            accident_time = pd.to_datetime(time_str, format='%H:%M').time()
                        except:
                            try:
                                accident_time = pd.to_datetime(time_str).time()
                            except:
                                print(f"Could not parse time: {time_str}")

                    # Create accident with handling of NaN
                    accident = Accident(
                        accident_index=str(row['Accident_Index']),
                        latitude=float(row['Latitude']),
                        longitude=float(row['Longitude']),
                        road_id=road.road_id,
                        junction_id=junction.junction_id,
                        condition_id=condition.condition_id,
                        accident_date=accident_date,
                        accident_time=accident_time,
                        police_attended=int(float(row['Did_Police_Officer_Attend_Scene_of_Accident'])) if not pd.isna(
                            row.get('Did_Police_Officer_Attend_Scene_of_Accident')) else None,
                        number_of_casualties=int(float(row.get('Number_of_Casualties', 0))) if not pd.isna(
                            row.get('Number_of_Casualties')) else 0,
                        number_of_vehicles=int(float(row.get('Number_of_Vehicles', 0))) if not pd.isna(
                            row.get('Number_of_Vehicles')) else 0,
                        pedestrian_crossing_human_control=int(
                            float(row.get('Pedestrian_Crossing-Human_Control'))) if not pd.isna(
                            row.get('Pedestrian_Crossing-Human_Control')) else None,
                        pedestrian_crossing_physical_facilities=int(
                            float(row.get('Pedestrian_Crossing-Physical_Facilities'))) if not pd.isna(
                            row.get('Pedestrian_Crossing-Physical_Facilities')) else None,
                        police_force=str(row.get('Police_Force')) if not pd.isna(row.get('Police_Force')) else None
                    )

                    session.add(accident)
                    session.flush()
                    count += 1


            except Exception as e:
                print(f"Error processing accident {row['Accident_Index']}: {str(e)}")
                session.rollback()

        print(f"Successfully loaded {count} accidents (skipped {skipped} incomplete records)")

    def _load_vehicles_drivers(self, df, session):
        count_driver = 0
        count_vehicle = 0

        for _, row in df.iterrows():
            try:
                # Skip rows with missing make/model or accident_index
                if pd.isna(row.get('make')) or pd.isna(row.get('model')) or pd.isna(row.get('Accident_Index')):
                    continue

                # Check if accident exists
                accident_index = str(row['Accident_Index'])

                with session.no_autoflush:
                    accident = session.get(Accident, accident_index)

                    if not accident:
                        print(f"Warning: No accident found for vehicle with Accident_Index {accident_index}")
                        continue

                    # Create driver first with NaN handling
                    driver = Driver(
                        age_band_of_driver=str(row.get('Age_Band_of_Driver', 'Unknown')) if not pd.isna(
                            row.get('Age_Band_of_Driver')) else 'Unknown',
                        driver_home_area_type=str(row.get('Driver_Home_Area_Type', 'Unknown')) if not pd.isna(
                            row.get('Driver_Home_Area_Type')) else 'Unknown',
                        driver_IMD_decile=int(float(row.get('Driver_IMD_Decile'))) if not pd.isna(
                            row.get('Driver_IMD_Decile')) else None,
                        sex=str(row.get('Sex_of_Driver', 'Unknown')) if not pd.isna(
                            row.get('Sex_of_Driver')) else 'Unknown',
                        journey_purpose=str(row.get('Journey_Purpose_of_Driver', 'Unknown')) if not pd.isna(
                            row.get('Journey_Purpose_of_Driver')) else 'Unknown'
                    )
                    session.add(driver)
                    count_driver += 1
                    session.flush()  # Need this to get the driver_id

                    # Find the vehicle make
                    make = session.query(VehicleMake).filter_by(
                        make_name=str(row['make'])
                    ).first()

                    # Find the vehicle model
                    model = session.query(VehicleModel).filter_by(
                        model_name=str(row['model'])
                    ).first()

                    # Handle numeric values
                    age_of_vehicle = int(float(row.get('Age_of_Vehicle'))) if not pd.isna(
                        row.get('Age_of_Vehicle')) else None
                    engine_capacity = int(float(row.get('Engine_Capacity_.CC.'))) if not pd.isna(
                        row.get('Engine_Capacity_.CC.')) else None
                    vehicle_location = int(float(row.get('Vehicle_Location.Restricted_Lane'))) if not pd.isna(
                        row.get('Vehicle_Location.Restricted_Lane')) else None
                    vehicle_reference = int(float(row.get('Vehicle_Reference', 0))) if not pd.isna(
                        row.get('Vehicle_Reference')) else 0


                    vehicle = Vehicle(
                        accident_index=accident_index,
                        make_id=make.make_id if make else None,
                        model_id=model.model_id if model else None,
                        driver_id=driver.driver_id,
                        age_of_vehicle=age_of_vehicle,
                        propulsion_code=str(row.get('Propulsion_Code')) if not pd.isna(
                            row.get('Propulsion_Code')) else None,
                        vehicle_type=str(row.get('Vehicle_Type', 'Unknown')) if not pd.isna(
                            row.get('Vehicle_Type')) else 'Unknown',
                        engine_capacity_CC=engine_capacity,
                        skidding_and_overturning=str(row.get('Skidding_and_Overturning')) if not pd.isna(
                            row.get('Skidding_and_Overturning')) else None,
                        towing_and_articulation=str(row.get('Towing_and_Articulation', 'Unknown')) if not pd.isna(
                            row.get('Towing_and_Articulation')) else 'Unknown',
                        vehicle_leaving_carriageway=str(
                            row.get('Vehicle_Leaving_Carriageway', 'Unknown')) if not pd.isna(
                            row.get('Vehicle_Leaving_Carriageway')) else 'Unknown',
                        vehicle_location_restricted_lane=vehicle_location,
                        vehicle_manoeuvre=str(row.get('Vehicle_Manoeuvre', 'Unknown')) if not pd.isna(
                            row.get('Vehicle_Manoeuvre')) else 'Unknown',
                        vehicle_reference=vehicle_reference,
                        vehicle_left_hand_drive=str(row.get('Was_Vehicle_Left_Hand_Drive', 'Unknown')) if not pd.isna(
                            row.get('Was_Vehicle_Left_Hand_Drive')) else 'Unknown',
                        first_point_of_impact=str(row.get('X1st_Point_of_Impact', 'Unknown')) if not pd.isna(
                            row.get('X1st_Point_of_Impact')) else 'Unknown',
                        hit_object_in_carriageway=str(row.get('Hit_Object_in_Carriageway')) if not pd.isna(
                            row.get('Hit_Object_in_Carriageway')) else None,
                        hit_object_off_carriageway=str(row.get('Hit_Object_off_Carriageway')) if not pd.isna(
                            row.get('Hit_Object_off_Carriageway')) else None,
                        vehicle_junction_location=str(row.get('Junction_Location', 'Unknown')) if not pd.isna(
                            row.get('Junction_Location')) else 'Unknown'
                    )

                    session.add(vehicle)
                    count_vehicle += 1

                    session.flush()


            except Exception as e:
                print(f"Error processing vehicle for accident {row.get('Accident_Index')}: {str(e)}")
                session.rollback()

        print(f"Successfully loaded {count_driver} drivers and {count_vehicle} vehicles")

################################
# Run it all here!

accident_file = 'ProjectDataFiles/Accidents_extract.csv'
vehicle_file = 'ProjectDataFiles/Vehicles_extract.csv'

# Initialize data loader
loader = TrafficAccidentDataLoader(conn_string_db)

# Load data
loader.load_data(accident_file, vehicle_file)
