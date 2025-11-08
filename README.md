# UK Traffic Accidents Analytics Engineering Project

A comprehensive analytics engineering project demonstrating dimensional modeling, data warehouse design, ETL pipeline development, and multi-database implementations using UK traffic accident data.

## 📊 Project Overview

This project showcases end-to-end analytics engineering and data modeling skills through the design and implementation of both a normalized relational database (OLTP) and a dimensional data warehouse (OLAP). The project demonstrates proficiency in dimensional modeling, ETL pipeline development, SQL optimization, and multi-paradigm database implementations (SQL and NoSQL).

## 🛠️ Technologies Used

- **Database Systems:** Microsoft SQL Server, MongoDB
- **Programming Languages:** Python, SQL, MongoDB Query Language
- **Python Libraries:** pandas, pyodbc, pymongo
- **Database Design:** Normalized schemas (3NF), Star schema (data warehouse)
- **Query Types:** OLTP queries, OLAP queries (rollup, drilldown, slice, dice)

## 🎯 Key Features

### Task 1: Relational Database Design (OLTP)
- **Normalized schema design** in Third Normal Form (3NF) with 9 interconnected tables
- **Referential integrity** enforced through foreign key relationships
- **Stored procedures** for automated table creation and management
- **Database views** for query simplification and data abstraction
- **Python ETL pipeline** using pandas and pyodbc for data ingestion and transformation

### Task 2: Data Warehouse Implementation (OLAP)
- **Dimensional modeling** using star schema architecture
- **Fact tables:** FactAccident, FactVehicle with granular metrics
- **Dimension tables:** DimDate (with calendar hierarchy), DimAccidentDetail, DimRoad, DimLocation, DimCondition, DimVehicleDetail, DimDriver
- **OLAP query capabilities:** Rollup, Drilldown, Slice, Dice, Top N operations
- **ETL transformation pipeline** from normalized OLTP to dimensional OLAP structure
- **Surrogate key management** for dimension tables

### Task 3: NoSQL Alternative Implementation
- **MongoDB document database** demonstrating polyglot persistence approach
- **Aggregation pipelines** for complex data transformations
- **Schema-flexible design** as contrast to rigid relational structure
- **Performance comparison** between SQL and NoSQL query patterns

## 📁 Project Structure

```
uk-traffic-accidents-database/
├── README.md
├── requirements.txt
├── .gitignore
├── python/
│   ├── load_to_database.py          # ETL pipeline for relational DB
│   ├── load_to_data_warehouse.py    # ETL pipeline for data warehouse
│   └── MongoDB_queries.py           # MongoDB queries
├── sql/
│   ├── relational_db/
│   │   ├── OLTP_queries.py         # Relational database queries
│   │   └── stored_procedures.sql    # Create/Drop table procedures
│   └── data_warehouse/
│       ├── OLAP_queries.py         # Data warehouse queries
│       └── dw_stored_procedures.sql # DW table management
├── docs/
│   ├── relational_db_diagram.png
│   ├── data_warehouse_diagram.png
│   └── project_documentation.pdf
└── data/
    └── README.md                    # Data source information
```

## 🗃️ Database Schema

### Relational Database (OLTP)
The normalized database includes the following tables:
- **Accident** - Core accident information with foreign keys to related tables
- **Location** - Geographic data (latitude, longitude, LSOA, local authority)
- **Road** - Road characteristics (class, type, speed limit)
- **Junction** - Junction control and detail information
- **Condition** - Environmental conditions (weather, lighting, road surface)
- **Vehicle** - Vehicle information involved in accidents
- **VehicleMake** & **VehicleModel** - Normalized vehicle make/model data
- **Driver** - Driver demographics and journey information

### Data Warehouse (OLAP)
Star schema optimized for analytical queries:
- **FactAccident** - Accident metrics (casualties, vehicles)
- **FactVehicle** - Vehicle-level details
- **DimDate** - Date dimension with full calendar hierarchy
- **DimAccidentDetail** - Accident-specific attributes
- **DimRoad** - Road and junction dimensions (consolidated)
- **DimLocation** - Geographic dimensions
- **DimCondition** - Environmental condition dimensions
- **DimVehicleDetail** - Vehicle characteristic dimensions
- **DimDriver** - Driver demographic dimensions

## 🚀 Getting Started

### Prerequisites
```bash
# Python 3.8+
# SQL Server (or SQL Server Express)
# MongoDB
```

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/uk-traffic-accidents-database.git
cd uk-traffic-accidents-database
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Set up your database connection:
   - Update connection strings in Python files
   - Create databases in SQL Server: `UKAccidents_OLTP` and `UKAccidents_OLAP`

4. Run the ETL pipelines:
```bash
# Load relational database
python python/load_to_database.py

# Load data warehouse
python python/load_to_data_warehouse.py
```

## 📈 Sample Query Demonstrations

These queries showcase different database operations and query patterns across OLTP, OLAP, and NoSQL implementations.

### OLTP Example: Filtering with Conditions
```sql
SELECT accident_index, accident_date, accident_time, 
       number_of_casualties, number_of_vehicles
FROM Accident
WHERE police_attended = 1
ORDER BY accident_date DESC;
```

### OLAP Example: Drilldown Operation (Hierarchical Aggregation)
```sql
SELECT 
    d.year,
    l.urban_rural_area,
    COUNT(*) AS accident_count
FROM FactAccident fa
JOIN DimDate d ON fa.date_key = d.date_key
JOIN DimLocation l ON fa.location_id = l.location_id
WHERE l.urban_rural_area != 'Unallocated'
GROUP BY d.year, l.urban_rural_area
ORDER BY d.year, l.urban_rural_area;
```

### MongoDB Example: Aggregation Pipeline with Group and Project Stages
```javascript
db.vehicles_extract.aggregate([
    {$match: {make: 'VOLVO'}},
    {$group: {
        _id: {make: '$make', model: '$model'},
        average_age: {$avg: '$Age_of_Vehicle'}
    }},
    {$project: {
        _id: 0,
        make: '$_id.make',
        model: '$_id.model',
        AverageAge: {$round: ['$average_age', 1]}
    }}
]);
```

## 📊 Project Highlights

- **Database Architecture:** Designed and implemented both normalized (3NF) and dimensional (star schema) data models
- **ETL Development:** Built automated Python pipelines for data extraction, transformation, and loading
- **Data Modeling:** Created 9 normalized tables with proper foreign key relationships and 9 dimensional tables with surrogate keys
- **Multi-Database Implementation:** Demonstrated proficiency across SQL Server and MongoDB
- **Query Optimization:** Implemented database views and indexes for improved query performance
- **Data Governance:** Established referential integrity constraints and data quality checks
- **Stored Procedures:** Automated database management tasks through T-SQL procedures
- **Schema Evolution:** Transformed normalized OLTP structure into optimized OLAP dimensional model

## 💡 Key Skills Demonstrated

- **Database Design:** Relational modeling with normalization (1NF, 2NF, 3NF) and dimensional modeling (star schema)
- **Data Architecture:** OLTP vs OLAP design patterns and when to apply each
- **ETL Development:** End-to-end data pipeline creation using Python, pandas, and database connectors
- **SQL Proficiency:** Advanced query writing including joins, subqueries, window functions, and stored procedures
- **OLAP Implementation:** Rollup, drilldown, slice, dice, and top-N query operations
- **NoSQL Design:** Document database implementation and aggregation pipeline development
- **Data Modeling:** Entity-relationship diagrams, surrogate key management, slowly changing dimensions
- **Database Administration:** Table creation automation, constraint management, view creation
- **Code Documentation:** Comprehensive technical documentation and schema visualization

## 🎓 Academic Context

This project was completed as part of MSIS 5663 (Database Management Systems) coursework at Oklahoma State University, demonstrating practical application of database design principles and data warehousing concepts.

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🔗 Connect With Me

- **LinkedIn: linkedin.com/in/anforeman
- **Email: andrew.neil528@gmail.com
