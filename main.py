import pandas as pd
import sqlite3

##### Part I: Basic Filtering #####

# --------------------------------------------------
# Create a connection to the planets database.
# We name it conn1 because this file works with multiple databases.
conn1 = sqlite3.connect('planets.db')

# Retrieve and display all rows from the planets table.
# This is useful for inspecting the schema and data before filtering.
df_planets = pd.read_sql("""SELECT * FROM planets;""", conn1)
print(df_planets)


# STEP 1
# --------------------------------------------------
# Select all planets that have zero moons.
# This uses a simple WHERE clause with an equality check.
df_no_moons = pd.read_sql("""
    SELECT *
    FROM planets
    WHERE num_of_moons = 0;
""", conn1)


# STEP 2
# --------------------------------------------------
# Select planet names and mass where the planet's name
# contains exactly 7 characters.
# LENGTH() is a SQLite string function.
df_name_seven = pd.read_sql("""
    SELECT name, mass
    FROM planets
    WHERE length(name) = 7
""", conn1)


##### Part 2: Advanced Filtering #####

# STEP 3
# --------------------------------------------------
# Select planets with a mass less than or equal to 1.00.
# This demonstrates numeric comparison filtering.
df_mass = pd.read_sql("""
    SELECT name, mass
    FROM planets
    WHERE mass <= 1.00
""", conn1)


# STEP 4
# --------------------------------------------------
# Select planets that have at least one moon AND
# have a mass less than 1.00.
# This demonstrates combining conditions with AND.
df_mass_moon = pd.read_sql("""
    SELECT *
    FROM planets
    WHERE num_of_moons >= 1
      AND mass < 1.00
""", conn1)


# STEP 5
# --------------------------------------------------
# Select planets whose color contains the word "blue".
# LIKE with wildcards (%) allows partial string matching.
df_blue = pd.read_sql("""
    SELECT name, color
    FROM planets
    WHERE color LIKE '%blue%'
""", conn1)


##### Part 3: Ordering and Limiting #####

# --------------------------------------------------
# Create a connection to the dogs database.
# Named conn2 to distinguish it from other connections.
conn2 = sqlite3.connect('dogs.db')

# Retrieve and display all dog records for inspection.
df_dogs = pd.read_sql("SELECT * FROM dogs;", conn2)
print(df_dogs)


# STEP 6
# --------------------------------------------------
# Select hungry dogs and order them by age (youngest first).
# hungry = 1 represents TRUE in SQLite.
df_hungry = pd.read_sql("""
    SELECT name, age, breed
    FROM dogs
    WHERE hungry = 1
    ORDER BY age ASC
""", conn2)


# STEP 7
# --------------------------------------------------
# Select hungry dogs between the ages of 2 and 7 (inclusive).
# Results are ordered alphabetically by name.
df_hungry_ages = pd.read_sql("""
    SELECT name, age, hungry
    FROM dogs
    WHERE age >= 2 
      AND age <= 7
      AND hungry = 1
    ORDER BY name ASC
""", conn2)


# STEP 8
# --------------------------------------------------
# Select the four oldest dogs.
# ORDER BY DESC ensures the oldest dogs appear first,
# and LIMIT restricts the output to four rows.
df_4_oldest = pd.read_sql("""
    SELECT name, age, breed
    FROM dogs
    ORDER BY age DESC
    LIMIT 4;
""", conn2)


##### Part 4: Aggregation #####

# --------------------------------------------------
# Create a connection to the Babe Ruth statistics database.
# Named conn3 for clarity and separation of concerns.
conn3 = sqlite3.connect('babe_ruth.db')

# Retrieve and display all Babe Ruth statistics.
df_stats = pd.read_sql("""SELECT * FROM babe_ruth_stats;""", conn3)
print(df_stats)


# STEP 9
# --------------------------------------------------
# Count the total number of seasons Babe Ruth played.
# COUNT(*) counts all rows in the table.
df_ruth_years = pd.read_sql("""
    SELECT COUNT(*) AS total_years
    FROM babe_ruth_stats;
""", conn3)


# STEP 10
# --------------------------------------------------
# Calculate the total number of home runs Babe Ruth hit.
# SUM() aggregates the HR column across all seasons.
df_hr_total = pd.read_sql("""
    SELECT SUM(HR) AS total_home_runs
    FROM babe_ruth_stats;
""", conn3)


##### Part 5: Grouping and Aggregation #####

# STEP 11
# --------------------------------------------------
# Count how many years Babe Ruth played for each team.
# GROUP BY team groups rows before aggregation.
df_teams_years = pd.read_sql("""
    SELECT team, COUNT(*) AS number_years
    FROM babe_ruth_stats
    GROUP BY team
""", conn3)


# STEP 12
# --------------------------------------------------
# Calculate the average number of at-bats per team.
# HAVING filters groups based on aggregate results.
df_at_bats = pd.read_sql("""
    SELECT team, AVG(at_bats) AS average_at_bats
    FROM babe_ruth_stats
    GROUP BY team
    HAVING AVG(at_bats) > 200
""", conn3)


# --------------------------------------------------
# Close all database connections once queries are complete.
conn1.close()
conn2.close()
conn3.close()
