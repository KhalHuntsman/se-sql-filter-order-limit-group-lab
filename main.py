import pandas as pd
import sqlite3

##### Part I: Basic Filtering #####

# Create the connection
# Note the connect is 'conn1' since there will be multiple .db used
conn1 = sqlite3.connect('planets.db')

# Select all
df_planets = pd.read_sql("""SELECT * FROM planets; """, conn1)
print (df_planets)


# STEP 1
# Replace None with your code
df_no_moons = pd.read_sql("""
SELECT *
    FROM planets
    WHERE num_of_moons = 0;
""", conn1)

# STEP 2
# Replace None with your code
df_name_seven = pd.read_sql("""
SELECT name, mass
    FROM planets
    WHERE length(name) = 7
""", conn1)

##### Part 2: Advanced Filtering #####

# STEP 3
# Replace None with your code
df_mass = pd.read_sql("""
SELECT name, mass
    FROM planets
    WHERE mass <= 1.00
""", conn1)

# STEP 4
# Replace None with your code
df_mass_moon = pd.read_sql("""
SELECT *
    FROM planets
    WHERE num_of_moons >= 1
    AND mass < 1.00
""", conn1)

# STEP 5
# Replace None with your code
df_blue = pd.read_sql("""
SELECT name, color
    FROM planets
    WHERE color LIKE '%blue%'
""", conn1)

##### Part 3: Ordering and Limiting #####

# STEP 0

# Create a connection
# Note the connect is 'conn2' since they will be multiple .db used
conn2 = sqlite3.connect('dogs.db')

# Select all
df_dogs = pd.read_sql("SELECT * FROM dogs;", conn2)
print(df_dogs)

# STEP 6
# Replace None with your code
df_hungry = pd.read_sql("""
SELECT name, age, breed
    FROM dogs
    WHERE hungry = 1
    ORDER BY age ASC
""", conn2)

# STEP 7
# Replace None with your code
df_hungry_ages = pd.read_sql("""
SELECT name, age, hungry
    FROM dogs
    WHERE age >= 2 
    AND age <= 7
    AND hungry = 1
    ORDER BY name ASC
""", conn2)

# STEP 8
# Replace None with your code
df_4_oldest = pd.read_sql("""
SELECT name, age, breed
FROM dogs
ORDER BY age DESC
LIMIT 4;
""", conn2)

##### Part 4: Aggregation #####

# STEP 0

# Create a connection
# Note the connect is 'conn3' since they will be multiple .db used
conn3 = sqlite3.connect('babe_ruth.db')

# Select all
df_stats = pd.read_sql("""SELECT * FROM babe_ruth_stats; """, conn3)
print(df_stats)

# STEP 9
# Replace None with your code
df_ruth_years = pd.read_sql("""
SELECT COUNT(*) AS total_years
    FROM babe_ruth_stats;
""", conn3)

# STEP 10
# Replace None with your code
df_hr_total = pd.read_sql("""
SELECT SUM(HR) AS total_home_runs
    FROM babe_ruth_stats;
""", conn3)


##### Part 5: Grouping and Aggregation #####

# STEP 11
# Replace None with your code
df_teams_years = pd.read_sql("""
SELECT team, COUNT(*) AS number_years
    FROM babe_ruth_stats
    GROUP BY team
""", conn3)

# STEP 12
# Replace None with your code
df_at_bats = pd.read_sql("""
SELECT team, AVG(at_bats) AS average_at_bats
    FROM babe_ruth_stats
    GROUP BY team
    HAVING AVG(at_bats) > 200
""", conn3)

conn1.close()
conn2.close()
conn3.close()