import pandas
import sqlalchemy as db

def read_csv():
    return pandas.read_csv(r"C:\Users\Ammad Ashraf\Documents\code\python-src\csv_to_db\data.csv")

def connect_db():
    engine = db.create_engine("mysql://ammada:public@localhost:3306/test")
    conn = engine.connect()
    return engine

if __name__ == '__main__':
    df = read_csv()

    # Select Specific columns
    print(df["Id"])
    print(df["City"])
    print(df["Year"])

    # Filter Rows based on condition
    rslt_df = df[df["Year"] > 2020]
    print(rslt_df.to_string())

    # Add new column
    df.insert(5, "Price", [1000, 1000, 1200, 1300, 1400], True)
    print(df.to_string())

    # Rename column
    df = df.rename(columns={"City" : "Location"})
    print(df.to_string())

    # Drop a column
    df = df.drop("Price", axis=1)
    print(df.to_string())

    # Group the data
    gk = df.groupby(["Location"])
    print(gk)

    # Get Average/Max
    print(df["Rooms"].mean())
    print(df["Rooms"].max())

    # Sort Rooms descending
    sorted_df = df.sort_values("Rooms")
    print(sorted_df.to_string())

    # Replace New York with NYC
    df_replaced = df.replace(to_replace="New York", value="NYC")
    print(df_replaced.to_string())
    
    # Divide rooms by 2
    df["Rooms"] = df["Rooms"].map(lambda a: a/2)
    print(df.to_string())

    # Create pivot table
    pt = pandas.pivot_table(df, values=["Rooms"], index="Id")
    print(pt)

    # Merge dataframes
    data = [[1,"Paris", 3000000], [2, "New York", 10000000], [3, "Tokyo", 5000000]]
    df2 = pandas.DataFrame(data, columns=["Id", "City", "Population"])
    merged = pandas.merge(df, df2, how="left", left_on="Location", right_on="City")
    print(merged.to_string())

    # Fill missing values
    merged = merged.fillna(0)
    print(merged)
    
    # Change types
    df["Rooms"] = df["Rooms"].astype(float)
    print(df) 

    # Make dataframe sum
    df["Sum"] = df["Rooms"] + df["Rooms"]
    print(df.to_string())

    # Get unique rooms
    print(df.Rooms.unique())

    # Filter on multiple condtions
    filtered_df = df[(df["Rooms"] > 3) & (df["Year"] > 2019)]

    print(filtered_df) 

    # Calculate cumulative sum of rooms
    sum = df["Rooms"].cumsum()
    print(sum)

    # Remove duplicate cities
    deduped = df.drop_duplicates(subset="Location")
    print(deduped.to_string())

    # Melt df
    melted_df = pandas.melt(df, id_vars=["Id"])
    print(melted_df)

