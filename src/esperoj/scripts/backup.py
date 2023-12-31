# src/esperoj/scripts/backup.py

from esperoj.database.airtable import Airtable

# Implement the backup functionality using the Airtable class

def backup_tables():
    airtable = Airtable("your_database_name", "your_api_key", "your_base_id")
    table_names = ["table1", "table2", "table3"]  # Replace with the actual table names

    for table_name in tables:
        table = airtable.table(table_name)
        records = table.get_all()

        # Backup records to CSV file
        csv_filename = f"{table_name}.csv"
        with open(csv_filename, "w") as csv_file:
            # Write CSV header
            csv_file.write("field1,field2,field3\n")

            # Write records to CSV file
            for record in records:
                csv_file.write(f"{record.field1},{record.field2},{record.field3}\n")

        # Backup records to JSON file
        json_filename = f"{table_name}.json"
        with open(json_filename, "w") as json_file:
            # Write records to JSON file
            json.dump(records, json_file)

        print(f"Backup for table '{table_name}' completed.")

if __name__ == "__main__":
    backup_tables()
