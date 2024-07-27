import os
import time


def backup_database(file_name: str):
    """Backup the database to a file"""
    print(f"Backing up database to {file_name}...")
    os.system(f"docker exec -t postgres pg_dump -U imontero -d piscineds > {file_name}")


def backup_table(table_name: str, file_name: str):
    """Backup a table to a file"""
    print(f"Backing up {table_name} to {file_name}...")
    os.system(f"docker exec -t postgres pg_dump -U imontero -d piscineds -t {table_name} > {file_name}")


def restore_table(file_name: str):
    """Restore a table from a file"""
    print(f"Restoring {file_name}...")
    os.system(f"docker exec -i postgres psql -U imontero -d piscineds -f {file_name}")


def copy_backup_to_container(file_name: str):
    """Copy a backup file to the container"""
    print(f"Copying {file_name} to the container...")
    os.system(f"docker cp {file_name} postgres:{file_name}")


if __name__ == "__main__":
    
    start_time = time.time()

    backup_database("piscineds.sql")

    #copy_backup_to_container("piscineds.sql")

    #backup_table("fusion", "fusion.sql")
    
    #restore_table("fusion_no_duplicates.sql") """

    print(f"--- {time.time() - start_time} seconds ---")