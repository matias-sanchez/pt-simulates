import multiprocessing
import random
import time
import mysql.connector

# --- Configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'msandbox',
    'password': 'msandbox',
    'database': 'vt_byuser'
}

NUM_WORKER_PROCESSES = 20  
BATCH_SIZE = 150 # the number of values in the query

NUM_QUERIES_PER_PROCESS = 1_000_000
# Print a progress update every N queries.
REPORT_INTERVAL = 10000

def safe_print(lock, message):
    """
    A process-safe print function that uses a shared lock.
    """
    with lock:
        print(message, flush=True)

# 1. The function to gather all in_values (run by the main process)
def gather_all_in_values():
    """Connects to the DB, queries for all distinct k's, and returns them as a list."""
    print("[Main Process] Starting to fetch all distinct channel_id ...")
    in_values = []
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("select distinct channel_id from channels_members;")
        in_values = [row[0] for row in cursor.fetchall()]
        cursor.close()
        print(f"[Main Process] Finished. Found {len(in_values)} total channel_id.")
    except mysql.connector.Error as err:
        print(f"[Main Process] Error: {err}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("[Main Process] Connection closed.")
    return in_values

# 2. The target function for the worker processes
def worker_query_data(process_id, in_values, print_lock):
    """
    Establishes a single connection and runs a random query in a loop.
    This function is executed in a separate process.
    """
    # Use the process-safe print function
    safe_print(print_lock, f"[Worker {process_id}] Process started, will run {NUM_QUERIES_PER_PROCESS} queries.")
    
    conn = None
    total_records_found = 0
    start_time = time.time()

    try:
        # Each process establishes its own connection to the database
        conn = mysql.connector.connect(**DB_CONFIG)
        safe_print(print_lock, f"[Worker {process_id}] Connection established.")

        # Main loop to run the query repeatedly
        for i in range(NUM_QUERIES_PER_PROCESS):
            # 3. generate the random value in query
            random_values = random.sample(in_values, BATCH_SIZE)

            # The placeholder for mysql-connector is %s
            placeholders = ', '.join(['%s'] * len(random_values))
            # Using the query from your script
            query = f"SELECT user_id, channel_id, channel_team_id, date_joined, date_deleted, last_read, last_read_abs, is_open, channel_type, channel_privacy_type, share_type, target_user_id, target_user_team_id, is_target_user_deleted, user_team_id, is_starred, latest_counted_ts, latest_event_ts, history_invalid_ts, max_invalid_message_ts, date_archived FROM channels_members WHERE channel_id IN ({placeholders}) and date_deleted = 0 and channel_type = 0 limit 10000, 1"
            
            cursor = conn.cursor()
            cursor.execute(query, tuple(random_values))
            results = cursor.fetchall()
            cursor.close()
            
            # Report progress periodically
            if (i + 1) % REPORT_INTERVAL == 0:
                safe_print(print_lock, f"[Worker {process_id}] Progress: {i + 1}/{NUM_QUERIES_PER_PROCESS} queries completed.")

    except mysql.connector.Error as err:
        safe_print(print_lock, f"[Worker {process_id}] Error: {err}")
    except Exception as e:
        safe_print(print_lock, f"[Worker {process_id}] An unexpected error occurred: {e}")
    finally:
        # Ensure the connection is closed when the process finishes or errors out
        if conn and conn.is_connected():
            conn.close()
            end_time = time.time()
            duration = end_time - start_time
            safe_print(
                print_lock,
                f"[Worker {process_id}] FINISHED. Ran {NUM_QUERIES_PER_PROCESS} queries "
            )


if __name__ == "__main__":
    # --- STEP 1: The main process gathers all channel_id first ---
    print("\n--- Starting Step 1: Gather All in_values  ---")
    all_in_values = gather_all_in_values()
    print("--- Step 1 Complete ---\n")

    if not all_in_values:
        print("No data_ids found. Exiting.")
        exit()

    # --- STEP 2 & 3: Create and start worker processes ---
    print(f"--- Starting Step 2: Creating {NUM_WORKER_PROCESSES} worker processes ---")
    
    # Create a lock to be shared among processes for safe printing
    lock = multiprocessing.Lock()
    
    worker_processes = []
    for i in range(NUM_WORKER_PROCESSES):
        # Create a Process, not a Thread
        process = multiprocessing.Process(
            target=worker_query_data, 
            args=(i + 1, all_in_values, lock) # Pass the lock to the worker
        )
        worker_processes.append(process)
        process.start() # Start the process

    # Wait for all worker processes to complete their execution
    for process in worker_processes:
        process.join()

    print("\n--- All worker processes have finished their loops. ---")
