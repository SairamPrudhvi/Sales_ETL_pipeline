import time
from watchdog.observers import Observer

from etl.watchdog_ingest import RawDataHandler
from db.database import get_engine

RUN_DURATION_SECONDS = 60  # 1 minute

def main() -> None:
    """
    Start watchdog listener for raw_data ingestion.
    """
    engine = get_engine()

    event_handler = RawDataHandler(engine)
    observer = Observer()
    observer.schedule(event_handler, path="raw_data", recursive=False)

    observer.start()
    print("Watching raw_data folder for incoming files.")

    start_time = time.time()

    try:
        while time.time() - start_time < RUN_DURATION_SECONDS:
            time.sleep(5)
    except KeyboardInterrupt:
        print("Ingestion interrupted manually")

    finally:
        observer.stop()
        observer.join()
        print("Ingestion stopped after 1 minute")

if __name__ == "__main__":
    main()
