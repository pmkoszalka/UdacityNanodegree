import bucket
import redshift
import validate
import logging
import logging_config

def run() -> None:
    """Main pipeline for the project"""
    
    logging.info("ETL process has started!")
    bucket.main()
    redshift.main()
    validate.main()
    logging.info("ETL process has completed successfully!")
    
if __name__ == "__main__":
    run()