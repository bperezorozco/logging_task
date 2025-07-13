import logging

from utils import create_log_report, pivot_logs, parse_file

LOG_TYPES = {
    'name': 'string',
    'activity': 'string',
    'job_id': 'Int64',
    'timestamp': 'string'
}

INPUT_PATH = "data/logs.log"
REPORT_PATH = "data/report.log"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

if __name__ == "__main__":
    df = parse_file(INPUT_PATH, LOG_TYPES)
    df = pivot_logs(df)
    create_log_report(df, REPORT_PATH)

# tests: check that the first time a job appears, the activity is START and the last time it appears, the activity is END
# tests: check that the duration is always positive
# tests: check that the duration is always less than 1 hour?
# tests: check that the duration is always less than 1 day (or assume it will be and therefore discard any data that is not)
# tests:
