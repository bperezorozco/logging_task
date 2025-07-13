import pandas as pd
import logging
from typing import Dict
import sys

WARNING_THRESHOLD_SECS = 5 * 60
ERROR_THRESHOLD_SECS = 10 * 60
logger = logging.getLogger(__name__)


def _log_duration_warnings_and_errors(row: pd.Series) -> str:
    """
    Checks the duration of a job and returns a warning or error message if thresholds are exceeded.

    Args:
        row (pd.Series): A row from the DataFrame, expected to contain a "duration" value and a name (used as job_id).

    Returns:
        str: A formatted warning/error string if thresholds are exceeded, otherwise an empty string.
    """
    job_id = row.name
    duration = row["duration"]
    if duration > ERROR_THRESHOLD_SECS:
        msg = f"ERROR: Job with job_id={job_id} took {duration} seconds to complete.\n"
        logger.error(msg)
        return msg
    elif duration > WARNING_THRESHOLD_SECS:
        msg = f"WARNING: Job with job_id={job_id} took {duration} seconds to complete.\n"
        logger.error(msg)
        return msg
    elif duration < 0:
        msg = f"ERROR: Job with job_id={job_id} has a negative duration of {duration} seconds.\n"
        logger.error(msg)
        raise (ValueError(msg))
    return ""


def create_log_report(df_pivoted: pd.DataFrame, output_path: str) -> str:
    """
    Calculates durations from START and END timestamps, logs any duration warnings/errors, 
    and returns the compiled report text.

    Args:
        df_pivoted (pd.DataFrame): A DataFrame with "START" and "END" datetime columns, indexed by job_id.

    Returns:
        str: A report string combining all duration warnings and errors.
    """
    df_pivoted["duration"] = (df_pivoted["END"] -
                              df_pivoted["START"]).dt.total_seconds()
    report = df_pivoted[["duration"]].apply(_log_duration_warnings_and_errors,
                                            axis=1).str.cat()

    # write report to file
    with open(output_path, "w+") as f:
        f.write(report)
        logger.info("Report generated successfully.")
    return report


def pivot_logs(df_logs: pd.DataFrame) -> pd.DataFrame:
    """
    Pivots a log DataFrame into a DataFrame indexed by job_id, with START and END timestamps.

    Args:
        df_logs (pd.DataFrame): A DataFrame with "job_id" (row index), "activity" (START/END), and "timestamp" columns.

    Returns:
        pd.DataFrame: A pivoted DataFrame with "START" and "END" columns indexed by job_id.

    Raises:
        ValueError: If duplicate START or END entries exist for the same job_id.
    """
    try:
        df_pivoted = df_logs.pivot(index="job_id",
                                   columns="activity",
                                   values="timestamp")
    except ValueError:
        logger.error(
            "Duplicate activities for the same job_id. Ensure each job_id has only one START and one END activity."
        )
        sys.exit(1)

    df_len_before_dropping = len(df_pivoted)
    df_pivoted = df_pivoted.dropna()
    n_na_dropped = df_len_before_dropping - len(df_pivoted)

    if n_na_dropped > 0:
        logger.warning(
            f"Dropped {n_na_dropped} rows with missing values. Please ensure that timestamps are present for all jobs in the %H:%M:%S format. Also ensure all jobs have a START and END activity."
        )
    return df_pivoted


def parse_file(file_path: str, types: Dict) -> pd.DataFrame:
    """
    Loads and validates a log file, parses timestamps, and cleans activity names.

    Args:
        file_path (str): Path to the log file.
        types (Dict): Dictionary of column names to pandas dtypes.

    Returns:
        pd.DataFrame: Clean log DataFrame with consistent timestamp and activity formatting.

    Raises:
        SystemExit: If the file is not found.
        ValueError: If timestamps are not in strictly increasing order.
    """
    try:
        df = pd.read_csv(
            file_path,
            names=["timestamp", "name", "activity", "job_id"],
            dtype=types,
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"],
                                         format="%H:%M:%S",
                                         errors="coerce")

        # strip whitespace from activity column
        df["activity"] = df["activity"].str.strip()

    except FileNotFoundError:
        logger.error("File not found. Please check the file path.")
        sys.exit(1)

    logger.info(f"File loaded successfully with {len(df)} rows.")

    if not df["timestamp"].is_monotonic_increasing:
        logger.error(
            "Timestamps are malformed or not in increasing order. Please provide a log file with timestamps in the %H:%M:%S format and in increasing order."
        )
        raise ValueError(
            "ERROR: Timestamps are not in increasing order. Please provide a log file with timestamps in increasing order. IMPORTANT: logs spanning more than one day are not currently supported."
        )

    return df
