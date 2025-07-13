import pandas as pd
import pytest
from utils import (
    create_log_report,
    pivot_logs,
    parse_file,
)

DEFAULT_LOG_TYPES = {
    'name': 'string',
    'activity': 'string',
    'job_id': 'Int64',
    'timestamp': 'string'
}

DEFAULT_INPUT_PATH = "data/logs.log"
DEFAULT_REPORT_PATH = "data/report.log"


def test_wrong_path():
    """
    Test 1: Wrong path
       - Ensure that the function raises a SystemExit when the file path is incorrect.
    """
    with pytest.raises(SystemExit):
        parse_file("data/nofile.log", {})


def test_malformed_timestamps():
    """
    Test 2: Malformed timestamps
        - Ensure that the function raises a ValueError when the timestamps are not in the correct format.
    """
    with pytest.raises(ValueError):
        parse_file("test_data/test_1.log", DEFAULT_LOG_TYPES)


def test_unsorted_activities():
    """
    Test 3: Unsorted activities
        - Sorting the log by timestamp is out of scope for this exercise, so we should raise an error if the activities are not sorted by timestamp.
    """
    with pytest.raises(ValueError):
        pivot_logs(parse_file("test_data/test_2.log", DEFAULT_LOG_TYPES))


def test_missing_acitivites():
    """
    Test 4: Missing activities
        - Whenever there are missing START/END activities, we drop the job_id from the DataFrame.
        - Test case 3 contains 3 valid pairs of activities (6 rows in the original log file, reduced to 3 rows after pivoting)
    """
    df_pivoted = pivot_logs(
        parse_file("test_data/test_3.log", DEFAULT_LOG_TYPES))
    assert len(df_pivoted) == 3


def test_over_24_hours():
    """
    Test 5: Over 24 hours
        - If the log spills into the next day, we should raise an error (out of scope for this exercise).
    """
    with pytest.raises(ValueError):
        pivot_logs(parse_file("test_data/test_4.log", DEFAULT_LOG_TYPES))


def test_negative_duration():
    """
    Test 6: Negative duration
        - If the duration is negative, we should raise an error.
    """
    with pytest.raises(ValueError):
        df = parse_file("test_data/test_5.log", DEFAULT_LOG_TYPES)
        df = pivot_logs(df)
        create_log_report(df, "_.log")


def test_logs_error():
    """
    Test 7: Logs error
        - If the duration is over 10 minutes, we should log an error.
    """
    df = parse_file("test_data/test_6.log", DEFAULT_LOG_TYPES)
    df = pivot_logs(df)
    report = create_log_report(df, "_.log")
    assert "ERROR" in report


def test_logs_warning():
    """
    Test 8: Logs warning
        - If the duration is over 5 minutes but under 10 mins, we should log an warning.
    """
    df = parse_file("test_data/test_7.log", DEFAULT_LOG_TYPES)
    df = pivot_logs(df)
    report = create_log_report(df, "_.log")
    assert "WARNING" in report


def test_no_warning():
    """
    Test 9: No warning
        - If the duration is less than 5 mins, there should be no warning
    """
    df = parse_file("test_data/test_8.log", DEFAULT_LOG_TYPES)
    df = pivot_logs(df)
    report = create_log_report(df, "_.log")
    print(report)
    assert len(report) == 0


def test_duration():
    """
    Test 10: Duration
        - Test duration in seconds manually
    """
    df = parse_file("test_data/test_9.log", DEFAULT_LOG_TYPES)
    df = pivot_logs(df)
    report = create_log_report(df, "_.log")
    assert "602" in report
