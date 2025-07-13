# Logging Duration Parser

This Python utility parses job logs to calculate durations between `START` and `END` events for each job, warns if durations exceed thresholds, and generates a report.

---

# Running via Docker
1. Build the image

`docker build -t log-parser .`

2. Run the container with a mounted volume

`docker run --rm -v $(pwd)/data:/app/data log-parser`


3. View the output

`cat data/report.log`

# Tests
1. Run the test suite through pytest as follows

`pytest tests.py`