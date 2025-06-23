# Send Temperature VN to InfluxDB

This repository contains a simple script that periodically fetches the current temperature of Ho Chi Minh City from the OpenWeatherMap API and writes the result to an InfluxDB bucket. The script is written in Python and uses the `influxdb-client` and `requests` libraries.

## Requirements

- Python 3.8 or newer
- An InfluxDB instance
- An API key from [OpenWeatherMap](https://openweathermap.org/)

All Python dependencies are listed in `requirements.txt`.

## Usage

1. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Edit `send_temperature_vn.py` and update the following variables with your configuration:
   - `token`: your InfluxDB API token.
   - `org`: your InfluxDB organization name.
   - `bucket`: the bucket where temperature data will be stored.
   - `url_influx`: URL to your InfluxDB instance.
   - `API_KEY`: your OpenWeatherMap API key.
   - `CITY`: city name (default is `Ho Chi Minh`).

3. Run the script:
   ```bash
   python send_temperature_vn.py
   ```

The script will query the weather API every five seconds and write the current temperature to InfluxDB.
