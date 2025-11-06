"""FMI (Finnish Meteorological Institute) API client."""

from datetime import UTC, datetime, timedelta
from typing import cast

from fmiopendata.wfs import download_stored_query

from ilmanhinta.config import settings
from ilmanhinta.models.fmi import FMIObservation, WeatherData


class FMIClient:
    """Client for FMI Open Data API (no API key needed)."""

    def __init__(self, station_id: str | None = None) -> None:
        """Initialize FMI client."""
        self.station_id = station_id or settings.fmi_station_id
        self._MAX_INTERVAL_HOURS = 168

    def fetch_observations(
        self, start_time: datetime, end_time: datetime, station_id: str | None = None
    ) -> WeatherData:
        """Fetch weather observations from FMI."""
        station = station_id or self.station_id

        logfire.info(
            f"Fetching FMI observations for station {station} from {start_time} to {end_time}"
        )

        try:
            # Batch requests into <= 168-hour chunks
            cursor = start_time
            observations_dict: dict[datetime, FMIObservation] = {}
            station_name = "Unknown"

            while cursor < end_time:
                chunk_end = min(cursor + timedelta(hours=self._MAX_INTERVAL_HOURS), end_time)

                obs = download_stored_query(
                    "fmi::observations::weather::multipointcoverage",
                    args=[
                        f"fmisid={station}",
                        f"starttime={self._fmt_fmi_time(cursor)}",
                        f"endtime={self._fmt_fmi_time(chunk_end)}",
                    ],
                )

                # Station metadata (take first non-Unknown)
                if station_name == "Unknown":
                    station_name = (
                        getattr(obs, "location_metadata", {})
                        .get(station, {})
                        .get("name", "Unknown")
                    )

                # logfire.info(f"fetch_observations: data={getattr(obs, 'data', None)}")
                data = getattr(obs, "data", None)
                if isinstance(data, dict) and data:
                    # Two possible shapes observed from fmiopendata for observations::weather::multipointcoverage:
                    # 1) { fmisid: { timestamp -> { parameter -> {value, units} } } }
                    # 2) { timestamp: { station_name -> { parameter -> {value, units} } } }

                    def _parse_ts(raw: object) -> datetime:
                        if isinstance(raw, datetime):
                            ts = raw
                        else:
                            ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                        # Assume UTC if naive
                        return ts if ts.tzinfo else ts.replace(tzinfo=UTC)

                    if station in data:
                        # Shape 1: keyed by station id
                        station_data = data[station]
                        if isinstance(station_data, dict):
                            for ts_key, values in station_data.items():
                                timestamp = _parse_ts(ts_key)
                                values = values or {}
                                observations_dict[timestamp] = FMIObservation(
                                    timestamp=timestamp,
                                    temperature=values.get("Air temperature", {}).get("value"),
                                    humidity=values.get("Relative humidity", {}).get("value"),
                                    wind_speed=values.get("Wind speed", {}).get("value"),
                                    wind_direction=values.get("Wind direction", {}).get("value"),
                                    pressure=values.get("Pressure (msl)", {}).get("value"),
                                    precipitation=values.get("Precipitation amount", {}).get(
                                        "value"
                                    ),
                                    cloud_cover=values.get("Cloud amount", {}).get("value"),
                                )
                    else:
                        # Shape 2: keyed by timestamp -> station name
                        for ts_key, per_station in data.items():
                            if not isinstance(per_station, dict):
                                continue

                            # Try to pick the correct station by name; fall back to the only one if unique
                            candidate_station_name = station_name
                            if (
                                candidate_station_name != "Unknown"
                                and candidate_station_name in per_station
                            ):
                                chosen_station = candidate_station_name
                            elif len(per_station) == 1:
                                chosen_station = next(iter(per_station.keys()))
                            else:
                                # Best-effort: try case-insensitive match, otherwise first key
                                lowered = {k.lower(): k for k in per_station}
                                chosen_station = lowered.get(
                                    candidate_station_name.lower(), next(iter(per_station.keys()))
                                )

                            values = per_station.get(chosen_station, {}) or {}
                            timestamp = _parse_ts(ts_key)
                            observations_dict[timestamp] = FMIObservation(
                                timestamp=timestamp,
                                temperature=values.get("Air temperature", {}).get("value"),
                                humidity=values.get("Relative humidity", {}).get("value"),
                                wind_speed=values.get("Wind speed", {}).get("value"),
                                wind_direction=values.get("Wind direction", {}).get("value"),
                                pressure=values.get("Pressure (msl)", {}).get("value"),
                                precipitation=values.get("Precipitation amount", {}).get("value"),
                                cloud_cover=values.get("Cloud amount", {}).get("value"),
                            )

                # Advance cursor; add 1 second to avoid boundary duplicates
                cursor = chunk_end + timedelta(seconds=1)

            observations = sorted(observations_dict.values(), key=lambda o: o.timestamp)

            logfire.info(f"Fetched {len(observations)} observations from FMI station {station}")

            return WeatherData(
                station_id=station,
                station_name=station_name,
                observations=observations,
            )

        except Exception as e:
            logfire.error(f"Error fetching FMI data: {e}")
            raise

    def fetch_realtime_observations(self, hours: int = 24) -> WeatherData:
        """Fetch real-time weather observations for the last N hours."""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        return self.fetch_observations(start_time, end_time)

    def fetch_forecast(self, hours: int = 24, station_id: str | None = None) -> WeatherData:
        """Fetch weather forecast from FMI."""
        station = station_id or self.station_id
        start_time = datetime.utcnow()
        end_time = start_time + timedelta(hours=hours)

        logfire.info(f"Fetching FMI forecast for station {station} from {start_time} to {end_time}")

        try:
            # Resolve station coordinates using observations metadata so we can
            # keep `fmisid` as input even though point forecasts don't accept it.
            lat, lon, station_name_meta = self._resolve_station_coords_and_name(station)

            # Use HARMONIE-AROME point forecast (HIRLAM is discontinued)
            # Point forecasts accept latlon (lat,lon) but not fmisid.
            forecast = download_stored_query(
                "fmi::forecast::harmonie::surface::point::multipointcoverage",
                args=[
                    f"latlon={lat},{lon}",
                    f"starttime={self._fmt_fmi_time(start_time)}",
                    f"endtime={self._fmt_fmi_time(end_time)}",
                ],
            )

            observations: list[FMIObservation] = []

            data = getattr(forecast, "data", None)
            if isinstance(data, dict) and data:
                # Support both shapes:
                # A) { some_key: { timestamp -> { parameter -> {value, units} } } }
                # B) { timestamp: { location_name -> { parameter -> {value, units} } } }

                def _parse_ts(raw: object) -> datetime:
                    if isinstance(raw, datetime):
                        ts = raw
                    else:
                        ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)

                def _looks_like_ts(k: object) -> bool:
                    try:
                        _parse_ts(k)
                        return True
                    except Exception:
                        return False

                if any(_looks_like_ts(k) for k in data):
                    # Shape B: keyed by timestamp -> location name
                    for ts_key, per_location in data.items():
                        if not isinstance(per_location, dict):
                            continue
                        # Choose location: prefer resolved station name, else the only/first key
                        if station_name_meta != "Unknown" and station_name_meta in per_location:
                            chosen_loc = station_name_meta
                        elif len(per_location) == 1:
                            chosen_loc = next(iter(per_location.keys()))
                        else:
                            chosen_loc = next(iter(per_location.keys()))

                        values = per_location.get(chosen_loc, {}) or {}
                        timestamp = _parse_ts(ts_key)
                        observations.append(
                            FMIObservation(
                                timestamp=timestamp,
                                temperature=values.get("Air temperature", {}).get("value"),
                                humidity=values.get("Humidity", {}).get("value"),
                                wind_speed=values.get("Wind speed", {}).get("value"),
                                wind_direction=values.get("Wind direction", {}).get("value"),
                                pressure=values.get("Air pressure", {}).get("value"),
                                precipitation=values.get("Precipitation amount", {}).get("value"),
                                cloud_cover=values.get("Total cloud cover", {}).get("value"),
                            )
                        )
                else:
                    # Shape A: descend into the first nested dict that looks like {timestamp -> parameters}
                    nested = None
                    for v in data.values():
                        if isinstance(v, dict) and any(_looks_like_ts(k) for k in v):
                            nested = v
                            break

                    if nested:
                        for ts_key, values in nested.items():
                            if not isinstance(values, dict):
                                continue
                            timestamp = _parse_ts(ts_key)
                            observations.append(
                                FMIObservation(
                                    timestamp=timestamp,
                                    temperature=values.get("Temperature", {}).get("value"),
                                    humidity=values.get("Relative humidity", {}).get("value"),
                                    wind_speed=values.get("Wind speed", {}).get("value"),
                                    wind_direction=values.get("Wind direction", {}).get("value"),
                                    pressure=values.get("Pressure", {}).get("value"),
                                    precipitation=values.get("Precipitation amount", {}).get(
                                        "value"
                                    ),
                                    cloud_cover=values.get("Total cloud cover", {}).get("value"),
                                )
                            )

            # Prefer resolved station name from observations metadata, fallback to forecast metadata if present
            station_name = station_name_meta or getattr(forecast, "location_metadata", {}).get(
                station, {}
            ).get("name", "Unknown")

            logfire.info(f"Fetched {len(observations)} forecast points from FMI station {station}")

            return WeatherData(
                station_id=station,
                station_name=station_name,
                observations=observations,
            )

        except Exception as e:
            logfire.error(f"Error fetching FMI forecast: {e}")
            raise

    def _resolve_station_coords_and_name(self, station: str) -> tuple[float, float, str]:
        """
        Resolve station coordinates and name for a given fmisid using observations metadata.

        Returns (lat, lon, name).
        """
        # Look back a short window to get metadata; observations queries accept fmisid
        now = datetime.now(UTC)
        windows = [timedelta(hours=6), timedelta(hours=168)]
        info: dict = {}
        meta_snapshot: dict | None = None

        def _match_from_meta(meta: object, st: str) -> dict:
            """Try multiple strategies to extract station metadata from various shapes."""
            if not isinstance(meta, dict):
                return {}

            # 1) Direct key match (string id)
            if st in meta and isinstance(meta[st], dict):
                return cast(dict, meta[st])

            # 2) Integer key match
            if st.isdigit():
                try:
                    st_int = int(st)
                    if st_int in meta and isinstance(meta[st_int], dict):
                        return cast(dict, meta[st_int])
                # Ignore exceptions here; failure to convert or lookup is expected as part of multi-strategy matching.
                except Exception:
                    pass

            # 3) Normalize all keys to strings and try again
            try:
                as_str_keys = {str(k): v for k, v in meta.items()}
                if st in as_str_keys and isinstance(as_str_keys[st], dict):
                    return cast(dict, as_str_keys[st])
            except Exception:
                pass

            # 4) Search values for embedded fmisid field (various casings/keys)
            candidate_keys = ("fmisid", "FMISID", "station_id", "stationId", "id")
            for v in meta.values():
                if not isinstance(v, dict):
                    continue
                for k in candidate_keys:
                    sid = v.get(k)
                    if sid is None:
                        continue
                    try:
                        if str(sid) == st:
                            return cast(dict, v)
                    except Exception:
                        continue

            return {}

        for window in windows:
            start = now - window
            obs = download_stored_query(
                "fmi::observations::weather::multipointcoverage",
                args=[
                    f"fmisid={station}",
                    f"starttime={self._fmt_fmi_time(start)}",
                    f"endtime={self._fmt_fmi_time(now)}",
                ],
            )

            meta = getattr(obs, "location_metadata", {})
            meta_snapshot = meta if isinstance(meta, dict) else None
            info = _match_from_meta(meta, station)
            if info:
                break

        # Try common key variants for latitude/longitude
        def _coerce_float(val: object) -> float | None:
            if val is None:
                return None
            try:
                if isinstance(val, (int, float)):
                    return float(val)
                s = str(val).strip()
                # Handle shapes like "60.17" or "(60.17)"
                s = s.replace("(", "").replace(")", "")
                return float(s)
            except Exception:
                return None

        lat_raw = (
            info.get("latitude")
            or info.get("lat")
            or (info.get("wgs84") or info.get("wgs84_center"))
        )
        lon_raw = (
            info.get("longitude")
            or info.get("lon")
            or (info.get("wgs84") or info.get("wgs84_center"))
        )

        lat_f: float | None = None
        lon_f: float | None = None

        # If wgs84 string provided, parse common encodings first
        wgs = info.get("wgs84") or info.get("wgs84_center")
        if isinstance(wgs, str):
            s = wgs.strip()
            if "," in s:
                # "lat,lon"
                try:
                    lat_s, lon_s = s.split(",", 1)
                    lat_f = _coerce_float(lat_s)
                    lon_f = _coerce_float(lon_s)
                except Exception:
                    pass
            elif s.upper().startswith("POINT") and "(" in s and ")" in s:
                # WKT: POINT(lon lat)
                try:
                    inside = s[s.find("(") + 1 : s.rfind(")")]
                    parts = inside.replace(",", " ").split()
                    if len(parts) >= 2:
                        lon_f = float(parts[0])
                        lat_f = float(parts[1])
                except Exception as e:
                    logfire.error(f"Failed to parse WKT coordinates from string '{s}': {e}")

        # Otherwise, coerce individual fields
        if lat_f is None:
            lat_f = _coerce_float(lat_raw)
        if lon_f is None:
            lon_f = _coerce_float(lon_raw)

        if lat_f is None or lon_f is None:
            # Add context to help debugging if this fails in production
            keys = list(meta_snapshot.keys()) if isinstance(meta_snapshot, dict) else []
            raise ValueError(
                f"Could not resolve coordinates for FMI station {station} from metadata: {info}. "
                f"Available meta keys: {keys[:10]}{'...' if len(keys) > 10 else ''}"
            )

        name = info.get("name", "Unknown")
        return float(lat_f), float(lon_f), name

    @staticmethod
    def _fmt_fmi_time(dt: datetime) -> str:
        """Format datetime for FMI WFS: YYYY-MM-DDTHH:MM:SSZ (UTC, no micros)."""
        dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
