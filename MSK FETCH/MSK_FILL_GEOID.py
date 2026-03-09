import csv
import re
from pathlib import Path

import pandas as pd
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
SERVICE_PORT_SEQ_XLSX = SCRIPT_DIR / "msk_service_port_seq.xlsx"
LOCATIONS_CSV = SCRIPT_DIR / "Locations.csv"
FILLED_XLSX = SCRIPT_DIR / "msk_service_port_seq_filled.xlsx"
REVIEW_XLSX = SCRIPT_DIR / "msk_service_port_seq_match_review.xlsx"
LOCATIONS_URL = "https://www.maersk.com/content/media/book/files/mepcAlternates/1061.csv"
LOOKUP_URL = "https://api.maersk.com/synergy/reference-data/geography/locations"

DOWNLOAD_HEADERS = {
    "Accept": "*/*",
    "Referer": "https://www.maersk.com/schedules/pointToPoint",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}

LOOKUP_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Consumer-Key": "uXe7bxTHLY0yY0e8jnS6kotShkLuAAqG",
    "Origin": "https://www.maersk.com",
    "Referer": "https://www.maersk.com/schedules/pointToPoint",
    "User-Agent": DOWNLOAD_HEADERS["User-Agent"],
}

# Minimal alias map. Keep this explicit and small; unmatched ports stay visible in review output.
PORT_ALIASES = {
    "ISTANBUL": ["AMBARLI PORT ISTANBUL"],
    "IZMIT": ["IZMIT KORFEZI"],
    "TANGIER": ["TANGIER CITY"],
    "TANGIER (TC1)": ["TANGIER CITY"],
    "TANGIER (TC3)": ["TANGIER CITY"],
    "TANGIERS (TM2)": ["TANGIER CITY"],
    "VADO LIGURE": ["VADO"],
}

PORT_COUNTRIES = {
    "AARHUS": "DK",
    "ALGECIRAS": "ES",
    "ANTWERP": "BE",
    "BARCELONA": "ES",
    "BREMERHAVEN": "DE",
    "COLOMBO": "LK",
    "GENOA": "IT",
    "GOTHENBURG": "SE",
    "GWANGYANG": "KR",
    "HAMBURG": "DE",
    "ISTANBUL": "TR",
    "IZMIT": "TR",
    "KOPER": "SI",
    "LA SPEZIA": "IT",
    "LONDON GATEWAY": "GB",
    "PORT SAID EAST": "EG",
    "QINGDAO": "CN",
    "RIJEKA": "HR",
    "ROTTERDAM": "NL",
    "SALALAH": "OM",
    "SHANGHAI": "CN",
    "TANGIER": "MA",
    "TANGIER (TC1)": "MA",
    "TANGIER (TC3)": "MA",
    "TANGIERS (TM2)": "MA",
    "TANJUNG PELEPAS": "MY",
    "VADO LIGURE": "IT",
    "VALENCIA": "ES",
    "WILHELMSHAVEN": "DE",
    "YANTIAN": "CN",
    "NINGBO": "CN",
    "GOTHENBURG": "SE",
}


def normalize_text(value):
    text = "" if value is None or pd.isna(value) else str(value)
    text = text.replace("\xa0", " ").strip().upper()
    text = re.sub(r"\s+", " ", text)
    return text


def simplify_text(value):
    text = normalize_text(value)
    text = re.sub(r"\([^)]*\)", "", text)
    text = text.replace("-", " ")
    text = re.sub(r"\bPORT\b", "", text)
    text = re.sub(r"\bNEW\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def download_locations_csv():
    response = requests.get(LOCATIONS_URL, headers=DOWNLOAD_HEADERS, timeout=60)
    response.raise_for_status()
    LOCATIONS_CSV.write_bytes(response.content)
    return LOCATIONS_CSV


def fetch_online_candidates(port_name):
    def one_query(term):
        response = requests.get(
            LOOKUP_URL,
            headers=LOOKUP_HEADERS,
            params={
                "cityName": str(term).strip().lower(),
                "pageSize": 25,
                "sort": "cityName",
                "type": "city",
            },
            timeout=30,
        )
        if response.status_code == 404:
            return pd.DataFrame()
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if df.empty:
            return df
        df["cities_norm"] = df["cityName"].map(normalize_text)
        df["cities_simple"] = df["cityName"].map(simplify_text)
        df["cities"] = df["cityName"]
        df["cityGeoIds"] = df["maerskGeoLocationId"]
        df["Location"] = df.get("unLocCode")
        df["Alternative"] = df.get("maerskRkstCode")
        df["countries"] = df.get("countryName")
        df["regions"] = df.get("regionName")
        return df

    terms = [port_name]
    terms.extend(PORT_ALIASES.get(normalize_text(port_name), []))
    frames = [one_query(term) for term in terms]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["cities_norm", "cityGeoIds"])


def read_locations_csv(csv_path):
    rows = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        for raw in reader:
            if len(raw) == 8:
                row = raw
            elif len(raw) == 9:
                # Fix malformed country value like "Congo, Dem. Rep. of"
                row = raw[:4] + [raw[4] + "," + raw[5]] + raw[6:]
            else:
                continue
            rows.append(dict(zip(header, row)))

    df = pd.DataFrame(rows)
    for col in ["Location", "Alternative", "countries", "cities", "cityGeoIds", "regions"]:
        if col not in df.columns:
            df[col] = None

    df["cities_norm"] = df["cities"].map(normalize_text)
    df["cities_simple"] = df["cities"].map(simplify_text)
    return df


def match_location(port_name, locations_df):
    port_norm = normalize_text(port_name)
    port_simple = simplify_text(port_name)
    preferred_country = PORT_COUNTRIES.get(port_norm)

    exact = locations_df[locations_df["cities_norm"] == port_norm]
    if not exact.empty:
        exact = exact.drop_duplicates(subset=["cities_norm", "cityGeoIds"])
        if preferred_country:
            exact_country = exact[exact.get("countryCode", pd.Series(index=exact.index)).fillna("").eq(preferred_country)]
            if not exact_country.empty:
                return "matched_exact", exact_country
        return "matched_exact", exact

    alias_candidates = PORT_ALIASES.get(port_norm, [])
    if alias_candidates:
        alias_norm = [normalize_text(item) for item in alias_candidates]
        alias_matches = locations_df[locations_df["cities_norm"].isin(alias_norm)]
        if not alias_matches.empty:
            alias_matches = alias_matches.drop_duplicates(subset=["cities_norm", "cityGeoIds"])
            if preferred_country:
                alias_country = alias_matches[
                    alias_matches.get("countryCode", pd.Series(index=alias_matches.index)).fillna("").eq(preferred_country)
                ]
                if not alias_country.empty:
                    return "matched_alias", alias_country
            return "matched_alias", alias_matches

    simplified = locations_df[locations_df["cities_simple"] == port_simple]
    if not simplified.empty:
        simplified = simplified.drop_duplicates(subset=["cities_simple", "cityGeoIds"])
        if preferred_country:
            simple_country = simplified[
                simplified.get("countryCode", pd.Series(index=simplified.index)).fillna("").eq(preferred_country)
            ]
            if not simple_country.empty:
                return "matched_simple", simple_country
        return "matched_simple", simplified

    online_df = fetch_online_candidates(port_name)
    if not online_df.empty:
        online_exact = online_df[online_df["cities_simple"] == port_simple]
        if online_exact.empty and alias_candidates:
            alias_simple = [simplify_text(item) for item in alias_candidates]
            online_exact = online_df[online_df["cities_simple"].isin(alias_simple)]
        if preferred_country:
            online_country = online_df[online_df["countryCode"].fillna("") == preferred_country]
            if not online_country.empty:
                online_exact = online_country if online_exact.empty else online_exact[online_exact["countryCode"] == preferred_country]
        if not online_exact.empty:
            return "matched_online", online_exact.drop_duplicates(subset=["cities_norm", "cityGeoIds"])
        if preferred_country:
            online_country = online_df[online_df["countryCode"].fillna("") == preferred_country]
            if not online_country.empty:
                return "matched_online", online_country.drop_duplicates(subset=["cities_norm", "cityGeoIds"])
        return "review_online", online_df.drop_duplicates(subset=["cities_norm", "cityGeoIds"]).head(10)

    contains_mask = (
        locations_df["cities_simple"].str.contains(re.escape(port_simple), na=False)
        | locations_df["cities_norm"].str.contains(re.escape(port_norm), na=False)
    )
    review = locations_df[contains_mask].drop_duplicates(subset=["cities_norm", "cityGeoIds"]).head(10)
    if not review.empty:
        return "review", review

    return "missing", locations_df.iloc[0:0]


def build_outputs(service_df, locations_df):
    filled_rows = []
    review_rows = []

    for row in service_df.to_dict(orient="records"):
        port = row.get("port")
        status, matches = match_location(port, locations_df)
        filled = dict(row)

        if status.startswith("matched") and len(matches) == 1:
            match = matches.iloc[0]
            filled["geo_id"] = match.get("cityGeoIds")
            filled["matched_city"] = match.get("cities")
            filled["matched_location"] = match.get("Location")
            filled["matched_alternative"] = match.get("Alternative")
        else:
            filled["geo_id"] = None
            filled["matched_city"] = None
            filled["matched_location"] = None
            filled["matched_alternative"] = None

        filled["match_status"] = status
        filled_rows.append(filled)

        if matches.empty:
            review_rows.append(
                {
                    "service": row.get("service"),
                    "service_seq": row.get("service_seq"),
                    "port": port,
                    "match_status": status,
                    "matched_city": None,
                    "cityGeoIds": None,
                    "Location": None,
                    "Alternative": None,
                    "countries": None,
                    "regions": None,
                }
            )
        else:
            for _, match in matches.iterrows():
                review_rows.append(
                    {
                        "service": row.get("service"),
                        "service_seq": row.get("service_seq"),
                        "port": port,
                        "match_status": status,
                        "matched_city": match.get("cities"),
                        "cityGeoIds": match.get("cityGeoIds"),
                        "Location": match.get("Location"),
                        "Alternative": match.get("Alternative"),
                        "countries": match.get("countries"),
                        "regions": match.get("regions"),
                    }
                )

    filled_df = pd.DataFrame(filled_rows)
    review_df = pd.DataFrame(review_rows)
    summary_df = (
        review_df.groupby(["port", "match_status"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["match_status", "port"])
    )
    return filled_df, review_df, summary_df


def main():
    download_locations_csv()
    locations_df = read_locations_csv(LOCATIONS_CSV)
    service_df = pd.read_excel(SERVICE_PORT_SEQ_XLSX)
    service_df["port"] = service_df["port"].astype(str).str.replace("\xa0", " ", regex=False).str.strip()

    filled_df, review_df, summary_df = build_outputs(service_df, locations_df)

    filled_df.to_excel(FILLED_XLSX, index=False)
    with pd.ExcelWriter(REVIEW_XLSX, engine="openpyxl") as writer:
        review_df.to_excel(writer, index=False, sheet_name="review")
        summary_df.to_excel(writer, index=False, sheet_name="summary")

    matched_count = int(filled_df["geo_id"].notna().sum())
    total_count = int(len(filled_df))
    print(f"Locations refreshed: {LOCATIONS_CSV}")
    print(f"Filled table saved: {FILLED_XLSX}")
    print(f"Review table saved: {REVIEW_XLSX}")
    print(f"Matched rows: {matched_count}/{total_count}")


if __name__ == "__main__":
    main()
