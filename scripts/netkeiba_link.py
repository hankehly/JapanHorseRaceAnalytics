import argparse
from typing import Final

RACE_KEY_LEN: Final[int] = 8
BASE_YEAR: Final[int] = 2000
BASE_URL: Final[str] = "https://db.netkeiba.com/race/{race_id}/"


def build_netkeiba_race_id(race_key: str) -> str:
    if len(race_key) != RACE_KEY_LEN or not race_key.isalnum():
        raise ValueError(f"Invalid race_key: {race_key}")
    race_course_code = race_key[0:2]
    year = BASE_YEAR + int(race_key[2:4])
    meeting_num = race_key[4].zfill(2)
    day_num = race_key[5].zfill(2)
    race_num = race_key[6:8]
    return f"{year}{race_course_code}{meeting_num}{day_num}{race_num}"


def build_url(race_key: str) -> str:
    return BASE_URL.format(race_id=build_netkeiba_race_id(race_key))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate netkeiba race URL from JRDB race key."
    )
    parser.add_argument("race_key", type=str, help="JRDB race key (8 chars)")
    args = parser.parse_args()
    try:
        print(build_url(args.race_key))
    except ValueError as e:
        parser.error(str(e))


if __name__ == "__main__":
    main()
