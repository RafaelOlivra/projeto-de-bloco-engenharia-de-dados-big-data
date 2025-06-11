import re
from string import ascii_uppercase
from json import loads
import os

# Get the current script directory
script_dir = os.path.dirname(os.path.abspath(__file__))
cities_json_path = os.path.join(script_dir, "../cities.json")

# Load cities from the JSON file
cities = {}
with open(cities_json_path, "r", encoding="utf-8") as f:
    cities = loads(f.read())

def expand_letter_range(start, end):
    """
    Expands a letter range from start to end (inclusive).
    
    Args:
        start (str): Starting letter (inclusive).
        end (str): Ending letter (inclusive).
    """
    return [c for c in ascii_uppercase if start <= c <= end]

def parse_ranges(ranges_str):
    """
    Parses a string of letter ranges in the format "XX{A-Z}" and returns a dictionary
    
    Args:
        ranges_str (str): A string containing letter ranges, e.g. "SP{A-C},RJ{D-F}"
    """
    result = {}
    for part in ranges_str.split(','):
        match = re.match(r'([A-Z]{2})\{([A-Z])-([A-Z])\}', part)
        if match:
            state, start, end = match.groups()
            result[state] = expand_letter_range(start, end)
    return result

def filter_cities(ranges, _cities=cities):
    """
    Filters cities based on the provided letter ranges.
    Args:
        ranges (dict): A dictionary where keys are state codes and values are lists of letters.
        _cities (dict): A dictionary of cities to filter (default is the loaded cities).
    Returns:
        dict: A dictionary of cities that match the letter ranges.
    """
    filtered = {}
    for name, data in _cities.items():
        state, city = name.split("-", 1)
        first_letter = city[0].upper()
        if state in ranges and first_letter in ranges[state]:
            filtered[name] = data
    return filtered
