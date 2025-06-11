from services.GoogleMaps import GoogleMaps


class LatLong:
    """
    A class to retrieve latitude and longitude for a given city and state.
    """

    def __init__(self):
        """
        Initialize LatLong service to retrieve latitude and longitude for a given city and state.
        Currently uses Google Maps API.
        """
        self.google_maps = GoogleMaps()

    def get_coordinates(self, city: str, state: str) -> tuple[float, float]:
        """
        Get the latitude and longitude of a location using Google Maps API.

        Args:
            city (str): The name of the city.
            state (str): The name or abbreviation of the state.

        Returns:
            tuple: A tuple containing 'lat' and 'long' as floats.

        Raises:
            ValueError: If coordinates could not be retrieved.
        """
        lat, long = self.google_maps.get_latitude_longitude(city + "," + state)

        if lat is None or long is None:
            raise ValueError(f"Could not find coordinates for {city}, {state}.")

        return lat, long
