class GasStation:
    def __init__(self, location_id, brand_name, location_name, latitude, longitude, address_line1, city, state_province, postal_code, country):
        self.location_id = location_id
        self.brand_name = brand_name
        self.location_name = location_name
        self.latitude = latitude
        self.longitude = longitude
        self.address_line1 = address_line1
        self.city = city
        self.state_province = state_province
        self.postal_code = postal_code
        self.country = country

    def __str__(self):
        return f"GasStation(location_id={self.location_id}, brand_name={self.brand_name}, location_name={self.location_name}, latitude={self.latitude}, longitude={self.longitude}, address_line1={self.address_line1}, city={self.city}, state_province={self.state_province}, postal_code={self.postal_code}, country={self.country})"

    def get_full_address(self):
        full_address = self.address_line1 + ", " + self.city + ", " + self.state_province + ", " + self.country
        if self.postal_code:
            full_address += " " + self.postal_code
        return full_address

    def to_dict(self):
        return {
            "location_id": self.location_id,
            "brand_name": self.brand_name,
            "location_name": self.location_name,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "address_line1": self.address_line1,
            "city": self.city,
            "state_province": self.state_province,
            "postal_code": self.postal_code,
            "country": self.country
        }

    @classmethod
    def from_database(cls, cursor, location_id):
        query = "SELECT * FROM gas_station WHERE location_id = %s"
        cursor.execute(query, (location_id,))
        row = cursor.fetchone()
        if row:
            return cls(*row)
        return None
