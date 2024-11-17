class WellDataProcessor:
    def __init__(self):
        self.wells = {}

    def add_wells(self, wells_data, well_type):
        for row in wells_data:
            well_id, field, coordx, coordy = row
            # Check if coordx and coordy are not None
            if coordx != 'NULL' and coordy != 'NULL' and coordx is not None and coordy is not None:
                # Convert coordinates to float to ensure numerical operations can be performed
                coordx = float(coordx)
                coordy = float(coordy)
                # Initialize the field in the wells dictionary if it doesn't exist
                if field not in self.wells:
                    self.wells[field] = {'production': [], 'injection': []}
                # Add the coordinates to the appropriate well type list
                self.wells[field][well_type].append((coordx, coordy))

    def get_wells(self):
        return self.wells