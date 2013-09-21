from areas import BritainAndIrelandArea
from religion_map import ReligionMap

class Rel(BritainAndIrelandArea, ReligionMap):
    output_table = 'religion_brirl'
    output_geom_type = 'point'
    increment = 0.025


if __name__ == '__main__':
    Rel().main()

