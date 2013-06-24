from religion_map import ReligionMap
import osmstataggregator

class Rel(osmstataggregator.BritainAndIrelandArea, ReligionMap):
    output_table = 'religion_brirl'


if __name__ == '__main__':
    Rel().main()

