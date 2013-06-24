# encoding: utf-8
from __future__ import division

import argparse
import psycopg2
from collections import Counter
import sys

def percentage_printer(input, msg=None, total=None):
    if total is None:
        total = len(input)
    if msg:
        if msg[-1] != ' ':
            msg = msg + " "
        if msg[0] != '\n':
            msg = '\n'+msg
        sys.stdout.write(msg)
    put_a_dot_every = max(int(total / 100), 1)
    for (done, element) in enumerate(input):
        if done % (put_a_dot_every * 10) == 0:
            sys.stdout.write("%s%%" % int(((done+1)*100)/total))
        elif done % put_a_dot_every == 0:
            sys.stdout.write(".")

        yield element

    sys.stdout.write("done\n")


def frange(start, stop, step=None):
    """A float-capable 'range' replacement"""
    step = step or 1.0
    stop = stop or start
    cur = start
    while cur < stop:
        yield cur
        cur += step


class OSMStatsAggregator(object):
    top, bottom = 90, -90
    left, right = -180, 180
    increment = 1
    #increment = None
    #land = None
    #output_table = None
    output_geom_col = "the_geom"
    #input_data_table = "whatever"
    input_geom_col = "the_geom"
    #database = None
    srid = 4326
    cut_land_boxes = True
    start_from_scratch = False
    rows_to_take = 100

    def parse_args(self):
        parser = argparse.ArgumentParser()

        # for bix size
        parser.add_argument('-i', '--increment', default=self.increment, type=float)
        parser.add_argument('-t', '--top', default=self.top, type=float)
        parser.add_argument('-l', '--left', default=self.left, type=float)
        parser.add_argument('-b', '--bottom', default=self.bottom, type=float)
        parser.add_argument('-r', '--right', default=self.right, type=float)

        parser.add_argument('--land', default=self.land, type=str)

        parser.add_argument('--output-table', default=self.output_table, type=str)
        parser.add_argument('--output-geom-col', default=self.output_geom_col, type=str)

        parser.add_argument('-d', '--database', default=self.database, type=str)
        parser.add_argument('--max-distance', default=30.0, type=float)

        parser.add_argument('--srid', default=self.srid, type=int)

        parser.add_argument('--cut-land-boxes',dest='cut_land_boxes',action='store_true')
        parser.add_argument('--no-cut-land-boxes',dest='cut_land_boxes',action='store_false')
        parser.set_defaults(cut_land_boxes=self.cut_land_boxes)

        parser.add_argument('--start-from-scratch', action='store_true', default=self.start_from_scratch)

        parser.add_argument('--rows-to-take', type=int, default=self.rows_to_take)

        # FIXME clean up SRID. we have it twice
        # FIXME --input-*-* are all over the place
        args = parser.parse_args()

        # Save to self
        self.__dict__.update(vars(args))

        self.minlon, self.maxlon = self.left, self.right
        self.minlat, self.maxlat = self.bottom, self.top

        assert self.minlon < self.maxlon
        assert self.minlat < self.maxlat
        assert self.increment > 0

        self.land_table, self.land_geom_col = self.land.split(".")

    def database_connection(self):
        if not hasattr(self, 'conn'):
            self.conn = psycopg2.connect("dbname="+self.database)

        return self.conn

    def create_table(self):
        conn = self.database_connection()
        cursor = conn.cursor()

        if self.start_from_scratch:
            cursor.execute("DROP TABLE IF EXISTS {0};".format(self.output_table))
            conn.commit()

        query = "SELECT 1 FROM pg_catalog.pg_tables WHERE  tablename = '{output_table}' limit 1;".format(output_table=self.output_table)
        cursor.execute(query)
        rows = cursor.fetchall()
        if len(rows) > 0:
            # table exists
            print "Table {output_table} already exists, not re-creating".format(output_table=self.output_table)
            return


        possible_columns = sorted(self.properties([]).keys())

        cursor.execute("CREATE TABLE {0} (id serial primary key, raw_data text[][] default NULL, properties_calculated boolean DEFAULT FALSE);".format(self.output_table))
        for column in possible_columns:
            cursor.execute("ALTER TABLE {0} ADD COLUMN {1} TExT DEFAULT NULL;".format(self.output_table, column))

        cursor.execute("SELECT AddGeometryColumn('{0}', '{1}', {2}, 'MULTIPOLYGON', 2);".format(self.output_table, self.output_geom_col, self.srid))
        cursor.execute("create index {0}__null_raw_data on {0} (raw_data) where raw_data IS NULL;".format(self.output_table))
        cursor.execute("create index {0}__properties_calculated on {0} (properties_calculated);".format(self.output_table))
        conn.commit()


    def generate_boxes(self):

        num_lons = int((self.maxlon - self.minlon)/self.increment)
        # at most 100 dots per line
        put_a_dot_every = max(int(num_lons / 100), 1)

        for this_minlat in frange(self.minlat, self.maxlat, self.increment):
            percent = ((this_minlat - self.minlat) / (self.maxlat - self.minlat) ) * 100
            sys.stdout.write("\n[%3d%%] %s " % (percent, this_minlat))

            done_lons = 0
            for this_minlon in frange(self.minlon, self.maxlon, self.increment):
                if done_lons % put_a_dot_every == 0:
                    sys.stdout.write(".")
                done_lons += 1

                centre_lat = this_minlat + (self.increment/2)
                centre_lon = this_minlon + (self.increment/2)

                #this_minlat, this_minlon = lat, lon
                this_maxlat, this_maxlon = this_minlat + self.increment, this_minlon + self.increment

                bbox_wkt = "ST_Multi(ST_MakeEnvelope({0}, {1}, {2}, {3}, {4}))".format(this_minlon, this_minlat, this_maxlon, this_maxlat, self.srid)
                yield {
                    'box_wkt': bbox_wkt,
                    'centre_lat': centre_lat,
                    'centre_lon': centre_lon,
                    'centre_y': centre_lat,
                    'centre_x': centre_lon,
                }

    def create_land_boxes(self):
        conn = self.database_connection()
        db_cursor = conn.cursor()

        query = "SELECT 1 FROM {output_table} limit 1;".format(output_table=self.output_table)
        db_cursor.execute(query)
        rows = db_cursor.fetchall()
        if len(rows) > 0:
            # There are rows in this table, ergo, don't re-create the land boxes
            print "Table {output_table} already has rows, not re-creating land boxes".format(output_table=self.output_table)
            return


        for bbox in self.generate_boxes():
            if not self.cut_land_boxes:
                # This is a quick work around, don't trim box based on coastline,
                # merely include it if it overlaps at all.
                query = "SELECT 1 from {land_table} where {land_col} && {bbox} limit 1;".format(land_table=self.land_table, land_col=land_col, bbox=bbox['box_wkt'])
                db_cursor.execute(query)
                rows = db_cursor.fetchall()
                if len(rows) == 0:
                    # no results, so this bbox doesn't overlap any land
                    # continue to next bbox
                    continue
                else:
                    bbox['geom'] = bbox['box_wkt']
                    continue
            else:

                query = """SELECT
                        ST_Multi(ST_Union(
                            CASE
                                WHEN ST_Within({bbox}, {land_table}.{land_col}) THEN {bbox}
                                WHEN ST_Within({land_table}.{land_col}, {bbox}) THEN ST_Multi({land_table}.{land_col})
                                WHEN
                                        ST_Intersects({land_table}.{land_col}, {bbox})
                                    THEN
                                        ST_CollectionExtract(ST_Multi(ST_Intersection({land_table}.{land_col}, {bbox})), 3)
                                ELSE NULL
                            END
                        )) AS geom
                        FROM
                            {land_table} WHERE {land_col} && {bbox};
                            """.format(land_table=self.land_table, land_col=self.land_geom_col, bbox=bbox['box_wkt'])
                db_cursor.execute(query)
                rows = db_cursor.fetchall()
                assert len(rows) == 1
                box = rows[0][0]
                if box is None:
                    # sea, no land
                    continue
                else:
                    bbox['box_wkb'] = box
                    bbox['geom'] = "'" + bbox['box_wkb'] + "'"


            query = "INSERT INTO {output_table} ( {output_geom_col} ) VALUES ( {bbox} );".format(output_table=self.output_table, output_geom_col=self.output_geom_col, bbox=bbox['geom'])
            db_cursor.execute(query)

        conn.commit()

    def populate_raw_data(self):
        conn = self.database_connection()
        db_cursor = conn.cursor()

        query = "SELECT id, (ST_XMax({geom})+ST_XMin({geom}))/2 as centre_x, (ST_YMax({geom})+ST_YMin({geom}))/2 as centre_y FROM {output_table} WHERE raw_data IS NULL AND NOT ST_IsEmpty({geom});".format(output_table=self.output_table, geom=self.output_geom_col)
        db_cursor.execute(query)
        boxes_to_update = db_cursor.fetchall()
        for (id, centre_x, centre_y) in percentage_printer(boxes_to_update, msg="Populating raw data:"):

            # First try a small bbox, we might have our 100 (or self.rows_to_take)
            # there, if we get < that, then we iterativly try larger bboxes,
            # stopping once we have gone as big as the max
            # We'll get /some/ results for deserted areas, and it should work
            # faster on dense areas.
            query = """
                select
                    st_distance_sphere(ST_SetSRID(ST_MakePoint({cx}, {cy}), {srid}), {input_data_table}.{input_geom_column}) as dist,
                    {data_cols}
                from {input_data_table}
                order by {input_data_table}.{input_geom_column}<->ST_SetSRID(ST_MakePoint({cx}, {cy}), {srid})
                limit {limit};
                """.format(
                    data_cols=", ".join(self.input_data_cols), input_geom_column=self.input_geom_col,
                    input_data_table=self.input_data_table,
                    srid=self.srid, cx=centre_x, cy=centre_y,
                    limit=self.rows_to_take,
                )
            db_cursor.execute(query)
            rows = db_cursor.fetchall()

            # Since we're using <-> to speed things up, we can't guarantee they
            # are ordered exactly, so re-sort to guarantee correct order
            rows.sort(key=lambda r:r[0])

            if len(rows) == 0:
                query = "UPDATE {output_table} SET raw_data = ARRAY[ARRAY[]]::text[][] WHERE id = {id};".format(output_table=self.output_table, id=id)
                db_cursor.execute(query)
            else:
                rows = [[str(x) for x in row] for row in rows]
                #raw_data = 'ARRAY[' + ", ".join("ARRAY["+",".join(repr(str(x)) for x in row)+"]" for row in rows) + ']'
                query = "UPDATE {output_table} SET raw_data = %s WHERE id = %s;".format(output_table=self.output_table)
                db_cursor.execute(query, [rows, id])
            
        conn.commit()

    def calculate_properties(self):
        conn = self.database_connection()
        # Give it a name, so it'll use a server side cursor. This is more memory effecient for large results

        writing_cursor = conn.cursor()

        reading_cursor = conn.cursor()
        query = "SELECT count(*) FROM {output_table} WHERE properties_calculated IS FALSE".format(output_table=self.output_table)
        reading_cursor.execute(query)
        total = reading_cursor.fetchall()[0][0]

        query = "SELECT id, raw_data FROM {output_table} WHERE properties_calculated IS FALSE AND raw_data IS NOT NULL".format(output_table=self.output_table)
        reading_cursor = conn.cursor("reading_properties")
        reading_cursor.execute(query)
        #boxes_to_update = [row for row in reading_cursor]
        for (id, raw_data) in percentage_printer(reading_cursor, msg="Calculating properties:", total=total):
            # first element has to be converted back to float
            raw_data = [[float(item[0])] + item[1:] for item in raw_data]
            properties = self.properties(raw_data)
            properties = [(k, properties[k]) for k in sorted(properties.keys())]
            query = ("UPDATE {output_table} SET properties_calculated = TRUE, " + ", ".join(k+" = %s" for k, v in properties) + " WHERE id = {id};").format(output_table=self.output_table, id=id)
            writing_cursor.execute(query, [v for k, v in properties])

        
        

    def main(self):
        try:

            self.parse_args()

            self.create_table()
            
            self.create_land_boxes()

            self.populate_raw_data()

            self.calculate_properties()
            print

        finally:
            # Commit anything unsaved yet
            self.database_connection().commit()
        return

# Some common areas
class IrelandArea(object):
    top = 55.9
    bottom = 51.1
    left = -10.9
    right = -4.7
    increment = 0.1

class EuropeArea(object):
    top = 71.5
    bottom = 36.2
    left = -25
    right = 50.3
    increment = 0.1
