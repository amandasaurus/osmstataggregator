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

def batch(itr, count=1000):
    current_buffer = []
    for el in itr:
        current_buffer.append(el)
        if len(current_buffer) >= count:
            yield current_buffer
            current_buffer = []

    if len(current_buffer) != 0:
        yield current_buffer

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
    output_geom_type = "polygon"
    #input_data_table = "whatever"
    input_geom_col = "the_geom"
    #database = None
    srid = 4326
    cut_land_boxes = True
    start_from_scratch = False
    rows_to_take = 100

    internal_string_sep = "|"

    def parse_args(self):
        """
        Parse command line options and figure out the settings
        """
        parser = argparse.ArgumentParser()

        # for box size
        parser.add_argument('-i', '--increment', default=self.increment, type=float)
        parser.add_argument('-t', '--top', default=self.top, type=float)
        parser.add_argument('-l', '--left', default=self.left, type=float)
        parser.add_argument('-b', '--bottom', default=self.bottom, type=float)
        parser.add_argument('-r', '--right', default=self.right, type=float)

        parser.add_argument('--land', default=self.land, type=str)

        parser.add_argument('--output-table', default=self.output_table, type=str)
        parser.add_argument('--output-geom-col', default=self.output_geom_col, type=str)
        parser.add_argument('--output-geom-type', default=self.output_geom_type, choices=['polygon', 'point'])

        parser.add_argument('-d', '--database', default=self.database, type=str)

        parser.add_argument('--srid', default=self.srid, type=int)

        parser.add_argument('--cut-land-boxes',dest='cut_land_boxes',action='store_true')
        parser.add_argument('--no-cut-land-boxes',dest='cut_land_boxes',action='store_false')
        parser.set_defaults(cut_land_boxes=self.cut_land_boxes)

        parser.add_argument('--start-from-scratch', action='store_true', default=self.start_from_scratch)

        parser.add_argument('--rows-to-take', type=int, default=self.rows_to_take)

        parser.add_argument('--recalculate-properties',action='store_true', default=False)

        args = parser.parse_args()

        # Save to self
        self.__dict__.update(vars(args))

        self.minlon, self.maxlon = self.left, self.right
        self.minlat, self.maxlat = self.bottom, self.top

        assert self.minlon < self.maxlon
        assert self.minlat < self.maxlat
        assert self.increment > 0
        assert self.rows_to_take >= 1

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
        table_exists = len(rows) > 0
        if table_exists:
            # table exists
            print "Table {output_table} already exists, not re-creating".format(output_table=self.output_table)

        else:
            cursor.execute("CREATE TABLE {0} (id serial primary key, raw_data text[] default NULL, properties_calculated boolean DEFAULT FALSE);".format(self.output_table))

        # What columns are there?
        if table_exists:
            cursor.execute("select column_name from information_schema.columns where table_name = %s", [self.output_table])
            existing_columns = [x[0] for x in cursor.fetchall()]
        else:
            existing_columns = []

        possible_columns = self.properties([])
        for column in sorted(possible_columns):
            if column not in existing_columns:
                # we're adding a properties column, so we defintily need to recalculate
                self.recalculate_properties = True

                if type(possible_columns[column]) in [str, basestring, unicode]:
                    cursor.execute("ALTER TABLE {0} ADD COLUMN {1} TEXT DEFAULT NULL;".format(self.output_table, column))
                elif type(possible_columns[column]) in [int, float]:
                    cursor.execute("ALTER TABLE {0} ADD COLUMN {1} REAL DEFAULT NULL;".format(self.output_table, column))
                else:
                    raise TypeError

        if not table_exists:
            # Need to create the geom column
            if self.output_geom_type == 'polygon':
                cursor.execute("SELECT AddGeometryColumn('{0}', '{1}', {2}, 'MULTIPOLYGON', 2);".format(self.output_table, self.output_geom_col, self.srid))
            elif self.output_geom_type == 'point':
                cursor.execute("SELECT AddGeometryColumn('{0}', '{1}', {2}, 'POINT', 2);".format(self.output_table, self.output_geom_col, self.srid))
            else:
                raise ValueError

        if not table_exists:
            cursor.execute("create index {0}__null_raw_data on {0} (raw_data) where raw_data IS NULL;".format(self.output_table))
            cursor.execute("create index {0}__properties_calculated on {0} (properties_calculated);".format(self.output_table))
            cursor.execute("create index {0}__{1} on {0} using gist ({1});".format(self.output_table, self.output_geom_col))

        conn.commit()
        cursor.close()


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
                point_wkt = "ST_SetSRID(ST_Point({0}, {1}), {2})".format(centre_lon, centre_lat, self.srid)
                if self.output_geom_type == 'point':
                    geom_wkt = point_wkt
                elif self.output_geom_type == 'polygon':
                    geom_wkt = bbox_wkt
                else:
                    raise TypeError

                yield {
                    'box_wkt': bbox_wkt,
                    'point_wkt': point_wkt,
                    'geom_wkt': geom_wkt,
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


        if self.output_geom_type == 'point':
            ## For points, we first put all the points in the DB

            # dodgy string joining for SQL here. Here be dragons. need it cause we need postgres to evaluate the function calls
            query_prefix = "INSERT INTO {output_table} ( {output_geom_col} ) VALUES ".format(output_table=self.output_table, output_geom_col=self.output_geom_col)
            for bbox_groups in batch(self.generate_boxes(), 10000):
                query = query_prefix + ", ".join("("+x['point_wkt']+")" for x in bbox_groups) + ";"
                db_cursor.execute(query)

            ## ... then we remove the ones that aren't on the ground
            print "\nRemoving non-land points..."
            query = "delete from {output_table} where not exists (select 1 from {land_table} where ST_Contains({land_table}.{land_col}, {output_table}.{output_geom_col}) limit 1);".format(output_table=self.output_table, land_table=self.land_table, land_col=self.land_geom_col, output_geom_col=self.output_geom_col)
            db_cursor.execute(query)
            print "removed."



        elif self.output_geom_type == 'polygon':
            for bbox in self.generate_boxes():
                if not self.cut_land_boxes:
                    # This is a quick work around, don't trim box based on coastline,
                    # merely include it if it overlaps at all.
                    query = "SELECT 1 from {land_table} where {land_col} && {bbox} limit 1;".format(land_table=self.land_table, land_col=self.land_geom_col, bbox=bbox['box_wkt'])
                    db_cursor.execute(query)
                    rows = db_cursor.fetchall()
                    if len(rows) == 0:
                        # no results, so this bbox doesn't overlap any land
                        # continue to next bbox
                        continue
                    else:
                        bbox['geom'] = bbox['box_wkt']
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
        db_cursor.close()

    def populate_raw_data(self):
        """
        For each of the points, populate the raw_data column with the closest raw data
        """
        conn = self.database_connection()
        db_cursor = conn.cursor()

        if self.output_geom_type == 'polygon':
            output_geom_as_point = "ST_Centroid(ST_Box2d({output_table}.{output_geom_col}))".format(output_table=self.output_table, output_geom_col=self.output_geom_col)
        elif self.output_geom_type == 'point':
            # already a point
            output_geom_as_point = self.output_table+"."+self.output_geom_col
        else:
            raise TypeError
        
        data_cols = (", "+repr(self.internal_string_sep)+", ").join(self.input_data_cols)

        # do
        query = """update
                        {output_table}
                    set raw_data = (
                        select array(select
                            CONCAT(
                                ST_Distance_Sphere({input_data_table}.{input_geom_col}, {output_geom_as_point})::text,
                                {internal_string_sep!r},
                                {data_cols}
                                )
                            from {input_data_table} order by {input_data_table}.{input_geom_col}<->{output_geom_as_point}
                            limit {limit}
                            ))
                    where raw_data IS NULL;"""
        query = query.format(
            output_table=self.output_table, input_data_table=self.input_data_table, input_geom_col=self.input_geom_col,
            data_cols = data_cols, output_geom_as_point=output_geom_as_point,
            limit=self.rows_to_take, internal_string_sep=self.internal_string_sep,
        )

        print "Calculating raw_data for each item..."
        db_cursor.execute(query)
        print "done."
        conn.commit()

    def calculate_properties(self):
        """
        Given the raw data for each point, aggregate and calculate our stats
        """
        conn = self.database_connection()


        writing_cursor = conn.cursor()

        if self.recalculate_properties:
            writing_cursor.execute("UPDATE {output_table} SET properties_calculated = FALSE;".format(output_table=self.output_table))

        reading_cursor = conn.cursor()
        query = "SELECT count(*) FROM {output_table} WHERE properties_calculated IS FALSE".format(output_table=self.output_table)
        reading_cursor.execute(query)
        total = reading_cursor.fetchall()[0][0]

        query = "SELECT id, raw_data FROM {output_table} WHERE properties_calculated IS FALSE AND raw_data IS NOT NULL".format(output_table=self.output_table)

        # Give it a name, so it'll use a server side cursor. This is more memory effecient for large results
        reading_cursor = conn.cursor("reading_properties")
        reading_cursor.execute(query)
        for (id, raw_data) in percentage_printer(reading_cursor, msg="Calculating properties:", total=total):
            # Raw data is an array of TEXT, each element is the distance to a point, and then the input data columns
            # e.g. { '12|christian|catholic', '23|christian|', … }
            # So split it into a 2d list of list. Would like to have a native postgres 2d array (e.g.g text[][]), but it couldn't work with the aggregates.
            raw_data = [x.split(self.internal_string_sep, 1+len(self.input_data_cols)) for x in raw_data]

            # floatify the distance (first element)
            raw_data = [[float(item[0])] + self.clean_row_data(item[1:]) for item in raw_data]

            # raw data not guarantted to be sorted by distance ascending, so do it here
            raw_data.sort(key=lambda r:r[0])

            properties = self.properties(raw_data)
            properties = [(k, properties[k]) for k in sorted(properties.keys())]
            query = ("UPDATE {output_table} SET properties_calculated = TRUE, " + ", ".join(k+" = %s" for k, v in properties) + " WHERE id = {id};").format(output_table=self.output_table, id=id)
            writing_cursor.execute(query, [v for k, v in properties])

        writing_cursor.close()
        reading_cursor.close()



    def clean_row_data(self, row):
        """Python data cleaning/sanitization. This does nothing, but subclasses might want to override it"""
        return row

    def convert_to_polygons(self):
        """
        Convert the geometry to polygons if needed
        """

        if self.output_geom_type == 'polygon':
            return
        elif self.output_geom_type == 'point':
            # check geometry_columns to see if we need to do this
            conn = self.database_connection()
            db_cursor = conn.cursor()
            db_cursor.execute("select type from geometry_columns where f_table_name =  %s and f_geometry_column = %s", [self.output_table, self.output_geom_col])
            rows = db_cursor.fetchall()
            assert len(rows) == 1
            type = rows[0][0].lower()
            if type == 'multipolygon':
                # nothing to do here
                print "Already polygonified, nothing to do here"
            elif type == 'point':
                # convert to a polygon
                query = """
                    alter table {output_table}
                        alter column {output_geom_col} type geometry(MultiPolygon, {srid})
                            using ST_Multi(ST_MakeEnvelope(ST_X({output_geom_col})-({increment}/2), ST_Y({output_geom_col})-({increment}/2), ST_X({output_geom_col})+({increment}/2), ST_Y({output_geom_col})+({increment}/2), {srid}));
                    """
                query = query.format(
                    output_geom_col=self.output_geom_col, output_table=self.output_table,
                    srid=self.srid, increment=self.increment)

                print "Converting to polygons"
                db_cursor.execute(query)
                print "done."
                conn.commit()
            else:
                raise ValueError("Unknown column type "+type)
        else:
            raise ValueError
        
        
    def main(self):
        try:

            self.parse_args()

            self.create_table()
            
            self.create_land_boxes()

            self.populate_raw_data()

            self.calculate_properties()

            self.convert_to_polygons()

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

class BritainAndIrelandArea(object):
    top = 59.7
    bottom = 49.8
    right = 2.1
    left = -10.9
    increment = 0.025

class NorthAmericaArea(object):
    top = 74.12
    bottom = 17.64
    left = -169.45
    right = -51.33
