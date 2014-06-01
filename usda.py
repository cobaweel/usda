import pickle, json, collections, csv, gzip, sys, sqlite3, os

# Copyright (c) 2014 Jaap Weel

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

#
# Reads the most interesting tables in the USDA database and saves
# them to a pickle file and a JSON file
#

class Spec(object):
    def __init__(self, table, name):
        self.table = table
        self.name = name
        self.nullable = False
        self.isprimary = False

    def null(self):
        self.nullable = True
        return self

    def primary(self):
        self.isprimary = True
        return self

class TextSpec(Spec):
    def __init__(self, *args, **kwarg):
        super(TextSpec, self).__init__(*args, **kwarg)

    def parse(self, field):
        if self.nullable and field == '~~' or field == '':
            return None
        else:
            assert field != '~~' and field != ''
            return field

class IntegerSpec(Spec):
    def __init__(self, *args, **kwarg):
        super(IntegerSpec, self).__init__(*args, **kwarg)

    def parse(self, field):
        if self.nullable and field == '':
            return None
        else:
            assert field != '~~' and field != ''
            return int(field)

class RealSpec(Spec):
    def __init__(self, *args, **kwarg):
        super(RealSpec, self).__init__(*args, **kwarg)

    def parse(self, field):
        if self.nullable and field == '':
            return None
        else:
            assert field != '~~' and field != ''
            return float(field)

class Schema(object):
    def __init__(self):
        self.specs_by_table = collections.defaultdict(list)
    
    def spec(self, ctor, table, name):
        spec = ctor(table, name)
        self.specs_by_table[table].append(spec)
        return spec

    def text(self, table, name):
        return self.spec(TextSpec, table, name)

    def integer(self, table, name):
        return self.spec(IntegerSpec, table, name)

    def real(self, table, name):
        return self.spec(RealSpec, table, name)

    def read(self):
        record_by_key_by_table = {}
        for table, specs in self.specs_by_table.iteritems():
            print 'read', table
            record_by_key = {}
            auto_inc = 0
            for line in open('data/%s.txt' % table):
                auto_inc += 1
                line = line.rstrip()
                record = {}
                key = []
                for spec, field in zip(specs, line.split('^')):
                    field = field.strip('~')
                    if spec.isprimary:
                        key.append(spec.parse(field))
                    record[spec.name] = spec.parse(field)
                key = ':'.join(key)
                if key == '': key = auto_inc
                if key in record_by_key: print table, 'duplicate %r' % key
                assert key not in record_by_key
                record_by_key[key] = record
            record_by_key_by_table[table] = record_by_key
        return record_by_key_by_table

class USDA(object):
    def __init__(self):
        self.schema = Schema()
        self.schema.text('FOOD_DES','NDB_No').primary()
        self.schema.text('FOOD_DES','FdGrp_Cd')
        self.schema.text('FOOD_DES','Long_Desc')
        self.schema.text('FOOD_DES','Shrt_Desc')
        self.schema.text('FOOD_DES','ComName').null()
        self.schema.text('FOOD_DES','ManufacName').null()
        self.schema.text('FOOD_DES','Survey').null()
        self.schema.text('FOOD_DES','Ref_desc').null()
        self.schema.integer('FOOD_DES','Refuse').null()
        self.schema.text('FOOD_DES','SciName').null()
        self.schema.real('FOOD_DES','N_Factor').null()
        self.schema.real('FOOD_DES','Pro_Factor').null()
        self.schema.real('FOOD_DES','Fat_Factor').null()
        self.schema.real('FOOD_DES','CHO_Factor').null()
        self.schema.text('FD_GROUP','FdGrp_Cd').primary()
        self.schema.text('FD_GROUP','FdGrp_Desc')
        self.schema.text('LANGUAL','NDB_No').primary()
        self.schema.text('LANGUAL','Factor_Code').primary()
        self.schema.text('NUT_DATA','NDB_No').primary()
        self.schema.text('NUT_DATA','Nutr_No').primary()
        self.schema.real('NUT_DATA','Nutr_Val')
        self.schema.real('NUT_DATA','Num_Data_Pts')
        self.schema.real('NUT_DATA','Std_Error').null()
        self.schema.text('NUT_DATA','Src_Cd')
        self.schema.text('NUT_DATA','Deriv_Cd').null()
        self.schema.text('NUT_DATA','Ref_NDB_No').null()
        self.schema.text('NUT_DATA','Add_Nutr_Mark').null()
        self.schema.integer('NUT_DATA','Num_Studies').null()
        self.schema.real('NUT_DATA','Min').null()
        self.schema.real('NUT_DATA','Max').null()
        self.schema.integer('NUT_DATA','DF').null()
        self.schema.real('NUT_DATA','Low_EB').null()
        self.schema.real('NUT_DATA','Up_EB').null()
        self.schema.text('NUT_DATA','Stat_cmt').null()
        self.schema.text('NUT_DATA','AddMod_Date').null()
        self.schema.text('NUT_DATA','CC').null()
        self.schema.text('NUTR_DEF','Nutr_No').primary()
        self.schema.text('NUTR_DEF','Units')
        self.schema.text('NUTR_DEF','Tagname').null()
        self.schema.text('NUTR_DEF','NutrDesc')
        self.schema.text('NUTR_DEF','Num_Dec')
        self.schema.integer('NUTR_DEF','SR_Order')
        self.schema.text('SRC_CD', 'Src_Cd').primary()
        self.schema.text('SRC_CD', 'SrcCd_Desc')
        self.schema.text('DERIV_CD', 'Deriv_Cd').primary()
        self.schema.text('DERIV_CD', 'Deriv_Desc')
        self.schema.text('WEIGHT', 'NDB_No').primary()
        self.schema.text('WEIGHT', 'Seq').primary()
        self.schema.real('WEIGHT', 'Amount')
        self.schema.text('WEIGHT', 'Msre_Desc')
        self.schema.real('WEIGHT', 'Gm_wgt')
        self.schema.integer('WEIGHT', 'Num_Data_Pts').null()
        self.schema.real('WEIGHT', 'Std_Dev').null()
        self.schema.text('FOOTNOTE', 'NDB_No')
        self.schema.text('FOOTNOTE', 'Footnt_No')
        self.schema.text('FOOTNOTE', 'Footnt_Typ')
        self.schema.text('FOOTNOTE', 'Nutr_No').null()
        self.schema.text('FOOTNOTE', 'Footnt_Txt')
        self.schema.text('DATSRCLN', 'NDB_No').primary()
        self.schema.text('DATSRCLN', 'Nutr_No').primary()
        self.schema.text('DATSRCLN', 'DataSrc_ID').primary()
        self.schema.text('DATA_SRC', 'DataSrc_ID').primary()
        self.schema.text('DATA_SRC', 'Authors').null()
        self.schema.text('DATA_SRC', 'Title')
        self.schema.text('DATA_SRC', 'Year').null()
        self.schema.text('DATA_SRC', 'Journal').null()
        self.schema.text('DATA_SRC', 'Vol_city').null()
        self.schema.text('DATA_SRC', 'Issue_State').null()
        self.schema.text('DATA_SRC', 'Start_Page').null()
        self.schema.text('DATA_SRC', 'End_Page').null()
        self.usda = self.schema.read()

    def csv(self):
        for table, specs in self.schema.specs_by_table.iteritems():
            writer = csv.writer(gzip.open('usda-%s.csv.gz' % table, 'w'))
            writer.writerow([spec.name for spec in specs])
            for record in self.usda[table].itervalues():
                writer.writerow([record[spec.name] for spec in specs])

    def pickle(self):
        pickle.dump(self.usda, gzip.open('usda.pickle.gz', 'w'))

    def json(self):
        json.dump(self.usda, gzip.open('usda.json.gz', 'w'),
                  encoding='latin_1')

    def sqlite3(self):
        os.unlink('usda.sqlite3')
        db = sqlite3.connect('usda.sqlite3')
        for table, specs in self.schema.specs_by_table.iteritems():
            names = [spec.name for spec in specs]
            create_sql = 'CREATE TABLE {} ({})'.format(
                table, ','.join(names))
            insert_sql = 'INSERT INTO {} VALUES ({})'.format(
                table, ','.join(['?'] * len(names)))
            db.execute(create_sql)
            def encode(field):
                if field is None:
                    return None
                else:
                    return unicode(str(field),encoding='latin_1')
            db.executemany(insert_sql, 
                           [ [encode(record[spec.name]) for spec in specs]
                             for record in self.usda[table].itervalues()])
        db.commit()
        db.close()

def methods_to_call():
    if len(sys.argv) == 1:
        return ('csv', 'json', 'pickle', 'sqlite3')
    else:
        return sys.argv[1:]

methods = methods_to_call()
usda = USDA()
for method in methods:
    print method
    getattr(usda, method)()


