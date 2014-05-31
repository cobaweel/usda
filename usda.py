import pickle, json, collections

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
            record_by_key = {}
            for line in open('data/%s.txt' % table):
                line = line.rstrip()
                record = {}
                key = []
                for spec, field in zip(specs, line.split('^')):
                    field = field.strip('~')
                    if spec.isprimary:
                        key.append(spec.parse(field))
                    else:
                        record[spec.name] = spec.parse(field)
                key = ':'.join(key)
                record_by_key[key] = record
            record_by_key_by_table[table] = record_by_key
        return record_by_key_by_table

class _:
    schema = Schema()
    schema.text('FOOD_DES','NDB_No').primary()
    schema.text('FOOD_DES','FdGrp_Cd')
    schema.text('FOOD_DES','Long_Desc')
    schema.text('FOOD_DES','Shrt_Desc')
    schema.text('FOOD_DES','ComName').null()
    schema.text('FOOD_DES','ManufacName').null()
    schema.text('FOOD_DES','Survey').null()
    schema.text('FOOD_DES','Ref_desc').null()
    schema.integer('FOOD_DES','Refuse').null()
    schema.text('FOOD_DES','SciName').null()
    schema.real('FOOD_DES','N_Factor').null()
    schema.real('FOOD_DES','Pro_Factor').null()
    schema.real('FOOD_DES','Fat_Factor').null()
    schema.real('FOOD_DES','CHO_Factor').null()
    schema.text('FD_GROUP','FdGrp_Cd').primary()
    schema.text('FD_GROUP','FdGrp_Desc')
    schema.text('NUT_DATA','NDB_No').primary()
    schema.text('NUT_DATA','Nutr_No')
    schema.real('NUT_DATA','Nutr_Val')
    schema.real('NUT_DATA','Num_Data_Pts')
    schema.real('NUT_DATA','Std_Error').null()
    schema.text('NUT_DATA','Src_Cd')
    schema.text('NUT_DATA','Deriv_Cd').null()
    schema.text('NUT_DATA','Ref_NDB_No').null()
    schema.text('NUT_DATA','Add_Nutr_Mark').null()
    schema.integer('NUT_DATA','Num_Studies').null()
    schema.real('NUT_DATA','Min').null()
    schema.real('NUT_DATA','Max').null()
    schema.integer('NUT_DATA','DF').null()
    schema.real('NUT_DATA','Low_EB').null()
    schema.real('NUT_DATA','Up_EB').null()
    schema.text('NUT_DATA','Stat_cmt').null()
    schema.text('NUT_DATA','AddMod_Date').null()
    schema.text('NUT_DATA','CC').null()
    schema.text('NUTR_DEF','Nutr_No').primary()
    schema.text('NUTR_DEF','Units')
    schema.text('NUTR_DEF','Tagname').null()
    schema.text('NUTR_DEF','NutrDesc')
    schema.text('NUTR_DEF','Num_Dec')
    schema.integer('NUTR_DEF','SR_Order')
    schema.text('SRC_CD', 'Src_Cd').primary()
    schema.text('SRC_CD', 'SrcCd_Desc')
    schema.text('DERIV_CD', 'Deriv_Cd').primary()
    schema.text('DERIV_CD', 'Deriv_Desc')
    schema.text('WEIGHT', 'NDB_No').primary()
    schema.text('WEIGHT', 'Seq')
    schema.real('WEIGHT', 'Amount')
    schema.text('WEIGHT', 'Msre_Desc')
    schema.real('WEIGHT', 'Gm_wgt')
    schema.integer('WEIGHT', 'Num_Data_Pts').null()
    schema.real('WEIGHT', 'Std_Dev').null()
    schema.text('FOOTNOTE', 'NDB_No')
    schema.text('FOOTNOTE', 'Footnt_No')
    schema.text('FOOTNOTE', 'Footnt_Typ')
    schema.text('FOOTNOTE', 'Nutr_No').null()
    schema.text('FOOTNOTE', 'Footnt_Txt')
    schema.text('DATSRCLN', 'NDB_No').primary()
    schema.text('DATSRCLN', 'Nutr_No').primary()
    schema.text('DATSRCLN', 'DataSrc_ID').primary()
    schema.text('DATA_SRC', 'DataSrc_ID').primary()
    schema.text('DATA_SRC', 'Authors').null()
    schema.text('DATA_SRC', 'Title')
    schema.text('DATA_SRC', 'Year').null()
    schema.text('DATA_SRC', 'Journal').null()
    schema.text('DATA_SRC', 'Vol_city').null()
    schema.text('DATA_SRC', 'Issue_State').null()
    schema.text('DATA_SRC', 'Start_Page').null()
    schema.text('DATA_SRC', 'End_Page').null()

    usda = schema.read()
    pickle.dump(usda, open('usda.pickle', 'w'))
    json.dump(usda, open('usda.json', 'w'), encoding='latin_1')
