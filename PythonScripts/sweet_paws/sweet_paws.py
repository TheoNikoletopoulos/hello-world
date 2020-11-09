# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 17:49:34 2020

@author: theo
"""
import datetime
import json
import os

import pandas as pd

FILE_NAME = r'C:\SweetPaws\XLData\SP_data.xlsx'
CLEAN_FILE_NAME = r'C:\SweetPaws\XLData\SweetPaws.xls'
DIFF_FILE_NAME = r'C:\SweetPaws\XLData\DifferentTotCost.xls'
JSON_DIR = r'C:\SweetPaws\JSON'
TXT_DIR = r'C:\SweetPaws\TXT'
DATE_SEPERATOR = '/'
COL_INDEX = ('cust_name',
             'phone',
             'phone2',
             'street',
             'area',
             'zip_code',
             'email',
             'reference',
             'reference_type',
             'date_from',
             'location_from',
             'date_to',
             'location_to',
             'transport_cost',
             'daily_cost',
             'total_cost',
             'pet_name',
             'sex',
             'species',
             'breed',
             'vaccined',
             'depested',
             'stray',
             'castrated',
             'date_of_birth',
             'color',
             'chip' ,
             'social',
             'special_food',
             'medicine',
             'remarks',
             'vet_name',
             'vet_phone')

CUST_COLS = slice(0, 9)
STAY_COLS = slice(9, 16)
PET_COLS = slice(16, 32)

BOOL_VALUES = ('Ναι', 'Οχι')
SEX_VALUES = ('Α', 'Θ', 'A')
SPECIES_VALUES = ('Σκυλος', 'Γατα', 'Αλλο')
LOCATION_VALUES = ('Σπιτι', 'Αυλωνα', 'Αλλο')
REFERENCE_TYPE_VALUES = ('Εκπαιδευτης', 'Groomer', 'Internet', 'Πελατης', 'Φυλλαδιο', 'Άλλο')


def create_pet(data, attr_keys):
    pet_data = data.iloc[0, PET_COLS].dropna()
    pet = {attr_keys[xl_col]: pet_data[xl_col] for xl_col in pet_data.index.values}
    pet['log_records'] = []
    for j in range(len(data.index)):
        log_record_data = data.iloc[j, STAY_COLS]
        log_record = {attr_keys[xl_col]: log_record_data[xl_col] for xl_col in log_record_data.index.values }
        pet['log_records'].append(log_record)
    
    return pet


def get_indices(data_df, col_descr):
    col_name = data_df.columns.values[COL_INDEX.index(col_descr)]
    return list(data_df[col_name].dropna().index.values)


def to_datetime(date_str):
    dd_mm_yyyy = date_str.split(DATE_SEPERATOR)
    if len(dd_mm_yyyy) != 3:
        raise ValueError
    else:
        day = int(dd_mm_yyyy[0])
        month = int(dd_mm_yyyy[1])
        year = int(dd_mm_yyyy[2])
        
        return datetime.datetime(year, month, day)
    
    
def to_datestrUS(date):
    if isinstance(date, datetime.datetime):
        day = str(date.day)
        month = str(date.month)
        year = str(date.year)
        
        return DATE_SEPERATOR.join([month, day, year])
    else:
        return date
    

def to_datestrUK(date):
    if isinstance(date, datetime.datetime):
        day = str(date.day)
        month = str(date.month)
        year = str(date.year)
        
        return DATE_SEPERATOR.join([day, month, year])
    else:
        return date
    
    
def not_valid_ref_types(raw_cust_data):
    not_valid_series = raw_cust_data.iloc[:, COL_INDEX.index('reference_type')].dropna().apply(lambda x: x not in REFERENCE_TYPE_VALUES)
    return raw_cust_data.loc[not_valid_series.values]


def not_valid_bool(raw_pet_data, col_name):
    not_valid_series = raw_pet_data.iloc[:, COL_INDEX.index(col_name)].dropna().apply(lambda x: x not in BOOL_VALUES)
    return raw_pet_data.loc[not_valid_series.values]


def not_valid_sex(raw_pet_data):
    not_valid_series = raw_pet_data.iloc[:, COL_INDEX.index('sex')].dropna().apply(lambda x: x not in SEX_VALUES)
    return raw_pet_data.loc[not_valid_series.values]


def not_valid_species(raw_pet_data):
    not_valid_series = raw_pet_data.iloc[:, COL_INDEX.index('species')].dropna().apply(lambda x: x not in SPECIES_VALUES)
    return raw_pet_data.loc[not_valid_series.values]    


def not_valid_location(log_records, location):
    not_valid_series = log_records.iloc[:, COL_INDEX.index(location)].dropna().apply(lambda x: x not in LOCATION_VALUES)
    return log_records.loc[not_valid_series.values]


def populate_cost_col(raw_data, col_name):
    column = raw_data.columns.values[COL_INDEX.index(col_name)]
    cost_indices = get_indices(raw_data, col_name)
    for i, index in enumerate(cost_indices):
        current_value = raw_data.iloc[index][column]
        if i < len(cost_indices) - 1:
            next_index = cost_indices[i + 1]
        else:
            next_index = len(raw_data.index) - 1
        
        for row in range(index + 1, next_index):
            if current_value != 0.0:
                raw_data.loc[row, column] = current_value
                
            
def compute_total_cost(clients):
    for client in clients:
        for pet in client['pets']:
            for log_record in pet['log_records']:
                tot_cost = log_record['total_cost']
                date_from = to_datetime(log_record['date_from'])
                date_to = to_datetime(log_record['date_to'])
                days_stayed = (date_to - date_from).days
                log_record['days_stayed'] = days_stayed
                derived_cost = days_stayed * log_record['daily_cost'] + log_record['transport_cost']
                
                log_record['derived_cost'] = tot_cost
                if tot_cost != derived_cost:
                    log_record['derived_cost'] = derived_cost
                    
                    
def save_customer_json(customer):
    fname = '{0:s}-{1:d}.json'.format(customer['cust_name'], customer['phone'])
    fname = os.path.join(JSON_DIR, fname)
    with open(fname, "w") as file:
        json.dump(customer, file)
        

def save_customer_txt(customer):
    cust_name = customer['cust_name']
    if '/' in cust_name:
        cust_name = cust_name.replace('/', '-')
    fname = '{0:s}-{1:d}.txt'.format(cust_name, customer['phone'])
    fname = os.path.join(TXT_DIR, fname)
    with open(fname, "w", encoding = 'utf-8') as file:
        for key, value in customer.items():
            if key == 'pets':
                pets = value
            else:
                file.write('{} = {}.\n'.format(key, value))
                
        for pet in pets:
            save_pet_txt(pet, file)

def save_pet_txt(pet, file):
    file.write('\n')
    for key, value in pet.items():
        if key == 'log_records':
            log_records = value
        else:
            file.write('{} = {}.\n'.format(key, value))
            
    file.write('DateFrom----DateTo----LocFrom----LocTo----DailyCost----TransportCost----TotalCost\n')
    for log_record in log_records:
        save_log_record_txt(log_record, file)
        

def save_log_record_txt(log_record, file):
    line = '{}--{}--{}--{}--{}--{}--{}\n'.format(log_record['date_from'], 
                                           log_record['date_to'],
                                           log_record['location_from'],
                                           log_record['location_to'],
                                           log_record['daily_cost'],
                                           log_record['transport_cost'],
                                           log_record['total_cost'])
    file.write(line)
    

# Read the xlsx file.
raw_data = pd.read_excel(FILE_NAME, sheet = 'Sheet1')
# Display some intial information about the contents of the file.
client_indices = get_indices(raw_data, 'cust_name')
print('Contains {} customers.'.format(len(client_indices)))
pet_indices = get_indices(raw_data, 'pet_name')
print('Contains {} pets.'.format(len(pet_indices)))
date_from_indices = get_indices(raw_data, 'date_from')
print('Contains {} dates from.'.format(len(date_from_indices)))
date_to_indices = get_indices(raw_data, 'date_to')
print('Contains {} dates to.'.format(len(date_to_indices)))

raw_cust_data = raw_data.iloc[client_indices, :]
# 1. Check for valid reference type values
invalid_ref_types_data = not_valid_ref_types(raw_cust_data)
if not invalid_ref_types_data.empty:
    print(invalid_ref_types_data.iloc[:, CUST_COLS])
    
raw_pet_data = raw_data.iloc[pet_indices, :]
# 2. Check for valid sex values
invalid_sex_data = not_valid_sex(raw_pet_data)
if not invalid_sex_data.empty:
    print(invalid_sex_data.iloc[:, PET_COLS])
    
# 3. Check for valid boolean fields values
bool_fields = ['vaccined', 'depested', 'stray', 'castrated']
for bool_field in bool_fields:
    invalid_bool_data = not_valid_bool(raw_pet_data, bool_field)
    if not invalid_bool_data.empty:
        print(bool_field.upper)
        print(invalid_bool_data.iloc[:, PET_COLS])
        
# 4. Check for valid species values
invalid_species_data = not_valid_species(raw_pet_data)
if not invalid_species_data.empty:
    print(invalid_species_data.iloc[:, PET_COLS])
    
# 5. Check for valid location values
locations = ['location_from', 'location_to']
for loc in locations:
    invalid_location_data = not_valid_location(raw_data, loc)
    if not invalid_location_data.empty:
        print(loc.upper)
        print(invalid_location_data.iloc[:, STAY_COLS])
        
# Get and display all dog breeds
species_col = raw_data.columns.values[COL_INDEX.index('species')]
dog_rows = raw_data[species_col] == SPECIES_VALUES[0]
dogs = raw_data.loc[dog_rows]
breed_col = raw_data.columns.values[COL_INDEX.index('breed')]
dog_breeds = set(dogs[breed_col].values)
for i, dog_breed in enumerate(sorted(list(dog_breeds)), 1):
    print('{0:d}  {1:s}'.format(i, dog_breed))
# CLEAN breed data    
maltez_rows = raw_data[breed_col] == 'Μαλτέζ'
raw_data.loc[maltez_rows.values, breed_col] = 'Μαλτεζ'
   
# Get and display all references
ref_col = raw_data.columns.values[COL_INDEX.index('reference')]
references = raw_cust_data[ref_col].dropna()
references = set(references.values)
for i, reference in enumerate(sorted(list(references)), 1):
    print('{0:d}  {1:s}'.format(i, reference))

populate_cost_col(raw_data, 'transport_cost')    
# Find all records with no transport cost - set to zero
location_from_col = raw_data.columns.values[COL_INDEX.index('location_from')]
location_to_col = raw_data.columns.values[COL_INDEX.index('location_to')]
transport_col = raw_data.columns.values[COL_INDEX.index('transport_cost')]
no_transport_rows = (raw_data[location_from_col] == LOCATION_VALUES[1]) & (raw_data[location_to_col] == LOCATION_VALUES[1])
raw_data.loc[no_transport_rows.values, transport_col] = 0.0

populate_cost_col(raw_data, 'daily_cost')
populate_cost_col(raw_data, 'total_cost')
    

# Date formats in excel are mixed up. Need to split the contents in two and
# process separately
split_row = 223 # 228
line_offset = 2
raw_data1 = raw_data.iloc[:split_row].copy()
raw_data2 = raw_data.iloc[split_row:].copy()

# FIRST VALIDATION: date_from < date_to
date_from_col = raw_data.columns.values[COL_INDEX.index('date_from')]
date_to_col = raw_data.columns.values[COL_INDEX.index('date_to')]
# Convert all dates to str of the format dd/mm/yyyy
raw_data1[date_from_col] = raw_data1[date_from_col].apply(to_datestrUS)
raw_data1[date_to_col] = raw_data1[date_to_col].apply(to_datestrUS)
# Get the associated datetimes and compare
for line, (date_from, date_to) in enumerate(zip(raw_data1[date_from_col].values, raw_data1[date_to_col].values), line_offset):
    try:
        date_from = to_datetime(date_from)
        date_to = to_datetime(date_to)
        if date_from > date_to:
            print('DATE ORDER ERROR: {} > {} in line {}'.format(date_from, date_to, line))
    except ValueError:
        print('PARSING ERROR: {}, {} in line {}'.format(date_from, date_to, line))
        
raw_data2[date_from_col] = raw_data2[date_from_col].apply(to_datestrUK)
raw_data2[date_to_col] = raw_data2[date_to_col].apply(to_datestrUK)
# Get the associated datetimes and compare
for line, (date_from, date_to) in enumerate(zip(raw_data2[date_from_col].values, raw_data2[date_to_col].values), line_offset):
    try:
        date_from = to_datetime(date_from)
        date_to = to_datetime(date_to)
        if date_from > date_to:
            print('DATE ORDER ERROR: {} > {} in line {}'.format(date_from, date_to, line + split_row))
    except ValueError:
        print('PARSING ERROR: {}, {} in line {}'.format(date_from, date_to, line + split_row))
        
clean_data = pd.concat([raw_data1, raw_data2])
clean_data.to_excel(CLEAN_FILE_NAME, index = None)
        
# Create main data structure
attr_keys = {xl_col: col for xl_col, col in zip(clean_data.columns.values, COL_INDEX)}
clients = []
for i in range(len(client_indices)):
    if i < len(client_indices) - 1:
        data = clean_data.iloc[client_indices[i] : client_indices[i + 1]]
    else:
        data = clean_data.iloc[client_indices[i] : ]
    client_data = data.iloc[0, CUST_COLS].dropna()
    client = {attr_keys[xl_col]: client_data[xl_col] for xl_col in client_data.index.values}
    client['pets'] = []
    pet_indices = get_indices(data, 'pet_name')
    if len(pet_indices) > 1:
        for i in range(len(pet_indices)):
            if i < len(pet_indices) - 1:
                pet_slice = data.loc[pet_indices[i] : pet_indices[i + 1] - 1]
            else:
                pet_slice = data.loc[pet_indices[i] : ]
            client['pets'].append(create_pet(pet_slice, attr_keys))
    else:
        client['pets'].append(create_pet(data, attr_keys))
    
    clients.append(client)

num_clients = len(clients)
num_pets = sum([len(client['pets']) for client in clients])
num_log_records = sum([len(pet['log_records']) for client in clients for pet in client['pets']])

# Fix phone number format (Pandas reads it as float)
for client in clients:
    client['phone'] = int(client['phone'])
    if 'phone2' in client and isinstance(client['phone2'], float):
        client['phone2'] = int(client['phone2'])

for client in clients:
    for pet in client['pets']:
        pet['parent'] = client['cust_name']
        for log_record in pet['log_records']:
            log_record['parent'] = '{0:s} - {1:s}'.format(pet['parent'], pet['pet_name'])
            
compute_total_cost(clients)
diff_records = [log_record for client in clients for pet in client['pets'] for log_record in pet['log_records'] if log_record['total_cost'] != log_record['derived_cost']]
diff_df = pd.DataFrame(diff_records)
diff_df.to_excel(DIFF_FILE_NAME, index = None)


for cust in clients:
    save_customer_txt(cust)