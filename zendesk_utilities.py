#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import os
import requests
import datetime
import pprint
import httplib2
import json
from datetime import datetime
import sqlite3
import sys

"""
Zendesk API documentation: https://developers.pipedrive.com/v1

General logic:
- for each object (organizations, users, tickets):
  - get json via API
  - strip off json labels as column labels
  - check datatype of json values to use in table creation
  - create a table in a local Sqlite3 database
  - for each record:
    - send record to generate sql function
    - execute insert into table 
"""
def get_organizations():
    data = get_data('organizations.json', 'organizations')
    return data

    
def get_users():
    data = get_data('users.json', 'users')
    return data


def get_tickets():
    url_filtered_tickets = 'search.json?query=created>2017-11-01 type:ticket'
    data = get_data(url_filtered_tickets, 'results')
    return data


def get_data(url_details, field_value):
    # Set the request parameters
    with open('api.secrets', 'r') as f:
        data = f.readlines()
        base_url = data[0].strip()
        user = data[1].strip()
        pwd = data[2].strip()

    url = base_url + url_details
    # print(user)
    # print(pwd)
    # print(url)
    results = []

    # Contine to call the API until all results are returned
    while url:
        # Do the HTTP get request
        headers = {'Accept': 'application/json'}
        response = requests.get(url, auth=(user, pwd), headers=headers)

        # Check for HTTP codes other than 200
        if response.status_code != 200:
            print('Status:', response.status_code,
                  'Problem with the request. Exiting.')
            exit()

        data = response.json()
        results.extend(data[field_value])

        if 'next_page' in data:
            url = data['next_page']

    print('{} records returned from Zendesk\n'.format(len(results)))

    return results


def get_type(value):
    data_type = ''
    if isinstance(value, int):
        data_type = 'INTEGER'
    elif isinstance(value, str):
        data_type = 'TEXT'
    elif value is None:
        data_type = 'TEXT'
    else:
        data_type = type(value)

    return data_type


def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict and x != {}:
            for a in x:
                flatten(x[a], name + str(a).replace('::','_') + '_')
        elif type(x) is list and x != []:
            i = 0
            for a in x:
                flatten(a, name + str(i).replace('::','_') + '_')
                i += 1
        else:
            if x == {} or x == []:
                x = None
            data_type = get_type(x)
            out[name[:-1]] = {'value': x, 'data_type': data_type, 'name': name[:-1]}

    flatten(y)
    return out


def assemble_table_data(data_list):
    data = []
    
    for value in data_list:
        data.append(flatten_json(value))
        
    return data
    
    
def build_attribute_dict(data_list):
    '''
    Take first record in json file
    Loop through label names & strip them off
    Check datatype on value
    Return attributes dictionary for building table
    '''
    table_data = {}
    for i in data_list:
        for key in i.keys():
            if key not in table_data:
                table_data[key] = i[key]
    
    attribute_dict = {}
    for column_name, values in table_data.items():
        attribute_dict[column_name] = {
            'name': values['name'].replace(' ', '_').replace('/', '_').replace('-', '_'),
            'data_type': values['data_type'].replace(' ', '_').replace('/', '_').replace('-', '_')
        }

    return attribute_dict


##################
# Function Calls #
##################

# https://help.zendesk.com/hc/en-us/articles/229488988-Getting-large-data-sets-with-the-Zendesk-API-and-Python

if __name__ == '__main__':
    start_time = time.time()

    print('Getting ticket data from Zendesk...\n')

    ticket_data = get_tickets()

    pprint.pprint(ticket_data)

    end_time = time.time()
    print('\nElapsed time: {:.2f} seconds\n'.format(end_time - start_time))
