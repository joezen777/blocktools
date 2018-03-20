from __future__ import print_function

import base64
import json
import datetime

print('Loading function')


def lambda_handler(event, context):
    output = []

    
    for record in event['records']:
        payload = base64.b64decode(record['data'])
        
        json_record = json.loads(payload)
        tDt = datetime.datetime.fromtimestamp(json_record['time'])
        json_record['time'] = tDt
        json_record['timeYear'] = tDt.year
        json_record['timeMonth'] = tDt.month
        json_record['timeDay'] = tDt.day
        json_record['timeHour'] = tDt.hour
        json_record['timeMinute'] = tDt.minute
        json_record['timeWeekDay'] = tDt.isoweekday()
        
        
        json_record['blocktime'] = datetime.datetime.fromtimestamp(json_record['blocktime'])
        
        json_record['fromTx'] = []
        for vin in json_record['vin']:
            if 'txid' in vin:
                json_record['fromTx'].append(vin['txid'])
            else:
                json_record['fromTx'].append('coinbase')
        
        json_record['toAddresses'] = []
        json_record['totalValue'] = 0
        json_record['payIn'] = len(json_record['vin'])
        json_record['payOut'] = len(json_record['vout'])
        for vout in json_record['vout']:
            json_record['toAddresses'].extend(vout['scriptPubKey']['addresses'])
            json_record['totalValue'] = json_record['totalValue'] + vout['value']
            
        
        print(json_record)
        

        # Do custom processing on the record payload here
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(json.dumps(json_record, indent=4, sort_keys=True, default=str))
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}

