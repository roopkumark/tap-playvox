ENDPOINTS_CONFIG = {
    'users': {
        'persist': True,
        'path': 'organisation/users',
        'data_key': 'data',
        'pk': ['id'],
        'paginate': False,
        'provides': {
            'id': 'id'
        }
    },
     'metrics': {
        'persist': True,
        'path': 'workactivity/workstreams/metrics/agents',
        'data_key': 'data',
        'metric_key': 'dates',
        'user_key':  'users',
        'pk': ['id', 'date'],
        'paginate': False,
        'provides': {
            'id': 'id',
            'date': 'date'
        }   
    }
    
}
