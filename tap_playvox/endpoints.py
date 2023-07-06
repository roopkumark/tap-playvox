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
        'path': 'api/workactivity/workstreams/metrics/agents',
        'data_key': 'data',
        'metric_key': 'dates',
        'user_key':  'users',
        'pk': ['id', 'date'],
        'paginate': False,
        'provides': {
            'id': 'id',
            'date': 'date'
        }   
    },
     'tasks': {
        'persist': True,
        'path': 'api/workactivity/tasks',
        'data_key': 'data',
        'pk': ['id', 'createdAt'],
        'paginate': False,
        'provides': {
            'id': 'id',
            'createdAt': 'createdAt'
        }   
    }
    
}
